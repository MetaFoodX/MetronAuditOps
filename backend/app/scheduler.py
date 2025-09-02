import asyncio
import csv
import glob
import json
import logging
import os
import shutil
import sys
import time
import zipfile
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from boto3.dynamodb.conditions import Attr
from datetime import datetime
from pathlib import Path
from pytz import timezone
from typing import Dict, Optional

from app.utils.config import get_config
from app.utils.dynamo_client import get_dynamodb

logger = logging.getLogger(__name__)


_job_lock = asyncio.Lock()
_date_propagation_state: Dict[str, Dict[str, bool]] = (
    {}
)  # {date: {running: bool, noData: bool}}
_ai_state: Dict[str, Dict[str, object]] = (
    {}
)  # {date: {running: bool, completedAt: str|None, lastError: str|None}}
_scheduler: Optional[AsyncIOScheduler] = None
_last_ui_trigger_ts: float = 0.0


def _ensure_repo_root_on_path() -> Path:
    """Ensure repository root is on sys.path so we can import audit modules."""
    backend_dir = Path(__file__).resolve().parents[1]
    repo_root = backend_dir.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    audit_dir = repo_root / "audit_automation"
    if str(audit_dir) not in sys.path:
        sys.path.insert(0, str(audit_dir))
    return repo_root


async def populate_today_audits() -> None:
    """Download latest audits, extract, and populate DynamoDB for today's data.

    Safe to run multiple times; DynamoDB puts are idempotent by key.
    """
    await populate_audits_for_date(None)


def get_propagation_state(date_str: str | None) -> Dict[str, bool]:
    key = date_str or "latest"
    return _date_propagation_state.get(key, {"running": False, "noData": False})


def set_propagation_state(
    date_str: str | None, running: bool = None, noData: bool = None
) -> None:
    key = date_str or "latest"
    state = _date_propagation_state.get(key, {"running": False, "noData": False})
    if running is not None:
        state["running"] = running
    if noData is not None:
        state["noData"] = noData
    _date_propagation_state[key] = state


def get_ai_state(date_str: str) -> Dict[str, object]:
    state = _ai_state.get(date_str, None)
    if state is None:
        state = {
            "running": False,
            "completedAt": None,
            "lastError": None,
            "coverage": {"total": 0, "withPan": 0},
        }
        _ai_state[date_str] = state
    return state


def set_ai_state(
    date_str: str, running: bool = None, completedAt: str = None, lastError: str = None
) -> None:
    state = get_ai_state(date_str)
    if running is not None:
        state["running"] = running
    if completedAt is not None:
        state["completedAt"] = completedAt
    if lastError is not None:
        state["lastError"] = lastError
    _ai_state[date_str] = state


def _compute_coverage_for_date(date_str: str) -> dict:
    try:
        from app.utils.config import get_config
        from app.utils.dynamo_client import get_dynamodb

        cfg = get_config()
        table = get_dynamodb().Table(cfg["dynamodb"]["table_names"]["scan_audit"])
        from boto3.dynamodb.conditions import Attr

        resp = table.scan(
            FilterExpression=Attr("RestaurantDate").contains(f"#{date_str}"),
            ProjectionExpression="RestaurantDate, panId, PanID, identifiedPan, genAIPanId, YOLOv8_Pan_ID, Corner_Best_Pan_ID",
        )
        items = resp.get("Items", [])
        total = len(items)
        with_pan = 0
        for it in items:
            # consider having any of the pan id fields as coverage
            if any(
                [
                    it.get("panId"),
                    it.get("PanID"),
                    it.get("identifiedPan"),
                    it.get("genAIPanId"),
                    it.get("YOLOv8_Pan_ID"),
                    it.get("Corner_Best_Pan_ID"),
                ]
            ):
                with_pan += 1
        return {"total": total, "withPan": with_pan}
    except Exception:
        return {"total": 0, "withPan": 0}


async def populate_audits_for_date(date_str: str = None, run_ai: bool = True) -> None:
    """Download audits for a specific date (or latest if None), extract, and populate DynamoDB.

    Args:
        date_str: Date in YYYY-MM-DD format, or None for latest date
        run_ai: If True, run AI enrichment (GenAI/YOLO/Corner); if False, ingest raw CSVs only
    """
    if _job_lock.locked():
        logger.info(
            "⏳ Audit population already running; skipping concurrent invocation"
        )
        return

    async with _job_lock:
        try:
            repo_root = _ensure_repo_root_on_path()

            # Lazy imports after sys.path is patched
            from audit_automation.download_s3_audits import (  # type: ignore
                start_download,
                start_download_for_date,
            )
            from audit_automation.scan_dynamo_manager import (
                ScanDynamoManager,  # type: ignore
            )
            from system.utils.config_loader import load_config  # type: ignore

            config = load_config()
            base_dir = Path(config["audit"]["audit_directory"])  # where zips/CSVs live

            # mark running state
            set_propagation_state(date_str, running=True)

            if date_str:
                logger.info(f"⬇️  Downloading audits for {date_str} from S3…")
                downloaded = start_download_for_date(date_str)
                # If nothing downloaded, mark noData and exit early
                if not downloaded:
                    set_propagation_state(date_str, running=False, noData=True)
                    logger.warning(
                        f"📭 No files found in S3 for {date_str}; stopping propagation"
                    )
                    return
            else:
                logger.info("⬇️  Downloading latest audits from S3…")
                downloaded = start_download()
                if not downloaded:
                    set_propagation_state(date_str, running=False, noData=True)
                    logger.warning(
                        "📭 No files found in S3 for latest date; stopping propagation"
                    )
                    return
            logger.info(
                f"📦 Extracting downloaded zips for {'date ' + date_str if date_str else 'latest date'}…"
            )
            _extract_zip_files_in_dir(str(base_dir))

            # Debug: List what was extracted
            logger.info(f"📁 Contents of {base_dir}:")
            for item in base_dir.iterdir():
                if item.is_dir():
                    logger.info(f"  📂 {item.name}/")
                    for subitem in item.iterdir():
                        logger.info(
                            f"    {'📄' if subitem.is_file() else '📂'} {subitem.name}"
                        )
                else:
                    logger.info(f"  📄 {item.name}")

            # Find all relevant CSVs (recursively) and populate
            csv_files = [
                p for p in base_dir.rglob("*.csv") if "Venue_Summaries" not in p.name
            ]
            if not csv_files:
                logger.warning(f"📭 No CSVs found in {base_dir}; nothing to populate")
                set_propagation_state(date_str, running=False, noData=True)
                return

            manager = ScanDynamoManager()
            total_processed = 0
            total_skipped = 0
            for csv_file in csv_files:
                try:
                    # Derive scan folder and restaurant id more robustly
                    csv_path = Path(csv_file)
                    scan_folder = str(csv_path.parent)

                    # Extract restaurant_id from the path more carefully
                    # Look for patterns like "169" or restaurant IDs in the path
                    path_parts = csv_path.parts
                    restaurant_id = None

                    # Try to find restaurant ID in the path
                    for part in path_parts:
                        # Look for numeric restaurant IDs
                        if part.isdigit():
                            restaurant_id = part
                            break
                        # Also check for restaurant names that might contain the ID
                        if "ScansToAudit" in part and "-" in part:
                            # Extract from patterns like "ScansToAudit-Mayan Princess-Balam-2025-08-17_16-00"
                            parts = part.split("-")
                            if len(parts) > 1:
                                # Try to find a numeric part that could be the restaurant ID
                                for p in parts:
                                    if p.isdigit():
                                        restaurant_id = p
                                        break
                                if restaurant_id:
                                    break

                    if not restaurant_id:
                        logger.warning(
                            f"⚠️ Could not extract restaurant_id from path: {csv_path}"
                        )
                        continue

                    logger.info(
                        f"🔍 Processing CSV: {csv_file} (restaurant_id: {restaurant_id})"
                    )

                    if run_ai:
                        # Run AI pipeline: GenAI actions → GenAI pan recognition → YOLOv8 → Corner detection
                        csv_to_ingest = await _run_ai_pipeline_on_csv(
                            str(csv_file), scan_folder, restaurant_id
                        )
                    else:
                        # Skip AI; ingest the raw CSV
                        csv_to_ingest = str(csv_file)

                    logger.info(f"📄 Populating DynamoDB from {csv_to_ingest}")
                    result = manager.populate_csv(str(csv_to_ingest))
                    total_processed += result.get("processed", 0)
                    total_skipped += result.get("skipped", 0)
                except Exception as e:  # continue on individual file errors
                    logger.error(f"❌ Failed to populate from {csv_file}: {e}")

            # Verify: recompute expected counts from CSVs and compare with DynamoDB
            expected_counts = _compute_expected_counts_from_csvs(csv_files, manager)
            mismatches = []
            ok = 0
            for restaurant_date, expected in expected_counts.items():
                try:
                    restaurant_id, _, date = restaurant_date.partition("#")
                    found = manager.verify_data(restaurant_id, date, expected)
                    if expected is not None and found == expected:
                        ok += 1
                    else:
                        mismatches.append(
                            {
                                "restaurant_date": restaurant_date,
                                "expected": expected,
                                "found": found,
                            }
                        )
                except Exception as e:
                    logger.error(f"❌ Verification failed for {restaurant_date}: {e}")

            if mismatches:
                logger.warning(
                    f"⚠️ Verification mismatches: {len(mismatches)} partitions did not match expected counts"
                )
                for mm in mismatches[:20]:
                    logger.warning(
                        f"  {mm['restaurant_date']}: expected={mm['expected']} found={mm['found']}"
                    )
                if len(mismatches) > 20:
                    logger.warning(f"  … and {len(mismatches) - 20} more")
            logger.info(
                f"🔎 Verification complete: OK={ok}, mismatches={len(mismatches)}"
            )

            # Sample-level item verification for critical fields
            sample_result = _verify_sample_items_in_dynamo(
                csv_files, manager, per_file=3
            )
            logger.info(
                f"🧪 Sample verification: checked={sample_result['checked']}, "
                f"missing={len(sample_result['missing'])}, field_mismatches={len(sample_result['field_mismatches'])}"
            )
            if sample_result["missing"]:
                logger.warning(
                    f"Missing items (showing up to 10): {sample_result['missing'][:10]}"
                )
            if sample_result["field_mismatches"]:
                logger.warning(
                    f"Field mismatches (showing up to 10): {sample_result['field_mismatches'][:10]}"
                )

            # Sample-level item verification for critical fields
            sample_result = _verify_sample_items_in_dynamo(
                csv_files, manager, per_file=3
            )
            logger.info(
                f"🧪 Sample verification: checked={sample_result['checked']}, "
                f"missing={len(sample_result['missing'])}, field_mismatches={len(sample_result['field_mismatches'])}"
            )
            if sample_result["missing"]:
                logger.warning(
                    f"Missing items (showing up to 10): {sample_result['missing'][:10]}"
                )
            if sample_result["field_mismatches"]:
                logger.warning(
                    f"Field mismatches (showing up to 10): {sample_result['field_mismatches'][:10]}"
                )

            logger.info(
                f"✅ Audit population finished: processed={total_processed}, skipped={total_skipped}"
            )

            # Record successful run
            now = datetime.now(timezone("America/Los_Angeles"))
            _record_successful_run(now, "scheduled")

            # Smart retry is only meaningful when AI is enabled
            if run_ai:
                try:
                    _smart_retry_for_missing(csv_files, base_dir)
                except Exception as e:
                    logger.warning(f"Smart retry pass failed: {e}")
            # completed successfully
            set_propagation_state(date_str, running=False, noData=False)
        except Exception as e:
            logger.exception(f"❌ Audit population job failed: {e}")
            set_propagation_state(date_str, running=False)


def _extract_zip_files_in_dir(zip_folder: str) -> None:
    """Extracts all ScansToAudit*.zip files in the given directory to sibling folders.

    Handles nested zip structures by flattening them to avoid double-nested directories.
    """
    zip_files = sorted(glob.glob(os.path.join(zip_folder, "ScansToAudit*.zip")))
    for zip_file in zip_files:
        extract_path = os.path.splitext(zip_file)[0]
        if not os.path.exists(extract_path):
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                # Check if the zip contains a nested structure with the same name
                zip_contents = zip_ref.namelist()
                has_nested_structure = any(
                    name.startswith(os.path.basename(extract_path) + "/")
                    for name in zip_contents
                )

                if has_nested_structure:
                    # Extract to a temp location first, then move contents up one level
                    temp_extract = extract_path + "_temp"
                    zip_ref.extractall(temp_extract)

                    # Find the nested directory and move its contents up
                    nested_dir = os.path.join(
                        temp_extract, os.path.basename(extract_path)
                    )
                    if os.path.exists(nested_dir):
                        # Move all contents from nested_dir to extract_path
                        for item in os.listdir(nested_dir):
                            src = os.path.join(nested_dir, item)
                            dst = os.path.join(extract_path, item)
                            if os.path.isdir(src):
                                shutil.move(src, dst)
                            else:
                                shutil.move(src, dst)
                        # Remove the now-empty nested directory
                        os.rmdir(nested_dir)

                    # Clean up temp directory
                    os.rmdir(temp_extract)
                else:
                    # Normal extraction
                    zip_ref.extractall(extract_path)

            logger.info(f"Extracted: {zip_file} -> {extract_path}")


def _today_pst_str() -> str:
    tz = timezone("America/Los_Angeles")
    return datetime.now(tz).strftime("%Y-%m-%d")


def _is_today_populated() -> bool:
    """Check whether today's PST date has any items in ScanAuditTable.

    Uses a paginated scan with a filter on RestaurantDate containing '#YYYY-MM-DD'.
    Stops early on first hit, up to a small number of pages to avoid heavy scans.
    """
    cfg = get_config()
    table_name = cfg["dynamodb"]["table_names"]["scan_audit"]
    table = get_dynamodb().Table(table_name)
    date_str = _today_pst_str()
    try:
        filter_expr = Attr("RestaurantDate").contains(f"#{date_str}")
        kwargs = {
            "FilterExpression": filter_expr,
            "ProjectionExpression": "RestaurantDate",
        }
        pages = 0
        MAX_PAGES = 5
        while True:
            resp = table.scan(**kwargs)
            items = resp.get("Items", [])
            if items:
                return True
            pages += 1
            if pages >= MAX_PAGES or "LastEvaluatedKey" not in resp:
                break
            kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
        return False
    except Exception as e:
        logger.warning(f"⚠️ Failed to check today's population status: {e}")
        # On failure to check, assume populated to avoid redundant re-downloads on boot
        return True


def _compute_expected_counts_from_csvs(csv_files, manager) -> dict:
    """Compute expected item counts per RestaurantDate from the CSVs using the same
    transformation as insertion (via manager.process_csv_row).
    """
    counts: dict = {}
    for p in csv_files:
        try:
            with open(p, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    item = manager.process_csv_row(row, p.name)
                    if not item:
                        continue
                    restaurant_date = item.get("RestaurantDate")
                    if not restaurant_date:
                        continue
                    counts[restaurant_date] = counts.get(restaurant_date, 0) + 1
        except Exception as e:
            logger.error(f"❌ Failed computing expected counts for {p}: {e}")
    return counts


def _verify_sample_items_in_dynamo(csv_files, manager, per_file: int = 3) -> dict:
    """Verify a small sample of items exist in DynamoDB and spot-check key fields.
    Returns a dict with 'checked', 'missing', and 'field_mismatches'.
    """
    from random import sample

    checked = 0
    missing = []
    field_mismatches = []

    for p in csv_files:
        try:
            with open(p, newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
            if not rows:
                continue

            # Sample a few rows from each CSV
            candidates = rows if len(rows) <= per_file else sample(rows, per_file)

            for row in candidates:
                item = manager.process_csv_row(row, Path(p).name)
                if not item:
                    continue
                checked += 1
                restaurant_date = item.get("RestaurantDate")
                scan_id = item.get("scanId")
                if not restaurant_date or not scan_id:
                    continue

                try:
                    # Query DynamoDB directly via manager.table
                    resp = manager.table.get_item(
                        Key={"RestaurantDate": restaurant_date, "scanId": scan_id}
                    )
                    dynamo_item = resp.get("Item")
                    if not dynamo_item:
                        missing.append(
                            {"RestaurantDate": restaurant_date, "scanId": scan_id}
                        )
                        continue

                    # Spot-check a few important fields
                    for field in [
                        ("restaurantId", int),
                        ("imageURL", str),
                        ("isAudited", str),
                        ("status", str),
                    ]:
                        fname, ftype = field
                        src_val = item.get(fname)
                        dst_val = dynamo_item.get(fname)
                        # only compare if source has value
                        if src_val is not None and dst_val is not None:
                            try:
                                # Normalize types for fair comparison
                                src_norm = (
                                    ftype(src_val) if ftype is not str else str(src_val)
                                )
                                dst_norm = (
                                    ftype(dst_val) if ftype is not str else str(dst_val)
                                )
                                if src_norm != dst_norm:
                                    field_mismatches.append(
                                        {
                                            "RestaurantDate": restaurant_date,
                                            "scanId": scan_id,
                                            "field": fname,
                                            "csv": src_val,
                                            "dynamo": dst_val,
                                        }
                                    )
                            except Exception:
                                # Type normalization failed; log mismatch
                                field_mismatches.append(
                                    {
                                        "RestaurantDate": restaurant_date,
                                        "scanId": scan_id,
                                        "field": fname,
                                        "csv": src_val,
                                        "dynamo": dst_val,
                                    }
                                )
                except Exception as e:
                    field_mismatches.append(
                        {
                            "RestaurantDate": restaurant_date,
                            "scanId": scan_id,
                            "error": str(e),
                        }
                    )
        except Exception as e:
            logger.error(f"❌ Sample verification failed for {p}: {e}")

    return {
        "checked": checked,
        "missing": missing,
        "field_mismatches": field_mismatches,
    }


async def _run_ai_pipeline_on_csv(
    csv_path: str, scan_folder: str, restaurant_id: str
) -> str:
    """Run GenAI Action, Pan recognition, YOLOv8 and Corner detection on the CSV.
    Returns the path to the enriched CSV that should be ingested to DynamoDB.
    """
    try:
        # Ensure repo root in path for audit_automation imports
        _ensure_repo_root_on_path()

        # Import AI steps lazily
        # Robust imports with fallbacks to direct module names
        try:
            from audit_automation.ActionAIAudit import (
                process_csv_and_images as run_genai_action,  # type: ignore
            )
        except Exception:
            from ActionAIAudit import (
                process_csv_and_images as run_genai_action,  # type: ignore
            )
        try:
            from audit_automation.download_registered_pans import (
                download_registered_pan_images,  # type: ignore
            )
        except Exception:
            from download_registered_pans import (
                download_registered_pan_images,  # type: ignore
            )
        try:
            from audit_automation.group_registered_pans import (
                group_registered_pan_images,  # type: ignore
            )
        except Exception:
            from group_registered_pans import (
                group_registered_pan_images,  # type: ignore
            )
        try:
            from audit_automation.panDailyAudit import (
                process_csv as run_pan_recognition,  # type: ignore
            )
        except Exception:
            from panDailyAudit import process_csv as run_pan_recognition  # type: ignore
        try:
            from audit_automation.yolov8_daily_audit_integration import (
                process_venue_with_yolov8,  # type: ignore
            )
        except Exception:
            from yolov8_daily_audit_integration import (
                process_venue_with_yolov8,  # type: ignore
            )
        try:
            from audit_automation.integrate_corner_analysis import (
                add_corner_analysis_to_audit_workflow,  # type: ignore
            )
        except Exception:
            from integrate_corner_analysis import (
                add_corner_analysis_to_audit_workflow,  # type: ignore
            )

        enriched_csv = csv_path

        # Step 1: GenAI Action Audit (sets GenAI Action/Reason, delete flags, etc.)
        try:
            logger.info("🤖 Running GenAI Action Audit…")
            run_genai_action(enriched_csv)
            _log_ai_csv_stats(enriched_csv, stage="after_genai_action")
        except Exception as e:
            logger.warning(f"GenAI Action Audit failed or skipped: {e}")

        # Step 2: Ensure registered pans are available locally, then run GenAI pan recognition
        try:
            logger.info("📥 Downloading registered pan images…")
            download_success = download_registered_pan_images(
                scan_folder, restaurant_id
            )

            if not download_success:
                logger.warning(
                    "⚠️ Pan download failed, attempting to continue with existing data..."
                )
                # Check if we have any existing pan images
                ref_folder = os.path.join(scan_folder, f"{restaurant_id}_register_pans")
                if os.path.exists(ref_folder) and len(os.listdir(ref_folder)) > 0:
                    logger.info("✅ Found existing pan images, continuing...")
                else:
                    logger.error(
                        "❌ No pan images available, skipping pan recognition step"
                    )
                    raise Exception("No registered pan images available")

            ref_folder = os.path.join(scan_folder, f"{restaurant_id}_register_pans")
            logger.info("🗂️  Grouping registered pans…")
            group_registered_pans = group_registered_pan_images(ref_folder)
            logger.info("🍳 Running GenAI pan recognition…")
            run_pan_recognition(enriched_csv, ref_folder, scan_folder)
            _log_ai_csv_stats(enriched_csv, stage="after_genai_pan")
        except Exception as e:
            logger.warning(f"GenAI pan recognition failed or skipped: {e}")

        # Step 3: YOLOv8 pan dimension detection (adds YOLOv8 columns, potential matches)
        try:
            logger.info("🎯 Running YOLOv8 pan dimension detection…")
            enriched_csv = process_venue_with_yolov8(
                enriched_csv, scan_folder, restaurant_id
            )
            _log_ai_csv_stats(enriched_csv, stage="after_yolo")
        except Exception as e:
            logger.warning(f"YOLOv8 processing failed or skipped: {e}")

        # Step 4: Corner embedding analysis (adds corner analysis columns)
        try:
            logger.info("🔍 Running corner embedding analysis…")
            enriched_csv = add_corner_analysis_to_audit_workflow(
                enriched_csv, scan_folder, restaurant_id
            )
            _log_ai_csv_stats(enriched_csv, stage="after_corner")
        except Exception as e:
            logger.warning(f"Corner analysis failed or skipped: {e}")

        return enriched_csv
    except Exception as e:
        logger.warning(f"AI pipeline error; falling back to raw CSV: {e}")
        return csv_path


def _log_ai_csv_stats(csv_path: str, stage: str) -> None:
    """Log how many rows have AI outputs populated in the CSV at a given stage."""
    try:
        import pandas as pd  # local import to avoid hard dep at import time

        df = pd.read_csv(csv_path)

        def _count_nonempty(col_names):
            for name in col_names:
                if name in df.columns:
                    return int(
                        df[name]
                        .astype(str)
                        .str.strip()
                        .replace({"nan": ""})
                        .ne("")
                        .sum()
                    )
            return 0

        genai_pan = _count_nonempty(["GenAI Pan ID", "GenAI_Pan_ID", "genAIPanId"])
        yolo_pan = _count_nonempty(
            ["YOLOv8_Pan_ID", "Yolov8 Pan ID", "YOLOv8_Best_Match_ID", "YOLO_Pan_ID"]
        )
        corner_pan = _count_nonempty(
            ["Corner_Best_Pan_ID", "Corner_Best_Empty_Pan_Match", "Corner_Pan_ID"]
        )
        logger.info(
            f"📊 AI stats ({stage}): genai_pan={genai_pan}, yolo_pan={yolo_pan}, corner_pan={corner_pan}"
        )
    except Exception as e:
        logger.warning(f"Could not log AI CSV stats at {stage}: {e}")


def _get_missing_pan_rows(csv_path: str) -> int:
    """Count rows that have no GenAI, YOLO, or Corner pan suggestion."""
    try:
        import pandas as pd

        df = pd.read_csv(csv_path)

        def nz(col):
            return (
                df[col].astype(str).str.strip().replace({"nan": ""}).ne("")
                if col in df.columns
                else None
            )

        genai = nz("GenAI Pan ID") or nz("GenAI_Pan_ID") or nz("genAIPanId")
        yolo = (
            nz("YOLOv8_Pan_ID")
            or nz("Yolov8 Pan ID")
            or nz("YOLOv8_Best_Match_ID")
            or nz("YOLO_Pan_ID")
            or nz("yoloPanId")
        )
        corner = (
            nz("Corner_Best_Pan_ID")
            or nz("Corner_Best_Empty_Pan_Match")
            or nz("Corner_Pan_ID")
            or nz("cornerPanId")
        )
        has_any = None
        for series in [genai, yolo, corner]:
            if series is not None:
                has_any = series if has_any is None else (has_any | series)
        if has_any is None:
            return 0
        missing = (~has_any).sum()
        return int(missing)
    except Exception as e:
        logger.warning(f"Could not compute missing rows for {csv_path}: {e}")
        return 0


def _load_retry_state(state_path: Path) -> dict:
    try:
        if state_path.exists():
            with state_path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_retry_state(state_path: Path, data: dict) -> None:
    try:
        with state_path.open("w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception as e:
        logger.warning(f"Could not save retry state to {state_path}: {e}")


def _smart_retry_for_missing(csv_files: list[Path], base_dir: Path) -> None:
    """Re-run pan pipeline on CSVs with many missing guesses.

    Strategy:
    - If missing rows > 10 or > 10% of file, and attempts < MAX, and min backoff elapsed → retry
    - Limit retries per CSV per day to avoid infinite loops
    """
    MAX_ATTEMPTS_PER_DAY = 2
    MIN_BACKOFF_SECONDS = 30 * 60  # 30 minutes

    state_path = base_dir / ".ai_retry_state.json"
    state = _load_retry_state(state_path)
    today_key = datetime.now(timezone("America/Los_Angeles")).strftime("%Y-%m-%d")

    for csv_path in csv_files:
        try:
            total_rows = 0
            try:
                import pandas as pd

                total_rows = int(pd.read_csv(csv_path).shape[0])
            except Exception:
                total_rows = 0

            missing = _get_missing_pan_rows(str(csv_path))
            if total_rows > 0:
                missing_ratio = missing / total_rows
            else:
                missing_ratio = 0.0

            # Thresholds
            if missing < 10 and missing_ratio < 0.10:
                continue

            # Retry budgeting
            key = str(csv_path)
            entry = state.get(key, {})
            last_day = entry.get("day")
            attempts = entry.get("attempts", 0)
            last_ts = entry.get("last_ts", 0)

            # Reset attempts if new day
            if last_day != today_key:
                attempts = 0

            if attempts >= MAX_ATTEMPTS_PER_DAY:
                logger.info(f"⏭️  Skipping retry for {csv_path} (max attempts reached)")
                continue

            if (time.time() - float(last_ts)) < MIN_BACKOFF_SECONDS:
                logger.info(f"⏳ Backing off retry for {csv_path}")
                continue

            # Derive scan folder and restaurant id
            scan_folder = str(csv_path.parent)
            restaurant_id = csv_path.parent.parent.name

            logger.info(
                f"🔁 Smart retry on {csv_path}: missing={missing}/{total_rows} ({missing_ratio:.1%})"
            )

            # Retry only pan-related steps (faster)
            try:
                # Ensure references
                try:
                    from audit_automation.download_registered_pans import (
                        download_registered_pan_images,  # type: ignore
                    )
                except Exception:
                    from download_registered_pans import (
                        download_registered_pan_images,  # type: ignore
                    )
                try:
                    from audit_automation.group_registered_pans import (
                        group_registered_pan_images,  # type: ignore
                    )
                except Exception:
                    from group_registered_pans import (
                        group_registered_pan_images,  # type: ignore
                    )
                try:
                    from audit_automation.panDailyAudit import (
                        process_csv as run_pan_recognition,  # type: ignore
                    )
                except Exception:
                    from panDailyAudit import (
                        process_csv as run_pan_recognition,  # type: ignore
                    )
                try:
                    from audit_automation.yolov8_daily_audit_integration import (
                        process_venue_with_yolov8,  # type: ignore
                    )
                except Exception:
                    from yolov8_daily_audit_integration import (
                        process_venue_with_yolov8,  # type: ignore
                    )
                try:
                    from audit_automation.integrate_corner_analysis import (
                        add_corner_analysis_to_audit_workflow,  # type: ignore
                    )
                except Exception:
                    from integrate_corner_analysis import (
                        add_corner_analysis_to_audit_workflow,  # type: ignore
                    )

                download_registered_pan_images(scan_folder, restaurant_id)
                ref_folder = os.path.join(scan_folder, f"{restaurant_id}_register_pans")
                group_registered_pan_images(ref_folder)

                run_pan_recognition(str(csv_path), ref_folder, scan_folder)
                _log_ai_csv_stats(str(csv_path), stage="retry_after_genai_pan")

                updated_csv = process_venue_with_yolov8(
                    str(csv_path), scan_folder, restaurant_id
                )
                _log_ai_csv_stats(updated_csv, stage="retry_after_yolo")

                final_csv = add_corner_analysis_to_audit_workflow(
                    updated_csv, scan_folder, restaurant_id
                )
                _log_ai_csv_stats(final_csv, stage="retry_after_corner")

                # Re-upload enriched CSV
                from audit_automation.scan_dynamo_manager import (
                    ScanDynamoManager,  # type: ignore
                )

                manager = ScanDynamoManager()
                logger.info(f"⬆️  Re-populating DynamoDB from {final_csv}")
                manager.populate_csv(final_csv)
            except Exception as e:
                logger.warning(f"Retry pipeline failed for {csv_path}: {e}")

            # Update state
            state[key] = {
                "day": today_key,
                "attempts": attempts + 1,
                "last_ts": time.time(),
            }
            _save_retry_state(state_path, state)

        except Exception as e:
            logger.warning(f"Smart retry error for {csv_path}: {e}")


def trigger_smart_retry_background() -> None:
    """Entry point for UI-triggered background smart retry.

    Debounced to avoid frequent triggers. Enumerates current CSVs and runs the
    smart retry logic synchronously (call this in a background thread).
    """
    global _last_ui_trigger_ts
    try:
        now = time.time()
        # Debounce: at most once every 5 minutes
        if (now - _last_ui_trigger_ts) < 300:
            logger.info("🛑 UI smart retry trigger debounced")
            return

        _ensure_repo_root_on_path()
        from system.utils.config_loader import load_config  # type: ignore

        config = load_config()
        base_dir = Path(config["audit"]["audit_directory"])  # where zips/CSVs live
        csv_files = [
            p for p in base_dir.rglob("*.csv") if "Venue_Summaries" not in p.name
        ]
        logger.info(f"🟢 UI-triggered smart retry scanning {len(csv_files)} CSV(s)…")
        _smart_retry_for_missing(csv_files, base_dir)
        _last_ui_trigger_ts = now
    except Exception as e:
        logger.warning(f"UI smart retry trigger failed: {e}")


def start_scheduler() -> AsyncIOScheduler:
    """Start and return a configured AsyncIOScheduler with 16:00 and 20:00 jobs (America/Los_Angeles)."""
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    tz = timezone("America/Los_Angeles")
    scheduler = AsyncIOScheduler(timezone=tz)

    # 16:00 (4:00 PM) and 20:00 (8:00 PM) daily
    scheduler.add_job(
        populate_today_audits,
        CronTrigger(hour=16, minute=0, timezone=tz),
        id="populate_audits_16",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=6 * 60 * 60,
    )
    scheduler.add_job(
        populate_today_audits,
        CronTrigger(hour=20, minute=0, timezone=tz),
        id="populate_audits_20",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=6 * 60 * 60,
    )

    # Add a health check job that runs every 30 minutes to check for missed runs
    scheduler.add_job(
        _health_check_and_catch_up,
        CronTrigger(minute="*/30", timezone=tz),  # Every 30 minutes
        id="health_check_catchup",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=5 * 60,  # 5 minutes grace time
    )

    scheduler.start()
    _scheduler = scheduler
    logger.info(
        "🕒 Scheduler started with jobs at 16:00 (4:00 PM) and 20:00 (8:00 PM) America/Los_Angeles"
    )
    logger.info("🔄 Health check job added every 30 minutes for automatic catch-up")
    return scheduler


async def run_once_on_startup() -> None:
    """Smart catch-up on missed runs at startup.

    - Checks if today's data is missing
    - Checks if any scheduled runs were missed
    - Automatically catches up on missed runs
    """
    try:
        tz = timezone("America/Los_Angeles")
        now = datetime.now(tz)

        # Check if today's data is populated
        if _is_today_populated():
            logger.info("✅ Today's audits present; checking for missed runs...")
            await _check_and_catch_up_missed_runs()
        else:
            logger.info("🛠️ Start-up catch-up: today's audits missing; populating once…")
            await populate_today_audits()

    except Exception as e:
        logger.error(f"Startup audit population failed: {e}")


async def _check_and_catch_up_missed_runs() -> None:
    """Check for missed scheduled runs and catch up on them."""
    try:
        tz = timezone("America/Los_Angeles")
        now = datetime.now(tz)
        today = now.date()

        # Define expected run times
        expected_runs = [
            (16, 0),  # 4:00 PM
            (20, 0),  # 8:00 PM
        ]

        missed_runs = []

        for hour, minute in expected_runs:
            expected_time = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

            # If we're past the expected time, check if this run was successful
            if now > expected_time:
                run_status = _check_run_status(today, hour, minute)

                if run_status["status"] != "completed":
                    missed_runs.append((hour, minute))
                    logger.warning(
                        f"🚨 Missed {hour:02d}:{minute:02d} run - Status: {run_status['status']}"
                    )
                else:
                    logger.info(
                        f"✅ {hour:02d}:{minute:02d} run completed at {run_status['run_time']}"
                    )

        if missed_runs:
            logger.warning(f"🚨 Detected {len(missed_runs)} missed runs: {missed_runs}")
            logger.info("🔄 Attempting to catch up on missed runs...")

            # Run the population for each missed run
            for hour, minute in missed_runs:
                logger.info(f"🔄 Catching up on missed {hour:02d}:{minute:02d} run...")
                try:
                    await populate_today_audits()
                    # Record the successful catch-up run
                    catchup_time = now.replace(
                        hour=hour, minute=minute, second=0, microsecond=0
                    )
                    _record_successful_run(catchup_time, "catchup")
                    logger.info(
                        f"✅ Successfully caught up on {hour:02d}:{minute:02d} run"
                    )
                except Exception as e:
                    logger.error(
                        f"❌ Failed to catch up on {hour:02d}:{minute:02d} run: {e}"
                    )
        else:
            logger.info("✅ All scheduled runs appear to be up to date")

    except Exception as e:
        logger.error(f"Failed to check for missed runs: {e}")


async def _health_check_and_catch_up() -> None:
    """Health check job that runs every 30 minutes to automatically catch up on missed runs."""
    try:
        logger.info("🔍 Running health check for missed runs...")
        await _check_and_catch_up_missed_runs()
        logger.info("✅ Health check completed")
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")


async def force_immediate_catch_up() -> dict:
    """Force an immediate catch-up check and return the results."""
    try:
        logger.info("🚨 FORCING IMMEDIATE CATCH-UP CHECK...")
        result = await _check_and_catch_up_missed_runs()
        logger.info("✅ Immediate catch-up check completed")
        return {
            "success": True,
            "message": "Immediate catch-up check completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        logger.error(f"❌ Immediate catch-up check failed: {e}")
        return {
            "success": False,
            "message": f"Immediate catch-up check failed: {str(e)}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def _has_recent_successful_run(
    date: datetime.date, expected_hour: int, expected_minute: int
) -> bool:
    """Check if a recent successful run exists for the given time window.

    This is a heuristic check - we look for recent data that suggests the run completed.
    """
    try:
        # Check if we have recent scan data (within the last few hours)
        # This is a simplified check - in production you might want to track run status in DynamoDB

        cfg = get_config()
        table_name = cfg["dynamodb"]["table_names"]["scan_audit"]
        table = get_dynamodb().Table(table_name)

        # Look for data from today
        date_str = date.strftime("%Y-%m-%d")
        filter_expr = Attr("RestaurantDate").contains(f"#{date_str}")

        # Check if we have recent data (within last 4 hours of expected run time)
        response = table.scan(
            FilterExpression=filter_expr,
            ProjectionExpression="RestaurantDate, #ts",
            ExpressionAttributeNames={"#ts": "timestamp"},
            Limit=10,  # Just check a few records
        )

        if response.get("Items"):
            item_count = len(response["Items"])
            logger.info(
                f"📊 Found {item_count} scan records for {date_str}, assuming run was successful"
            )
            # We have data for today, assume the run was successful
            # In a more sophisticated system, you'd check the actual timestamps
            return True

        logger.info(
            f"📭 No scan records found for {date_str}, run may not have completed"
        )
        return False

    except Exception as e:
        logger.warning(f"Could not determine if run was successful: {e}")
        # On error, assume run was successful to avoid unnecessary re-runs
        return True


def _record_successful_run(run_time: datetime, run_type: str = "scheduled") -> None:
    """Record a successful run in DynamoDB for tracking purposes."""
    try:
        cfg = get_config()
        table_name = cfg["dynamodb"]["table_names"][
            "audit_session"
        ]  # Use audit session table for run tracking

        table = get_dynamodb().Table(table_name)

        # Create a run record
        run_record = {
            "RestaurantDate": f"RUN_TRACKING#{run_time.strftime('%Y-%m-%d')}",
            "RunID": f"{run_time.strftime('%H%M')}_{run_type}_{int(run_time.timestamp())}",
            "RunTime": run_time.isoformat(),
            "RunType": run_type,  # "scheduled", "manual", "catchup"
            "Status": "completed",
            "Timestamp": datetime.now(timezone.utc).isoformat(),
            "DataCount": 0,  # Could be enhanced to track actual data processed
        }

        table.put_item(Item=run_record)
        logger.info(
            f"📝 Recorded successful run: {run_type} at {run_time.strftime('%H:%M')}"
        )

    except Exception as e:
        logger.warning(f"Failed to record successful run: {e}")


def _check_run_status(
    date: datetime.date, expected_hour: int, expected_minute: int
) -> dict:
    """Check the status of a specific scheduled run."""
    try:
        cfg = get_config()
        table_name = cfg["dynamodb"]["table_names"]["audit_session"]

        table = get_dynamodb().Table(table_name)

        # Look for run records for this specific time
        date_str = date.strftime("%Y-%m-%d")
        time_str = f"{expected_hour:02d}{expected_minute:02d}"

        response = table.query(
            KeyConditionExpression="RestaurantDate = :rd",
            FilterExpression="begins_with(RunID, :time)",
            ExpressionAttributeValues={
                ":rd": f"RUN_TRACKING#{date_str}",
                ":time": time_str,
            },
        )

        if response.get("Items"):
            # Found run records, check if any were successful
            for item in response["Items"]:
                if item.get("Status") == "completed":
                    return {
                        "status": "completed",
                        "run_time": item.get("RunTime"),
                        "run_type": item.get("RunType", "unknown"),
                    }

        # If no run records found, check if we have scan data for today
        # This is a fallback for when run tracking isn't working
        if _has_recent_successful_run(date, expected_hour, expected_minute):
            return {
                "status": "completed",
                "run_time": "unknown",
                "run_type": "inferred_from_data",
            }

        return {"status": "not_found", "run_time": None, "run_type": None}

    except Exception as e:
        logger.warning(f"Failed to check run status: {e}")
        # On error, check if we have data as a fallback
        try:
            if _has_recent_successful_run(date, expected_hour, expected_minute):
                return {
                    "status": "completed",
                    "run_time": "unknown",
                    "run_type": "inferred_from_data",
                }
        except Exception:
            pass
        return {"status": "error", "run_time": None, "run_type": None}


def mark_run_as_completed(
    date: datetime.date, hour: int, minute: int, run_type: str = "manual"
) -> bool:
    """Manually mark a run as completed for a specific date and time."""
    try:
        cfg = get_config()
        table_name = cfg["dynamodb"]["table_names"]["audit_session"]

        table = get_dynamodb().Table(table_name)

        # Create a run record
        run_time = datetime.now(timezone("America/Los_Angeles")).replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
        run_record = {
            "RestaurantDate": f"RUN_TRACKING#{date.strftime('%Y-%m-%d')}",
            "RunID": f"{hour:02d}{minute:02d}_{run_type}_{int(run_time.timestamp())}",
            "RunTime": run_time.isoformat(),
            "RunType": run_type,  # "manual", "scheduled", "catchup"
            "Status": "completed",
            "Timestamp": datetime.now(timezone.utc).isoformat(),
            "DataCount": 0,
            "Notes": f"Manually marked as completed by user",
        }

        table.put_item(Item=run_record)
        logger.info(
            f"📝 Manually marked run as completed: {hour:02d}:{minute:02d} on {date.strftime('%Y-%m-%d')}"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to mark run as completed: {e}")
        return False


async def manual_catch_up_runs() -> dict:
    """Manually trigger catch-up on missed runs. Returns status of what was caught up."""
    try:
        tz = timezone("America/Los_Angeles")
        now = datetime.now(tz)
        today = now.date()

        # Define expected run times
        expected_runs = [
            (16, 0),  # 4:00 PM
            (20, 0),  # 8:00 PM
        ]

        missed_runs = []
        caught_up_runs = []

        for hour, minute in expected_runs:
            expected_time = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

            # If we're past the expected time, check if this run was successful
            if now > expected_time:
                run_status = _check_run_status(today, hour, minute)

                if run_status["status"] != "completed":
                    missed_runs.append((hour, minute))

        if missed_runs:
            logger.info(
                f"🔄 Manual catch-up triggered for {len(missed_runs)} missed runs: {missed_runs}"
            )

            # Run the population for each missed run
            for hour, minute in missed_runs:
                logger.info(f"🔄 Catching up on missed {hour:02d}:{minute:02d} run...")
                try:
                    await populate_today_audits()
                    # Record the successful catch-up run
                    catchup_time = now.replace(
                        hour=hour, minute=minute, second=0, microsecond=0
                    )
                    _record_successful_run(catchup_time, "manual_catchup")
                    caught_up_runs.append(f"{hour:02d}:{minute:02d}")
                    logger.info(
                        f"✅ Successfully caught up on {hour:02d}:{minute:02d} run"
                    )
                except Exception as e:
                    logger.error(
                        f"❌ Failed to catch up on {hour:02d}:{minute:02d} run: {e}"
                    )

            return {
                "success": True,
                "message": f"Caught up on {len(caught_up_runs)} missed runs",
                "caught_up_runs": caught_up_runs,
                "total_missed": len(missed_runs),
            }
        else:
            return {
                "success": True,
                "message": "No missed runs detected",
                "caught_up_runs": [],
                "total_missed": 0,
            }

    except Exception as e:
        logger.error(f"Manual catch-up failed: {e}")
        return {
            "success": False,
            "message": f"Manual catch-up failed: {str(e)}",
            "caught_up_runs": [],
            "total_missed": 0,
        }


def get_run_status_summary() -> dict:
    """Get a summary of all run statuses for today."""
    try:
        tz = timezone("America/Los_Angeles")
        now = datetime.now(tz)
        today = now.date()

        # Define expected run times
        expected_runs = [
            (16, 0),  # 4:00 PM
            (20, 0),  # 8:00 PM
        ]

        run_summary = {}

        for hour, minute in expected_runs:
            run_status = _check_run_status(today, hour, minute)
            time_key = f"{hour:02d}:{minute:02d}"
            run_summary[time_key] = run_status

        return {
            "success": True,
            "date": today.strftime("%Y-%m-%d"),
            "timezone": "America/Los_Angeles",
            "runs": run_summary,
            "current_time": now.strftime("%H:%M:%S"),
            "next_run": "20:00" if now.hour < 20 else "16:00 (tomorrow)",
        }

    except Exception as e:
        logger.error(f"Failed to get run status summary: {e}")
        return {
            "success": False,
            "message": f"Failed to get run status: {str(e)}",
            "runs": {},
            "date": None,
            "timezone": None,
            "current_time": None,
            "next_run": None,
        }
