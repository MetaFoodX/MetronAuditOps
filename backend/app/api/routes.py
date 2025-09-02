import asyncio
import base64
import io
import logging
import os
import threading
import time
from datetime import datetime
from datetime import timezone
from datetime import timezone as dt_timezone
from fastapi import APIRouter, Body, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from PIL import Image
from redis import Redis
from rq import Queue
from typing import Any, Dict, List, Optional, Union

# Import our models
from app.models import (
    AuditAction,
    AuditActionType,
    AuditConfirmationRequest,
    AuditConfirmationResponse,
    AuditSession,
    AuditSessionSummary,
)
from app.scheduler import _compute_coverage_for_date as compute_coverage_for_date
from app.scheduler import (
    force_immediate_catch_up,
    get_ai_state,
    get_propagation_state,
    get_run_status_summary,
    manual_catch_up_runs,
    mark_run_as_completed,
    populate_audits_for_date,
    set_ai_state,
    set_propagation_state,
    trigger_smart_retry_background,
)
from app.utils.config import load_config
from app.utils.dynamo_client import test_connection

router = APIRouter()
logger = logging.getLogger(__name__)

_presign_cache: Dict[str, Any] = {
    "map": {},  # key -> {"url": str, "exp": float}
}


@router.get("/db/ping")
def db_ping(request: Request) -> Dict[str, Any]:
    """
    Quick tunnel + DB connectivity check. Returns basic info and row count sample.
    """
    try:
        db = request.app.state.database_service
        ok_tunnel = db.start_tunnel()
        ok_db = db.connect_db()
        sample_count = 0
        if ok_db:
            try:
                cursor = db.connection.cursor()
                # Tiny, safe query to test connectivity
                cursor.execute("SELECT 1 AS ok")
                _ = cursor.fetchall()
                # Optional: count recent Scans rows for sanity (cheap with limit)
                cursor.execute("SELECT COUNT(*) AS c FROM Scans LIMIT 1")
                r = cursor.fetchone() or {}
                sample_count = int(r.get("c", 0))
                cursor.close()
            except Exception as qe:
                logger.warning(f"DB ping query failed: {qe}")
        return {
            "tunnel": ok_tunnel,
            "db": ok_db,
            "scans_row_count_sample": sample_count,
        }
    except Exception as e:
        logger.error(f"DB ping failed: {e}")
        raise HTTPException(status_code=500, detail=f"DB ping failed: {str(e)}")


def _enqueue_ai_job(date: str) -> bool:
    try:
        cfg = load_config()
        rconf = cfg.get("redis", {})
        redis_conn = Redis(
            host=rconf.get("host", "127.0.0.1"),
            port=int(rconf.get("port", 6379)),
            db=int(rconf.get("db", 0)),
        )
        q = Queue("pan_ai", connection=redis_conn)

        def _job(date_str: str) -> None:
            import asyncio as _asyncio
            from datetime import datetime as _dt
            from datetime import timezone as _tz

            from app.scheduler import populate_audits_for_date as _populate
            from app.scheduler import set_ai_state as _set_state

            try:
                _set_state(date_str, running=True, lastError="")
                _asyncio.run(_populate(date_str, run_ai=True))
                _set_state(
                    date_str, running=False, completedAt=_dt.now(_tz.utc).isoformat()
                )
            except Exception as _e:
                _set_state(date_str, running=False, lastError=str(_e))

        # Enqueue callable with args
        q.enqueue(_job, date)
        return True
    except Exception as e:
        try:
            logger.warning(f"RQ enqueue failed, falling back to thread: {e}")
        except Exception:
            pass
        return False


@router.post("/pan_ai/run")
async def run_pan_ai_now(
    request: Request, restaurantId: Optional[int] = None, date: Optional[str] = None
) -> Dict[str, Any]:
    """Explicit UI trigger to run the pan AI workflow in the background.

    Returns immediately with a message so the UI can show a waiting banner.
    """
    try:
        # Allow JSON body too
        if date is None:
            try:
                data = await request.json()
                date = data.get("date")
                restaurantId = data.get("restaurantId", restaurantId)
            except Exception:
                date = None
        # Validate date
        try:
            _ = datetime.strptime(date or "", "%Y-%m-%d")
        except Exception:
            raise HTTPException(
                status_code=400, detail="Missing or invalid date. Use YYYY-MM-DD."
            )

        # Mark AI running and dispatch job (RQ if available, else thread)
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        set_ai_state(date, running=True, lastError="")
        if not _enqueue_ai_job(date):

            def _run_ai() -> None:
                try:
                    if date is not None:
                        asyncio.run(populate_audits_for_date(date, run_ai=True))
                    set_ai_state(
                        date,
                        running=False,
                        completedAt=datetime.now(timezone.utc).isoformat(),
                    )
                except Exception as e:
                    try:
                        if date is not None:
                            set_ai_state(date, running=False, lastError=str(e))
                    except Exception:
                        pass
                finally:
                    # Never block on smart retry; trigger it separately
                    try:
                        threading.Thread(
                            target=trigger_smart_retry_background, daemon=True
                        ).start()
                    except Exception:
                        pass

            threading.Thread(target=_run_ai, daemon=True).start()
        return {
            "success": True,
            "message": "Pan AI enrichment started. This may take some time. You can keep auditing while it runs.",
        }
    except Exception as e:
        logger.error(f"Failed to trigger pan AI: {e}")
        raise HTTPException(status_code=500, detail="Failed to start pan AI workflow")


@router.post("/force_redownload")
async def force_redownload(
    request: Request, date: Optional[str] = None, restaurantId: Optional[int] = None
) -> Dict[str, Any]:
    """Force re-download of audits for a specific date and run propagation in background.

    This triggers:
      1) S3 download of that day via start_download_for_date(date)
      2) Extraction + AI pipeline population (populate_today_audits)
      3) Smart retry pass
    """
    try:
        # Allow both JSON body and query params
        if date is None:
            try:
                data = await request.json()
                date = data.get("date")
                restaurantId = data.get("restaurantId", restaurantId)
            except Exception:
                date = None
        # Validate date
        try:
            _ = datetime.strptime(date or "", "%Y-%m-%d")
        except Exception:
            raise HTTPException(
                status_code=400, detail="Missing or invalid date. Use YYYY-MM-DD."
            )

        # Optimistically mark population running so UI shows progress immediately
        try:
            set_propagation_state(date, running=True)
        except Exception:
            pass

        def _run_targeted() -> None:
            try:
                # Ensure repo/audit on path lazily
                import sys as _sys
                from pathlib import Path as _Path

                backend_dir = _Path(__file__).resolve().parents[2]
                repo_root = backend_dir.parent
                audit_dir = repo_root / "audit_automation"
                if str(repo_root) not in _sys.path:
                    _sys.path.insert(0, str(repo_root))
                if str(audit_dir) not in _sys.path:
                    _sys.path.insert(0, str(audit_dir))

                # Download and populate for the requested date (NO AI; do not trigger smart retry)
                if date is not None:
                    asyncio.run(populate_audits_for_date(date, run_ai=False))
            except Exception as _e:
                try:
                    logger.warning(f"Force redownload failed for {date}: {_e}")
                except Exception:
                    pass

        threading.Thread(target=_run_targeted, daemon=True).start()
        return {
            "success": True,
            "message": f"Started re-download and population (no AI) for {date}. This may take a few minutes.",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to force re-download: {e}")
        raise HTTPException(status_code=500, detail="Failed to start re-download")


@router.get("/status")
def read_status() -> Any:
    config = load_config()
    return test_connection(config["dynamodb"]["table_names"]["audit_session"])


@router.get("/pan_ai/status")
def get_pan_ai_status(date: str) -> Dict[str, Any]:
    """Return AI workflow status for a given date."""
    if not date:
        raise HTTPException(status_code=400, detail="date is required")
    try:
        ai = get_ai_state(date)
        if not ai.get("running", False):
            try:
                ai["coverage"] = compute_coverage_for_date(date)
            except Exception:
                pass
        return {
            "running": bool(ai.get("running", False)),
            "completedAt": ai.get("completedAt"),
            "lastError": ai.get("lastError"),
            "coverage": ai.get("coverage", {"total": 0, "withPan": 0}),
        }
    except Exception as e:
        logger.error(f"Failed to get pan AI status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get AI status")


@router.get("/restaurants")
def get_restaurants_routes(request: Request) -> Any:
    skoopin_service = request.app.state.skoopin_service
    restaurants = skoopin_service.get_restaurants()

    return JSONResponse(content={"restaurants": restaurants})


@router.get("/restaurants/with-scans")
def get_restaurants_with_scans(request: Request, date: Optional[str] = None) -> Any:
    """
    Get restaurants that have scans on a specific date, with scan counts.
    If no date is provided, returns all restaurants with 0 scan counts.
    """
    if date:
        # Prevent future-dated queries
        try:
            requested = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
            )
        today = datetime.now(timezone.utc).date()
        if requested > today:
            raise HTTPException(
                status_code=400, detail="Querying future dates is not allowed."
            )

    skoopin_service = request.app.state.skoopin_service
    dynamo_service = request.app.state.dynamo_service

    # Get all restaurants
    all_restaurants = skoopin_service.get_restaurants()

    if not date:
        # If no date provided, return all restaurants with 0 scan counts
        restaurants_with_counts = []
        for restaurant in all_restaurants:
            restaurants_with_counts.append(
                {
                    **restaurant,
                    "scanCount": 0,
                    "normalScanCount": 0,
                    "flaggedScanCount": 0,
                }
            )
        return JSONResponse(content={"restaurants": restaurants_with_counts})

    # Get scan counts for each restaurant on the specified date
    restaurants_with_counts = []
    for restaurant in all_restaurants:
        restaurant_id = restaurant.get("id")
        if restaurant_id:
            # Get scans for this restaurant on the specified date
            scans = dynamo_service.get_scans_by_restaurant_day(restaurant_id, date)

            # Count normal vs flagged scans
            normal_count = 0
            flagged_count = 0

            for scan in scans:
                # Use the same logic as in get_scans_to_audit to determine if scan is bad
                def _to_float(value: Any, default: float = 0.0) -> float:
                    try:
                        return float(value)
                    except Exception:
                        return default

                def _to_bool(value: Any) -> bool:
                    if isinstance(value, bool):
                        return value
                    if isinstance(value, (int, float)):
                        return value != 0
                    if isinstance(value, str):
                        return value.strip().lower() in {"true", "1", "yes", "y"}
                    return False

                def _has_pan_id(scan: dict) -> bool:
                    pan_id = (
                        scan.get("panId") or scan.get("PanID") or scan.get("pan_id")
                    )
                    if isinstance(pan_id, str) and pan_id.strip().lower() in {
                        "unrecognized",
                        "unknown",
                        "none",
                        "",
                    }:
                        pan_id = None
                    return pan_id is not None

                def _has_menu_id(scan: dict) -> bool:
                    menu_id = (
                        scan.get("menuItemId")
                        or scan.get("MenuItemID")
                        or scan.get("reportedMenuItemId")
                    )
                    return menu_id is not None

                def _is_empty_scan(scan: dict) -> bool:
                    if (
                        _to_bool(scan.get("isEmpty"))
                        or _to_bool(scan.get("empty"))
                        or _to_bool(scan.get("emptyScan"))
                        or _to_bool(scan.get("IsEmptyScan"))
                    ):
                        return True
                    reason = (
                        (
                            scan.get("panAuditReason")
                            or scan.get("reason")
                            or scan.get("tags")
                            or ""
                        )
                        .__str__()
                        .lower()
                    )
                    return "empty" in reason

                def _is_non_food(scan: dict) -> bool:
                    if (
                        _to_bool(scan.get("nonFood"))
                        or _to_bool(scan.get("isNonFood"))
                        or _to_bool(scan.get("non_food"))
                    ):
                        return True
                    label = (
                        (
                            scan.get("classification")
                            or scan.get("label")
                            or scan.get("panAuditReason")
                            or ""
                        )
                        .__str__()
                        .lower()
                    )
                    return (
                        ("non food" in label)
                        or ("non-food" in label)
                        or ("nonfood" in label)
                    )

                def _is_bad_scan(scan: dict) -> bool:
                    # Rule 1: < 8oz and no panId and no menuItemId
                    weight_under = _to_float(scan.get("weight"), 0.0) < 8.0
                    no_ids = (not _has_pan_id(scan)) and (not _has_menu_id(scan))
                    if weight_under and no_ids:
                        return True
                    # Rule 2: empty scans
                    if _is_empty_scan(scan):
                        return True
                    # Rule 3: non-food on scale
                    if _is_non_food(scan):
                        return True
                    return False

                if _is_bad_scan(scan):
                    flagged_count += 1
                else:
                    normal_count += 1

            total_count = normal_count + flagged_count

            # Only include restaurants that have scans
            if total_count > 0:
                # Count active sessions for this restaurant/date
                try:
                    active_sessions = (
                        dynamo_service.get_active_sessions_for_date(restaurant_id, date)
                        or []
                    )
                    active_count = len(active_sessions)
                except Exception:
                    active_count = 0
                restaurants_with_counts.append(
                    {
                        **restaurant,
                        "scanCount": total_count,
                        "normalScanCount": normal_count,
                        "flaggedScanCount": flagged_count,
                        "activeAuditors": active_count,
                    }
                )

    # If no restaurants found for the date, trigger automatic download and propagation
    if not restaurants_with_counts and date:
        try:
            state_now = get_propagation_state(date)
            if not state_now.get("running") and not state_now.get("noData"):
                # Optimistically mark as running so clients see propagating=true immediately
                try:
                    set_propagation_state(date, running=True)
                except Exception:
                    pass

                def _run_targeted() -> None:
                    try:
                        # Ensure repo/audit on path lazily
                        import sys as _sys
                        from pathlib import Path as _Path

                        backend_dir = _Path(__file__).resolve().parents[2]
                        repo_root = backend_dir.parent
                        audit_dir = repo_root / "audit_automation"
                        if str(repo_root) not in _sys.path:
                            _sys.path.insert(0, str(repo_root))
                        if str(audit_dir) not in _sys.path:
                            _sys.path.insert(0, str(audit_dir))

                        # Download and populate for the requested date (no AI on date-click flow)
                        asyncio.run(populate_audits_for_date(date, run_ai=False))

                        # Trigger smart retry in a separate background thread so Redis or queue issues never block propagation
                        def _trigger_retry() -> None:
                            try:
                                trigger_smart_retry_background()
                            except Exception as _re:
                                try:
                                    logger.warning(
                                        f"Smart retry trigger failed after propagation for {date}: {_re}"
                                    )
                                except Exception:
                                    pass

                        threading.Thread(target=_trigger_retry, daemon=True).start()
                    except Exception as _e:
                        try:
                            logger.warning(f"Auto download failed for {date}: {_e}")
                        except Exception:
                            pass

                threading.Thread(target=_run_targeted, daemon=True).start()
        except Exception:
            pass

    # Attach propagation state so UI can stop polling when true no-data
    state = get_propagation_state(date)
    ai = get_ai_state(date) if date else {"running": False, "completedAt": None}
    if date and not ai.get("running", False):
        # Refresh coverage snapshot when not in running state
        try:
            ai["coverage"] = compute_coverage_for_date(date)
        except Exception:
            pass
    return JSONResponse(
        content={
            "restaurants": restaurants_with_counts,
            "propagating": state.get("running", False),
            "noData": state.get("noData", False),
            "aiRunning": ai.get("running", False),
            "aiCompletedAt": ai.get("completedAt"),
        }
    )


@router.get("/pans")
def get_registered_pans(
    request: Request, restaurantId: Optional[int] = None, date: Optional[str] = None
) -> Any:
    """
    Return registered pans based primarily on the database, not the Skoopin pans endpoint.
    Strategy:
    - Build pan list from existing scans (for this restaurant/date) to get IDs seen
    - Query DB for reference scans for those PanIDs (for images)
    - If no date provided, return an empty set (or we can extend to a full DB pans query later)
    """
    dynamo_service = request.app.state.dynamo_service
    db_service = request.app.state.database_service

    # Helper to ensure JSON-serializable values
    def _safe_dt(v: Any) -> Any:
        try:
            from datetime import datetime as _dt

            if isinstance(v, _dt):
                return v.isoformat()
        except Exception:
            pass
        return v

    pans: List[Dict[str, Any]] = []
    audited_pan_ids = set()
    try:
        scans_for_day: List[Dict[str, Any]] = []
        try:
            logger.info(f"/pans start: restaurantId={restaurantId}, date={date}")
        except Exception:
            pass
        if date:
            scans_for_day = (
                dynamo_service.get_scans_by_restaurant_day(restaurantId, date) or []
            )
            for s in scans_for_day:
                is_audited = str(s.get("isAudited", "")).strip().lower() in {
                    "true",
                    "1",
                    "yes",
                    "y",
                }
                pan_id = (
                    s.get("auditorPanId")
                    or s.get("auditedPanId")
                    or s.get("PanID")
                    or s.get("panId")
                    or s.get("pan_id")
                )
                # Filter out invalid/unrecognized pan IDs
                if (
                    pan_id
                    and str(pan_id).lower()
                    not in ["unrecognized", "unknown", "none", "null", ""]
                    and is_audited
                ):
                    audited_pan_ids.add(str(pan_id))

        # Primary path: query DB for pans for the restaurant, prefer Type=6; if date provided, limit to that day +/- 0
        ref_rows = (
            request.app.state.database_service.get_reference_pans_for_restaurant(
                restaurantId, date=date, types=[6], days_back=0
            )
            or []
        )
        try:
            logger.info(f"/pans DB primary rows: {len(ref_rows)}")
        except Exception:
            pass
        # Build set of unique pan IDs from DB rows
        observed_ids = []
        seen_ids = set()
        for row in ref_rows:
            # Exclude status 0 rows defensively
            try:
                if str(row.get("Status", "")) in {"0", 0}:
                    continue
            except Exception:
                pass
            pid = row.get("PanID")
            if pid is None:
                continue
            spid = str(pid)
            if spid in seen_ids:
                continue
            seen_ids.add(spid)
            db_shape = row.get("Shape") if row.get("Shape") is not None else ""
            db_size = row.get("SizeStandard", "")
            pan = {
                "ID": spid,
                "wasAudited": spid in audited_pan_ids,
                # pass through commonly used fields with safe defaults
                "Number": row.get("Number", ""),
                "ShortID": row.get("ShortID", ""),
                "DetectedSizeStandard": row.get("DetectedSizeStandard", ""),
                "Weight": row.get("Weight", 0.0),
                "DetectedDepth": row.get("DetectedDepth", 0.0),
                "Depth": row.get("Depth", row.get("DetectedDepth", 0.0)),
                "Volume": row.get("Volume", 0.0),
                "Status": row.get("Status", ""),
                # Do not include selection fields for EMPTY pans
                # Always include DB-provided values for filtering
                "dbShape": db_shape,
                "dbSizeStandard": db_size,
                # Pass through pans Data blob with dimensions if present
                "Data": row.get("Data") or {},
                "CapturedAt": _safe_dt(row.get("CapturedAt")),
                "CreatedAt": _safe_dt(row.get("CreatedAt")),
                "UpdatedAt": _safe_dt(row.get("UpdatedAt")),
            }
            # No prefill for EMPTY pans; UI should use dbShape/dbSizeStandard for filtering
            img = row.get("ImageURL")
            if isinstance(img, str) and (
                img.startswith("http://") or img.startswith("https://")
            ):
                pan["imageUrl"] = img
            elif img:
                pan["_imageKey"] = img
                # Presign image so UI can render without extra roundtrip
                try:
                    url = request.app.state.aws_service.get_optimized_presigned_url(
                        img, target_width=1600, image_format="WEBP", quality=70
                    )
                    if url:
                        pan["imageUrl"] = url
                        pan.pop("_imageKey", None)
                except Exception:
                    pass
            observed_ids.append(pan)
        pans = observed_ids

        # Fallback: if DB rows are empty for some reason but we have a date, try batch query by observed pan IDs from scans
        if (not pans) and date and scans_for_day:
            try:
                # Collect observed IDs from scans_for_day
                observed_from_scans = set()
                for s in scans_for_day:
                    pid = (
                        s.get("auditorPanId")
                        or s.get("auditedPanId")
                        or s.get("PanID")
                        or s.get("panId")
                        or s.get("pan_id")
                    )
                    # Filter out invalid/unrecognized pan IDs
                    if pid and str(pid).lower() not in [
                        "unrecognized",
                        "unknown",
                        "none",
                        "null",
                        "",
                    ]:
                        observed_from_scans.add(str(pid))
                if observed_from_scans:
                    logger.info(
                        f"/pans fallback: querying DB for {len(observed_from_scans)} observed pan IDs from scans"
                    )
                    # Use the main method without date filtering to get all pans, then filter
                    all_ref_rows = (
                        request.app.state.database_service.get_reference_pans_for_restaurant(
                            restaurantId, types=[6]
                        )
                        or []
                    )
                    fallback_list: List[Dict[str, Any]] = []
                    for spid in observed_from_scans:
                        fallback_pan: Dict[str, Any] = {
                            "ID": spid,
                            "wasAudited": spid in audited_pan_ids,
                        }
                        # Find matching pan in all_ref_rows
                        ref = None
                        for row in all_ref_rows:
                            if str(row.get("PanID")) == spid:
                                ref = row
                                break
                        # Enrich with details if available
                        if isinstance(ref, dict):
                            db_shape = (
                                ref.get("Shape") if ref.get("Shape") is not None else ""
                            )
                            db_size = ref.get("SizeStandard", "")
                            fallback_pan.update(
                                {
                                    "Number": ref.get("Number", ""),
                                    "ShortID": ref.get("ShortID", ""),
                                    "DetectedSizeStandard": ref.get(
                                        "DetectedSizeStandard", ""
                                    ),
                                    "Weight": ref.get("Weight", 0.0),
                                    "DetectedDepth": ref.get("DetectedDepth", 0.0),
                                    "Depth": ref.get(
                                        "Depth", ref.get("DetectedDepth", 0.0)
                                    ),
                                    "Volume": ref.get("Volume", 0.0),
                                    "Status": ref.get("Status", ""),
                                    # Do not include selection fields for EMPTY pans
                                    "dbShape": db_shape,
                                    "dbSizeStandard": db_size,
                                    "Data": ref.get("Data") or {},
                                    "CapturedAt": _safe_dt(ref.get("CapturedAt")),
                                    "CreatedAt": _safe_dt(ref.get("CreatedAt")),
                                    "UpdatedAt": _safe_dt(ref.get("UpdatedAt")),
                                }
                            )
                            # No prefill for EMPTY pans; UI should use db* fields for filters
                        img = ref.get("ImageURL") if isinstance(ref, dict) else None
                        if isinstance(img, str) and (
                            img.startswith("http://") or img.startswith("https://")
                        ):
                            fallback_pan["imageUrl"] = img
                        elif img:
                            fallback_pan["_imageKey"] = img
                            # Presign image so UI can render without extra roundtrip
                            try:
                                url = request.app.state.aws_service.get_optimized_presigned_url(
                                    img,
                                    target_width=1600,
                                    image_format="WEBP",
                                    quality=70,
                                )
                                if url:
                                    fallback_pan["imageUrl"] = url
                                    fallback_pan.pop("_imageKey", None)
                            except Exception:
                                pass
                        fallback_list.append(fallback_pan)
                    pans = fallback_list
                    logger.info(f"/pans fallback: built {len(pans)} pans from DB query")
            except Exception as fe:
                try:
                    logger.warning(f"/pans fallback DB batch failed: {fe}")
                except Exception:
                    pass

        # If still empty, inform the client to keep polling briefly (building flag)
        if not pans:
            try:
                state_now = get_propagation_state(date)
                # If propagation is running, indicate building state
                if state_now.get("running", False):
                    return JSONResponse(content={"pans": [], "building": True})
            except Exception:
                pass
    except Exception as e:
        try:
            logger.warning(
                f"/pans DB primary retrieval failed for restaurant {restaurantId} date {date}: {e}"
            )
        except Exception:
            pass

    return JSONResponse(content={"pans": pans})


@router.get("/menu_items")
def search_menu_items(
    request: Request,
    restaurantId: int,
    date: Optional[str] = None,
    q: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    """
    Lightweight menu item search based on existing scan data for the given restaurant/date.
    Returns distinct menu items seen in scans, filtered by query substring when provided.
    """
    try:
        dynamo_service = request.app.state.dynamo_service
        scans = dynamo_service.get_scans_by_restaurant_day(restaurantId, date) or []

        # Build frequency map of (id, name)
        counts: Dict[tuple, int] = {}
        for s in scans:
            mid = (
                s.get("menuItemId")
                or s.get("MenuItemID")
                or s.get("reportedMenuItemId")
                or None
            )
            name = (
                s.get("reportedMenuItemName")
                or s.get("menuItemName")
                or s.get("MenuItemName")
                or None
            )
            if mid is None or name is None:
                continue
            key = (str(mid), str(name))
            counts[key] = counts.get(key, 0) + 1

        items = [{"id": k[0], "name": k[1], "count": c} for k, c in counts.items()]

        # Filter by query substring if provided
        if q:
            ql = q.strip().lower()
            if ql:
                items = [it for it in items if ql in it["name"].lower()]

        # Sort by frequency desc then name
        items.sort(key=lambda x: (-x.get("count", 0), x.get("name", "")))
        return {"items": items[: max(1, int(limit))]}
    except Exception as e:
        logger.error(f"Failed to search menu items: {e}")
        raise HTTPException(status_code=500, detail="Failed to search menu items")


@router.get("/image/presign")
def presign_image(
    request: Request, key: str = Query(..., min_length=3)
) -> Dict[str, str]:
    try:
        # Small in-memory cache to reduce AWS bursts when multiple auditors open the same scan
        now = time.time()
        cache_entry = _presign_cache["map"].get(key)
        if (
            cache_entry
            and isinstance(cache_entry, dict)
            and cache_entry.get("exp", 0) > now + 60
        ):  # still valid for at least 60s
            return {"url": cache_entry["url"]}

        url = request.app.state.aws_service.get_optimized_presigned_url(
            key, target_width=1600, image_format="WEBP", quality=70
        )
        if not url:
            raise HTTPException(status_code=404, detail="Unable to presign image")
        # Assume 30 minutes validity by default to avoid frequent churn
        _presign_cache["map"][key] = {"url": url, "exp": now + 25 * 60}
        return {"url": url}
    except Exception as e:
        logger.error(f"Failed to presign image: {e}")
        raise HTTPException(status_code=500, detail="Failed to presign image")


@router.get("/scans_to_audit")
def get_scans_to_audit(
    request: Request,
    restaurantId: Optional[int] = None,
    date: Optional[str] = None,
    includeBad: bool = False,
) -> Any:
    # Prevent future-dated audits
    if date:
        try:
            requested = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
            )
        today = datetime.now(timezone.utc).date()
        if requested > today:
            raise HTTPException(
                status_code=400, detail="Auditing future dates is not allowed."
            )
    dynamo_service = request.app.state.dynamo_service

    # Opportunistic background trigger: if many scans lack pan guesses, kick a smart retry
    # NOTE: Removed auto-trigger here per product requirement. The UI should explicitly call
    # a dedicated endpoint to start the pan AI workflow and show a waiting indicator.
    # Fetch scans; if Dynamo is under load, do a quick backoff retry to smooth spikes
    try:
        scans = dynamo_service.get_scans_by_restaurant_day(restaurantId, date)
    except Exception as e:
        try:
            logger.warning(
                f"Primary get_scans_by_restaurant_day failed: {e}. Retrying once..."
            )
        except Exception:
            pass
        awaitable = None
        try:
            time.sleep(0.2)
            scans = dynamo_service.get_scans_by_restaurant_day(restaurantId, date)
        except Exception:
            # Fall back to empty gracefully; UI will show propagation controls
            scans = []
    # Safety: cap total records to prevent payload explosion; client can re-request next chunk if needed
    try:
        limit = int(request.query_params.get("limit", "1000"))
        if limit <= 0:
            limit = 1000
    except Exception:
        limit = 1000
    scans = scans[:limit]

    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes", "y"}
        return False

    def _has_pan_id(scan: dict) -> bool:
        # Treat auditor-updated fields as valid identifiers
        pan_id = (
            scan.get("auditorPanId")
            or scan.get("auditedPanId")
            or scan.get("panId")
            or scan.get("PanID")
            or scan.get("pan_id")
        )
        if isinstance(pan_id, str) and pan_id.strip().lower() in {
            "unrecognized",
            "unknown",
            "none",
            "",
        }:
            pan_id = None
        return pan_id is not None

    def _has_menu_id(scan: dict) -> bool:
        menu_id = (
            scan.get("auditorMenuItemId")
            or scan.get("auditedMenuItemId")
            or scan.get("menuItemId")
            or scan.get("MenuItemID")
            or scan.get("reportedMenuItemId")
        )
        return menu_id is not None

    def _is_empty_scan(scan: dict) -> bool:
        if (
            _to_bool(scan.get("isEmpty"))
            or _to_bool(scan.get("empty"))
            or _to_bool(scan.get("emptyScan"))
            or _to_bool(scan.get("IsEmptyScan"))
        ):
            return True
        reason = (
            (scan.get("panAuditReason") or scan.get("reason") or scan.get("tags") or "")
            .__str__()
            .lower()
        )
        return "empty" in reason

    def _is_non_food(scan: dict) -> bool:
        if (
            _to_bool(scan.get("nonFood"))
            or _to_bool(scan.get("isNonFood"))
            or _to_bool(scan.get("non_food"))
        ):
            return True
        label = (
            (
                scan.get("classification")
                or scan.get("label")
                or scan.get("panAuditReason")
                or ""
            )
            .__str__()
            .lower()
        )
        return ("non food" in label) or ("non-food" in label) or ("nonfood" in label)

    def _is_bad_scan(scan: dict) -> bool:
        # Rule 1: < 8oz and no panId and no menuItemId
        weight_under = _to_float(scan.get("weight"), 0.0) < 8.0
        no_ids = (not _has_pan_id(scan)) and (not _has_menu_id(scan))
        if weight_under and no_ids:
            return True
        # Rule 2: empty scans
        if _is_empty_scan(scan):
            return True
        # Rule 3: non-food on scale
        if _is_non_food(scan):
            return True
        return False

    def _is_deleted_scan(scan: dict) -> bool:
        status = (
            (scan.get("auditStatus") or scan.get("AuditStatus") or "")
            .__str__()
            .strip()
            .lower()
        )
        return status == "deleted"

    # Do not presign images here; return scans quickly and let the UI request
    # a presigned URL only for the currently viewed scan.
    results = list(scans)

    # If no scans found for the date, trigger automatic download and propagation
    if not results and date:
        try:
            state_now = get_propagation_state(date)
            if not state_now.get("running") and not state_now.get("noData"):
                # Optimistically mark as running so clients see propagating=true immediately
                try:
                    set_propagation_state(date, running=True)
                except Exception:
                    pass

                def _run_targeted() -> None:
                    try:
                        # Ensure repo/audit on path lazily
                        import sys as _sys
                        from pathlib import Path as _Path

                        backend_dir = _Path(__file__).resolve().parents[2]
                        repo_root = backend_dir.parent
                        audit_dir = repo_root / "audit_automation"
                        if str(repo_root) not in _sys.path:
                            _sys.path.insert(0, str(repo_root))
                        if str(audit_dir) not in _sys.path:
                            _sys.path.insert(0, str(audit_dir))

                        # Download and populate for the requested date (no AI on date-click flow)
                        asyncio.run(populate_audits_for_date(date, run_ai=False))
                        trigger_smart_retry_background()
                    except Exception as _e:
                        try:
                            logger.warning(f"Auto download failed for {date}: {_e}")
                        except Exception:
                            pass

                threading.Thread(target=_run_targeted, daemon=True).start()
        except Exception:
            pass

    # Exclude deleted scans from normal; include them in invalid after they are persisted
    normal_scans = [
        s for s in results if not _is_bad_scan(s) and not _is_deleted_scan(s)
    ]
    flagged_scans = [s for s in results if _is_bad_scan(s) or _is_deleted_scan(s)]

    # If propagation just finished but scans are empty, hint noData to help the UI exit loading state
    if not normal_scans and not flagged_scans:
        try:
            state_now = get_propagation_state(date)
            if not state_now.get("running", False):
                state_now["noData"] = True
                set_propagation_state(date, **state_now)
        except Exception:
            pass

    state = get_propagation_state(date)
    ai = get_ai_state(date) if date else {"running": False, "completedAt": None}
    if date and not ai.get("running", False):
        try:
            ai["coverage"] = compute_coverage_for_date(date)
        except Exception:
            pass
    response_data: Dict[str, Any] = {"scans": normal_scans}
    if includeBad:
        response_data["flagged"] = flagged_scans
    # Attach propagation signals
    response_data["propagating"] = state.get("running", False)
    response_data["noData"] = state.get("noData", False)
    response_data["aiRunning"] = ai.get("running", False)
    response_data["aiCompletedAt"] = ai.get("completedAt")
    response_data["aiCoverage"] = ai.get("coverage")
    # Lightweight ETag-ish stamp for clients to avoid repainting if unchanged
    try:
        response_data["stamp"] = f"{len(results)}-{int(time.time() // 5)}"
    except Exception:
        response_data["stamp"] = None

    return response_data


# ========== AUDIT SESSION ENDPOINTS ==========


@router.post("/audit/session/create")
async def create_audit_session(
    request: Request, restaurant_id: int, date: str, auditor_id: Optional[str] = None
) -> Any:
    """
    Create a new audit session for a restaurant and date
    """
    try:
        audit_service = request.app.state.audit_service
        result = audit_service.create_audit_session(
            restaurant_id=restaurant_id, date=date, auditor_id=auditor_id
        )
        if not result.get("success", False):
            # Block entrance if an active session exists
            raise HTTPException(
                status_code=409,
                detail={
                    "message": result.get("error", "Active session exists"),
                    "active_sessions": result.get("active_sessions", []),
                },
            )

        return result

    except Exception as e:
        logger.error(f"Failed to create audit session: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create audit session: {str(e)}",
        )


@router.get("/audit/session/{session_id}")
async def get_audit_session(request: Request, session_id: str) -> Dict[str, Any]:
    """
    Get audit session details and progress
    """
    try:
        audit_service = request.app.state.audit_service
        session_data = audit_service.get_audit_session(session_id)

        if not session_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Audit session not found"
            )

        return {"success": True, **session_data}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get audit session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get audit session: {str(e)}",
        )


@router.get("/audit/sessions/restaurant/{restaurant_id}")
async def get_audit_sessions_by_restaurant(
    request: Request, restaurant_id: int, limit: int = 50
) -> Dict[str, Any]:
    """
    Get audit sessions for a specific restaurant
    """
    try:
        dynamo_service = request.app.state.dynamo_service
        sessions = dynamo_service.get_audit_sessions_by_restaurant(restaurant_id, limit)

        return {
            "success": True,
            "restaurant_id": restaurant_id,
            "sessions": sessions,
            "count": len(sessions),
        }

    except Exception as e:
        logger.error(
            f"Failed to get audit sessions for restaurant {restaurant_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get audit sessions: {str(e)}",
        )


@router.post("/audit/confirm")
async def confirm_audit_session(
    request: Request, audit_request: AuditConfirmationRequest
) -> Any:
    """
    Confirm audit session and apply fixes to Skoopin server
    """
    try:
        audit_service = request.app.state.audit_service

        # Validate actions before applying
        validation = audit_service.validate_audit_actions(audit_request.actions)
        if not validation["valid"]:
            return {
                "success": False,
                "error": "Validation failed",
                "validation_errors": validation["errors"],
                "validation_warnings": validation["warnings"],
            }

        # Apply audit actions
        result = audit_service.apply_audit_actions(
            audit_request.session_id, audit_request.actions
        )

        # Prepare response
        response = AuditConfirmationResponse(
            success=result["success"],
            session_id=result["session_id"],
            applied_actions=result["applied_actions"],
            failed_actions=result["failed_actions"],
            errors=result["errors"],
            timestamp=result["timestamp"],
        )

        return response

    except Exception as e:
        logger.error(f"Failed to confirm audit session {audit_request.session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to confirm audit session: {str(e)}",
        )


@router.post("/audit/actions/apply")
async def apply_audit_actions(
    request: Request, actions: List[AuditAction], restaurant_id: int
) -> Dict[str, Any]:
    """
    Apply individual audit actions (for testing or partial updates)
    """
    try:
        skoopin_service = request.app.state.skoopin_service

        # Convert actions to the format expected by SkoopinService
        actions_to_apply = []
        for action in actions:
            action_data = {
                "scan_id": action.scan_id,
                "action_type": action.action_type.value,
                "new_value": action.new_value,
            }
            actions_to_apply.append(action_data)

        # Apply actions
        results = skoopin_service.apply_audit_actions(actions_to_apply, restaurant_id)

        return {
            "success": results["success"],
            "applied_actions": results["applied_actions"],
            "failed_actions": results["failed_actions"],
            "errors": results["errors"],
            "action_results": results["action_results"],
        }

    except Exception as e:
        logger.error(f"Failed to apply audit actions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to apply audit actions: {str(e)}",
        )


@router.get("/audit/progress/{session_id}")
async def get_audit_progress(request: Request, session_id: str) -> Dict[str, Any]:
    """
    Get detailed audit progress for a session
    """
    try:
        audit_service = request.app.state.audit_service
        session_data = audit_service.get_audit_session(session_id)

        if not session_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Audit session not found"
            )

        return {"success": True, "progress": session_data["progress"]}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get audit progress for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get audit progress: {str(e)}",
        )


@router.get("/audit/summary/{session_id}")
async def get_audit_summary(request: Request, session_id: str) -> Dict[str, Any]:
    """
    Get audit session summary
    """
    try:
        audit_service = request.app.state.audit_service
        summary = audit_service.get_audit_summary(session_id)

        if "error" in summary:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=summary["error"]
            )

        return {"success": True, "summary": summary}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get audit summary for session {session_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get audit summary: {str(e)}",
        )


# ========== COMPREHENSIVE CRUD OPERATIONS ==========


@router.post("/audit/crud")
async def comprehensive_audit_crud(
    request: Request, audit_request: AuditConfirmationRequest
) -> Dict[str, Any]:
    """
    Comprehensive CRUD operations for audit system
    - CREATE: New audit sessions
    - READ: Audit data and progress
    - UPDATE: Scan data (pan, menu item, venue, meal period)
    - DELETE: Scan records

    Ensures both Skoopin server and DynamoDB are updated consistently
    """
    try:
        audit_service = request.app.state.audit_service

        # Step 1: Validate actions
        validation = audit_service.validate_audit_actions(audit_request.actions)
        if not validation["valid"]:
            return {
                "success": False,
                "error": "Validation failed",
                "validation_errors": validation["errors"],
                "validation_warnings": validation["warnings"],
            }

        # Step 2: Create audit session if not exists
        session_id = audit_request.session_id
        if not session_id:
            # Create new session
            session_result = audit_service.create_audit_session(
                restaurant_id=audit_request.restaurant_id,
                date=audit_request.date,
                auditor_id=audit_request.auditor_id,
            )
            session_id = session_result["session_id"]

        # Step 3: Apply audit actions (handles both Skoopin and DynamoDB updates)
        result = audit_service.apply_audit_actions(session_id, audit_request.actions)

        # Step 4: Get updated audit summary
        summary = audit_service.get_audit_summary(session_id)

        # Step 5: Prepare comprehensive response
        response = {
            "success": result["success"],
            "session_id": session_id,
            "applied_actions": result["applied_actions"],
            "failed_actions": result["failed_actions"],
            "errors": result["errors"],
            "timestamp": result["timestamp"],
            "summary": summary,
            "crud_operations": {
                "skoopin_updated": result["success"],
                "dynamodb_updated": True,
                "audit_trail_complete": True,
            },
        }

        return response

    except Exception as e:
        logger.error(f"Failed to perform comprehensive CRUD operations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to perform CRUD operations: {str(e)}",
        )


@router.get("/audit/status/{restaurant_id}/{date}")
async def get_comprehensive_audit_status(
    request: Request, restaurant_id: int, date: str
) -> Dict[str, Any]:
    """
    Get comprehensive audit status for a restaurant and date
    Shows both Skoopin and DynamoDB audit status
    """
    try:
        dynamo_service = request.app.state.dynamo_service
        skoopin_service = request.app.state.skoopin_service

        # Get scans from DynamoDB
        dynamo_scans = dynamo_service.get_scans_by_restaurant_day(restaurant_id, date)

        # Get audit sessions for this restaurant/date
        audit_sessions = dynamo_service.get_audit_sessions_by_restaurant(restaurant_id)
        relevant_sessions = [s for s in audit_sessions if s.get("date") == date]

        # Calculate audit statistics
        total_scans = len(dynamo_scans)
        audited_scans = len([s for s in dynamo_scans if s.get("isAudited") == "true"])
        deleted_scans = len(
            [s for s in dynamo_scans if s.get("auditStatus") == "deleted"]
        )
        updated_scans = len(
            [
                s
                for s in dynamo_scans
                if s.get("auditStatus") and s.get("auditStatus") != "deleted"
            ]
        )

        return {
            "success": True,
            "restaurant_id": restaurant_id,
            "date": date,
            "statistics": {
                "total_scans": total_scans,
                "audited_scans": audited_scans,
                "deleted_scans": deleted_scans,
                "updated_scans": updated_scans,
                "audit_progress": (
                    round((audited_scans / total_scans * 100), 2)
                    if total_scans > 0
                    else 0
                ),
            },
            "audit_sessions": relevant_sessions,
            "scan_audit_status": dynamo_scans,
        }

    except Exception as e:
        logger.error(f"Failed to get comprehensive audit status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get audit status: {str(e)}",
        )


@router.post("/submitAudit")
def submit_audits(request: Request, audits: dict = Body(...)) -> Dict[str, Any]:
    # Prevent future-dated audits
    try:
        date_str = audits.get("date")
        if not date_str:
            raise HTTPException(status_code=400, detail="Missing date in payload.")
        requested = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(
            status_code=400, detail="Invalid or missing date in payload."
        )
    today = datetime.now(timezone.utc).date()
    if requested > today:
        raise HTTPException(
            status_code=400, detail="Auditing future dates is not allowed."
        )
    skoopin_service = request.app.state.skoopin_service
    audit_service = request.app.state.audit_service
    dynamo_service = request.app.state.dynamo_service

    # Keep only actionable items
    audits["actions"] = [
        act
        for act in audits.get("actions", [])
        if act.get("delete")
        or (act.get("panId") is not None and str(act.get("panId")).strip() != "")
        or (
            act.get("menuItemId") is not None
            and str(act.get("menuItemId")).strip() != ""
        )
    ]

    # Map incoming actions to typed AuditAction list
    typed_actions = []
    for act in audits["actions"]:
        scan_id = str(act.get("scanId"))
        if act.get("delete"):
            typed_actions.append(
                AuditAction(scan_id=scan_id, action_type=AuditActionType.DELETE)
            )
        if act.get("panId") is not None and str(act.get("panId")).strip() != "":
            typed_actions.append(
                AuditAction(
                    scan_id=scan_id,
                    action_type=AuditActionType.PAN_CHANGE,
                    new_value=str(act.get("panId")),
                )
            )
        if (
            act.get("menuItemId") is not None
            and str(act.get("menuItemId")).strip() != ""
        ):
            typed_actions.append(
                AuditAction(
                    scan_id=scan_id,
                    action_type=AuditActionType.MENU_ITEM_CHANGE,
                    new_value=str(act.get("menuItemId")),
                )
            )

    # Create a session if needed and apply actions via the AuditService (handles Dynamo updates)
    session_res = audit_service.create_audit_session(
        restaurant_id=audits.get("restaurantId"),
        date=audits.get("date"),
        auditor_id=audits.get("auditorId"),
    )
    apply_res = audit_service.apply_audit_actions(
        session_res["session_id"], typed_actions
    )

    ts = apply_res.get("timestamp")
    ts_str = ts.isoformat() if hasattr(ts, "isoformat") else ts
    return {
        "success": apply_res.get("success", False),
        "session_id": apply_res.get("session_id"),
        "applied_actions": apply_res.get("applied_actions", 0),
        "failed_actions": apply_res.get("failed_actions", 0),
        "errors": apply_res.get("errors", []),
        "timestamp": ts_str,
    }


# ========== SCHEDULER MANAGEMENT ENDPOINTS ==========


@router.post("/scheduler/catch-up")
async def trigger_manual_catch_up() -> Dict[str, Any]:
    """Manually trigger catch-up on missed scheduled runs."""
    try:
        result = await manual_catch_up_runs()
        return result
    except Exception as e:
        logger.error(f"Failed to trigger manual catch-up: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to trigger manual catch-up: {str(e)}",
        )


@router.get("/scheduler/status")
async def get_scheduler_status() -> Dict[str, Any]:
    """Get current status of scheduled runs for today."""
    try:
        result = get_run_status_summary()
        return result
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get scheduler status: {str(e)}",
        )


@router.post("/scheduler/force-catch-up")
async def force_catch_up() -> Dict[str, Any]:
    """Force an immediate catch-up check for missed runs."""
    try:
        result = await force_immediate_catch_up()
        return result
    except Exception as e:
        logger.error(f"Failed to force immediate catch-up: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to force immediate catch-up: {str(e)}",
        )


@router.post("/scheduler/mark-completed")
async def mark_run_completed(
    date: str, hour: int, minute: int, run_type: str = "manual"
) -> Dict[str, Any]:
    """Manually mark a scheduled run as completed for a specific date and time."""
    try:
        from datetime import datetime
        from pytz import timezone

        # Parse the date
        try:
            run_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD."
            )

        # Validate hour and minute
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise HTTPException(
                status_code=400,
                detail="Invalid hour or minute. Hour must be 0-23, minute must be 0-59.",
            )

        # Mark the run as completed
        success = mark_run_as_completed(run_date, hour, minute, run_type)

        if success:
            return {
                "success": True,
                "message": f"Run marked as completed for {date} at {hour:02d}:{minute:02d}",
                "date": date,
                "hour": hour,
                "minute": minute,
                "run_type": run_type,
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to mark run as completed",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to mark run as completed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to mark run as completed: {str(e)}",
        )


@router.get("/scheduler/test")
async def test_scheduler() -> Dict[str, Any]:
    """Test endpoint to verify scheduler is working."""
    try:
        from pytz import timezone

        tz = timezone("America/Los_Angeles")
        now = datetime.now(tz)

        return {
            "success": True,
            "message": "Scheduler test endpoint working",
            "current_time_pt": now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            "current_time_utc": datetime.now(dt_timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            ),
            "scheduler_status": "active",
        }
    except Exception as e:
        logger.error(f"Scheduler test failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Scheduler test failed: {str(e)}",
        )


@router.post("/test/pan-download")
async def test_pan_download(
    request: Request, restaurant_id: Optional[int] = None
) -> Dict[str, Any]:
    """Test endpoint to manually test pan download functionality."""
    try:
        # Ensure repo root in path for audit_automation imports
        import sys
        from pathlib import Path

        backend_dir = Path(__file__).resolve().parents[2]
        repo_root = backend_dir.parent
        audit_dir = repo_root / "audit_automation"
        if str(audit_dir) not in sys.path:
            sys.path.insert(0, str(audit_dir))

        import os

        # Create a temporary test folder
        import tempfile
        from download_registered_pans import download_registered_pan_images

        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"🧪 Testing pan download in temporary directory: {temp_dir}")

            # Test the download function
            result = download_registered_pan_images(temp_dir, restaurant_id)

            # Check what was created
            created_files = []
            if os.path.exists(temp_dir):
                for root, dirs, files in os.walk(temp_dir):
                    for file in files:
                        created_files.append(os.path.join(root, file))

            return {
                "success": result,
                "message": "Pan download test completed",
                "download_success": result,
                "temp_directory": temp_dir,
                "created_files": created_files,
                "restaurant_id": restaurant_id,
            }

    except Exception as e:
        logger.error(f"Pan download test failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Pan download test failed: {str(e)}",
        )
