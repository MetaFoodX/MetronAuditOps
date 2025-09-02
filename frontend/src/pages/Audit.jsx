import React, { useEffect, useMemo, useState, useCallback } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import axios from "axios";
import moment from "moment-timezone";
import {
  Typography,
  Paper,
  Box,
  ButtonBase,
  IconButton,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
  Button,
  Chip,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Grid,
  Divider,
  Tooltip,
  Stack,
  LinearProgress,
  CircularProgress,
  Skeleton,
  TextField,
} from "@mui/material";
import Autocomplete from "@mui/material/Autocomplete";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDateFns } from "@mui/x-date-pickers/AdapterDateFns";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { parseISO } from "date-fns";
import { ArrowBackIos, ArrowForwardIos } from "@mui/icons-material";
import {
  initAuditSessionRecord,
  getFilteredPans,
  getScanAction,
} from "../utils/auditUtils";
import SummaryDialog from "./SummaryDialog";

export default function Audit() {
  const location = useLocation();
  const { restaurant, date } = location.state || {};
  const navigate = useNavigate();

  const apiBaseUrl = useMemo(() => {
    const envUrl = import.meta.env.VITE_API_BASE_URL;
    if (envUrl && String(envUrl).trim() !== "") return envUrl;
    // Default to same-origin proxy (nginx forwards /api/* to backend)
    return "";
  }, []);

  const [userId, setUserId] = useState(null);
  const [scans, setScans] = useState([]);
  const [currentScan, setCurrentScan] = useState(null);
  const [session, setSession] = useState(null);
  const [startTime, setStartTime] = useState(null);
  const [endTime, setEndTime] = useState(null);
  const [registeredPans, setRegisteredPans] = useState([]);
  const [scanIndex, setScanIndex] = useState(0);
  const [openSummary, setOpenSummary] = useState(false);
  const [loadingScans, setLoadingScans] = useState(false);
  const [loadingPans, setLoadingPans] = useState(false);
  const [propagating, setPropagating] = useState(false);
  const [noData, setNoData] = useState(false);
  const [aiRunning, setAiRunning] = useState(false);
  const [aiCompletedAt, setAiCompletedAt] = useState(null);
  const [openSelectedZoom, setOpenSelectedZoom] = useState(false);
  const [openChangeDialog, setOpenChangeDialog] = useState(false);
  const [restaurants, setRestaurants] = useState([]);
  const [loadingRestaurants, setLoadingRestaurants] = useState(false);
  const [restaurantsError, setRestaurantsError] = useState("");
  const [dateLocal, setDateLocal] = useState(null);
  const [restaurantLocal, setRestaurantLocal] = useState(null);
  const [scanFilter, setScanFilter] = useState(
    () =>
      (typeof window !== "undefined" &&
        window.sessionStorage.getItem("audit_scan_filter")) ||
      "normal",
  );
  const [confirmArmed, setConfirmArmed] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Page variables to keep track of audit actions
  const [audits, setAudits] = useState([]);
  const [filterPanShape, setFilterPanShape] = useState("");
  const [filterPanSize, setFilterPanSize] = useState("");
  const [selectedPanId, setSelectedPanId] = useState(null);
  const [filteredPans, setFilteredPans] = useState([]);
  const [isDeleted, setIsDeleted] = useState(false);
  const [scanViewMode, setScanViewMode] = useState(
    () =>
      (typeof window !== "undefined" &&
        window.sessionStorage.getItem("audit_view_mode")) ||
      "all",
  ); // all | manual | automated
  const [menuQuery, setMenuQuery] = useState("");
  const [menuOptions, setMenuOptions] = useState([]);
  const [menuLoading, setMenuLoading] = useState(false);

  // Helper function to convert dbShape numbers to readable labels
  const getShapeLabel = (dbShape) => {
    if (dbShape === 1) return "Rectangular";
    if (dbShape === 3) return "Oval";
    return "";
  };

  // (no extra audit confirmation state)

  // Visible scans based on dropdown filter (All / Needs manual / Automated)
  const computeNeedsManualForFilter = useCallback((scan) => {
    if (!scan) return false;
    // If scan is flagged, it always needs manual audit (until server resolves it)
    if (scan?.__flagged) return true;

    // Only consider server/state data to decide if it still needs manual.
    // Do NOT consider local, unsaved edits; we keep the scan visible until Submit.
    const panRaw =
      scan?.auditorPanId ??
      scan?.auditedPanId ??
      scan?.panId ??
      scan?.PanID ??
      scan?.pan_id ??
      scan?.identifiedPan;
    const panStr = panRaw == null ? "" : String(panRaw).trim().toLowerCase();
    const hasPan = !(
      panStr === "" ||
      panStr === "0" ||
      panStr === "unrecognized" ||
      panStr === "unknown" ||
      panStr === "none" ||
      panStr === "null" ||
      panStr === "undefined" ||
      panStr === "nan"
    );

    const menuIdRaw =
      scan?.auditorMenuItemId ??
      scan?.auditedMenuItemId ??
      scan?.menuItemId ??
      scan?.MenuItemID ??
      scan?.reportedMenuItemId;
    let hasMenuName = false;
    if (menuIdRaw != null && String(menuIdRaw).trim() !== "") {
      hasMenuName = true;
    } else {
      const menuNameRaw =
        scan?.reportedMenuItemName ?? scan?.menuItemName ?? scan?.MenuItemName;
      const menuNameStr =
        menuNameRaw == null ? "" : String(menuNameRaw).trim().toLowerCase();
      hasMenuName =
        menuNameStr !== "" &&
        menuNameStr !== "unknown" &&
        menuNameStr !== "none" &&
        menuNameStr !== "null" &&
        menuNameStr !== "undefined" &&
        menuNameStr !== "nan" &&
        menuNameStr !== "unrecognized";
    }

    // Needs manual if either pan OR menu name is missing
    return !(hasPan && hasMenuName);
  }, []);

  const visibleIndices = useMemo(() => {
    if (!Array.isArray(scans) || scans.length === 0) return [];
    // When viewing invalid only or all, ignore the Show filter and show everything in the dataset
    if (scanFilter === "invalidOnly" || scanFilter === "all")
      return scans.map((_, idx) => idx);
    if (scanViewMode === "all") return scans.map((_, idx) => idx);
    const wantManual = scanViewMode === "manual";
    return scans
      .map((s, i) => ({ s, i }))
      .filter(({ s }) => {
        // Keep locally marked-for-deletion visible until submit regardless of view filter
        const action = getScanAction(audits, s?.scanId) || {};
        if (action.delete) return true;
        // Apply manual filter logic otherwise
        return wantManual
          ? computeNeedsManualForFilter(s)
          : !computeNeedsManualForFilter(s);
      })
      .map(({ i }) => i);
  }, [scans, scanViewMode, computeNeedsManualForFilter, scanFilter, audits]);

  const maxIndex = Math.max(0, visibleIndices.length - 1);
  const currentVisiblePos = useMemo(
    () => visibleIndices.indexOf(scanIndex),
    [visibleIndices, scanIndex],
  );

  const handlePrev = useCallback(() => {
    setScanIndex((i) => {
      if (visibleIndices.length === 0) return 0;
      const pos = visibleIndices.indexOf(i);
      const nextPos = pos <= 0 ? maxIndex : pos - 1;
      return visibleIndices[nextPos] ?? visibleIndices[0];
    });
  }, [visibleIndices, maxIndex]);

  const handleNext = useCallback(() => {
    setScanIndex((i) => {
      if (visibleIndices.length === 0) return 0;
      const pos = visibleIndices.indexOf(i);
      const nextPos = pos === -1 || pos >= maxIndex ? 0 : pos + 1;
      return visibleIndices[nextPos] ?? visibleIndices[0];
    });
  }, [visibleIndices, maxIndex]);

  const fetchScansForFilter = useCallback(
    (filter) => {
      setLoadingScans(true);
      const includeBad = filter !== "normal";
      axios
        .get(`${apiBaseUrl}/api/scans_to_audit`, {
          params: {
            restaurantId: restaurant.id,
            date,
            includeBad,
            limit: 1000,
            _ts: Date.now(),
          },
        })
        .then((res) => {
          const main = res.data?.scans || [];
          const flagged = res.data?.flagged || [];
          const isPropagating = res.data?.propagating || false;
          const isNoData = res.data?.noData || false;
          const _aiRunning = res.data?.aiRunning || false;
          const _aiCompletedAt = res.data?.aiCompletedAt || null;

          setPropagating(isPropagating);
          setNoData(isNoData);
          setAiRunning(_aiRunning);
          setAiCompletedAt(_aiCompletedAt);

          let combined = main;
          if (filter === "all") {
            combined = [
              ...main,
              ...flagged.map((s) => ({ ...s, __flagged: true })),
            ];
          } else if (filter === "invalidOnly") {
            combined = flagged.map((s) => ({ ...s, __flagged: true }));
          }
          setScans(
            combined.map((s) => ({
              ...s,
              // Derive lightweight thumbnail key; URL will be fetched lazily on demand
              _imageKey: s.imageURL,
            })),
          );
          // Merge audits so edits from other filters/datasets persist and appear in summary
          setAudits((prev) => {
            // If no previous or restaurant/date changed, initialize fresh
            if (
              !prev ||
              prev.restaurantId !== restaurant.id ||
              prev.date !== date
            ) {
              return initAuditSessionRecord(restaurant, date, combined);
            }
            // Merge actions by scanId, preserving existing edits
            const existingById = new Map(
              prev.actions.map((a) => [a.scanId, a]),
            );
            const nextActions = [...existingById.values()];
            for (const scan of combined) {
              const id = scan?.scanId;
              if (!id) continue;
              if (!existingById.has(id)) {
                nextActions.push({
                  scanId: id,
                  delete: false,
                  panId: null,
                  menuItemId: null,
                });
              }
            }
            return { ...prev, actions: nextActions };
          });
          setScanIndex(0);
        })
        .catch(() => alert("Failed to fetch scans"))
        .finally(() => setLoadingScans(false));
    },
    [apiBaseUrl, restaurant, date],
  );

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.sessionStorage.setItem("audit_scan_filter", scanFilter);
    }
    fetchScansForFilter(scanFilter);
  }, [scanFilter, fetchScansForFilter]);

  // In Normal mode, default the Show filter to "Needs manual only"
  useEffect(() => {
    if (scanFilter === "normal") {
      setScanViewMode("manual");
    }
  }, [scanFilter]);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.sessionStorage.setItem("audit_view_mode", scanViewMode);
    }
  }, [scanViewMode]);

  // Prepare change dialog initial values when opened
  useEffect(() => {
    if (!openChangeDialog) return;
    // Initialize local fields
    try {
      setDateLocal(date ? parseISO(date) : new Date());
    } catch {
      setDateLocal(new Date());
    }
    setRestaurantLocal(restaurant || null);
    // Load restaurants if needed
    if (restaurants.length === 0 && !loadingRestaurants) {
      setLoadingRestaurants(true);
      setRestaurantsError("");
      axios
        .get(`${apiBaseUrl}/api/restaurants`)
        .then((res) => setRestaurants(res.data?.restaurants || []))
        .catch(() => setRestaurantsError("Failed to load restaurants"))
        .finally(() => setLoadingRestaurants(false));
    }
  }, [
    openChangeDialog,
    date,
    restaurant,
    restaurants.length,
    loadingRestaurants,
    apiBaseUrl,
  ]);

  useEffect(() => {
    let cancelled = false;
    const key = restaurant?.id ? `${restaurant.id}:${date}` : "unknown";
    const maxTries = 8;
    const delayMs = 1500;

    const loadPans = (attempt = 0) => {
      if (cancelled) return;
      if (!restaurant || !restaurant?.id || !date) {
        try {
          console.warn("[pans] Skipping DB fetch: missing restaurant or date", {
            hasRestaurant: !!restaurant,
            restaurantId: restaurant?.id,
            date,
          });
        } catch (error) {
          console.error("Error logging warning:", error);
        }
        setLoadingPans(false);
        return;
      }
      if (attempt === 0) {
        setLoadingPans(true);
        try {
          console.info("[pans] useEffect triggered for DB fetch");
        } catch (error) {
          console.error("Error logging info:", error);
        }
      }
      try {
        console.info(
          `[pans] DB fetch start (attempt ${attempt + 1}/${maxTries}) → GET ${apiBaseUrl || "(same-origin)"}/api/pans`,
          { restaurantId: restaurant.id, date },
        );
      } catch (error) {
        console.error("Error logging fetch start:", error);
      }
      axios
        .get(`${apiBaseUrl}/api/pans`, {
          params: {
            restaurantId: String(restaurant.id),
            date,
            _ts: Date.now(),
          },
          timeout: 30000,
        })
        .then((res) => {
          if (cancelled) return;
          const building = !!res.data?.building;
          const _panRecords = res.data?.pans || [];
          try {
            console.info(
              `[pans] DB fetch success (attempt ${attempt + 1}) — pans=${_panRecords.length}, building=${building}`,
            );
          } catch (error) {
            console.error("Error logging success:", error);
          }
          setRegisteredPans(_panRecords);
          setFilteredPans(_panRecords);

          if (building || _panRecords.length === 0) {
            if (attempt + 1 < maxTries) {
              try {
                console.warn(
                  "[pans] No pans yet or building in progress; will re-try",
                );
              } catch (error) {
                console.error("Error logging retry warning:", error);
              }
              setTimeout(() => loadPans(attempt + 1), delayMs);
            } else {
              try {
                console.error("[pans] Exhausted retries fetching pans from DB");
              } catch (error) {
                console.error("Error logging exhausted retries:", error);
              }
              setLoadingPans(false);
            }
          } else {
            setLoadingPans(false);
          }
        })
        .catch((e) => {
          if (cancelled) return;
          try {
            console.error(
              `[pans] DB fetch failed (attempt ${attempt + 1}):`,
              e?.message || e,
            );
          } catch (error) {
            console.error("Error logging fetch failure:", error);
          }
          if (attempt + 1 < maxTries) {
            setTimeout(() => loadPans(attempt + 1), delayMs);
          } else {
            alert("Failed to fetch registered pans (DB)");
            setLoadingPans(false);
          }
        });
    };

    loadPans(0);
    return () => {
      cancelled = true;
    };
  }, [restaurant, date, apiBaseUrl]);

  // Reset filters & auto‐select pan on scan change
  useEffect(() => {
    // 1) reset filters
    setFilterPanShape("");
    setFilterPanSize("");

    // 2) auto-select pan if it's recognized
    const auditedPanId = getScanAction(audits, currentScan?.scanId)?.panId;
    if (auditedPanId != null) {
      setSelectedPanId(auditedPanId);
      const _recognizedPan = registeredPans.find(
        (pan) => pan.ID === auditedPanId,
      );
      if (_recognizedPan) {
        setFilterPanShape((_recognizedPan["dbShape"] || "").toString());
        setFilterPanSize(_recognizedPan["dbSizeStandard"] || "");
        const filtered = getFilteredPans(
          _recognizedPan["dbShape"] || "",
          _recognizedPan["dbSizeStandard"] || "",
          registeredPans,
        );
        setFilteredPans(filtered.length > 0 ? filtered : registeredPans);
      }
    } else {
      // Try detected pan from DB across possible fields and normalize to a registered pan
      const rawDbPanId = sanitizePanId(
        currentScan?.panId ??
          currentScan?.PanID ??
          currentScan?.pan_id ??
          currentScan?.identifiedPan ??
          null,
      );
      const matchedDbPan = rawDbPanId ? findPanByExternalId(rawDbPanId) : null;
      if (matchedDbPan) {
        setSelectedPanId(matchedDbPan.ID);
        setFilterPanShape((matchedDbPan["dbShape"] || "").toString());
        setFilterPanSize(matchedDbPan["dbSizeStandard"] || "");
        const filtered = getFilteredPans(
          matchedDbPan["dbShape"] || "",
          matchedDbPan["dbSizeStandard"] || "",
          registeredPans,
        );
        setFilteredPans(filtered.length > 0 ? filtered : registeredPans);
      } else if (currentScan?.panId && currentScan.panId !== "Unrecognized") {
        // Fallback to legacy direct panId
        setSelectedPanId(currentScan.panId);
        const _recognizedPan = registeredPans.find(
          (pan) => pan.ID === currentScan.panId,
        );
        if (_recognizedPan) {
          setFilterPanShape((_recognizedPan["dbShape"] || "").toString());
          setFilterPanSize(_recognizedPan["dbSizeStandard"] || "");
          const filtered = getFilteredPans(
            _recognizedPan["dbShape"] || "",
            _recognizedPan["dbSizeStandard"] || "",
            registeredPans,
          );
          setFilteredPans(filtered.length > 0 ? filtered : registeredPans);
        }
      } else {
        setSelectedPanId(null);
        setFilteredPans(registeredPans);
      }
    }
  }, [currentScan, registeredPans, audits, findPanByExternalId, sanitizePanId]);

  // On pan selection filter changes
  useEffect(() => {
    const filtered = getFilteredPans(
      filterPanShape,
      filterPanSize,
      registeredPans,
    );
    setFilteredPans(filtered.length > 0 ? filtered : registeredPans);
  }, [filterPanSize, filterPanShape, registeredPans]);

  // On scan index changes (respecting visibility filter)
  useEffect(() => {
    if (scans.length > 0 && visibleIndices.length > 0) {
      const pos = visibleIndices.indexOf(scanIndex);
      const safeIndex = pos === -1 ? visibleIndices[0] : scanIndex;
      if (safeIndex !== scanIndex) setScanIndex(safeIndex);
      setCurrentScan(scans[safeIndex]);
      const currentAudit = audits?.actions?.find(
        (a) => a.scanId === scans[safeIndex]?.scanId,
      );
      const persistedDeleted =
        String(
          scans[safeIndex]?.auditStatus || scans[safeIndex]?.AuditStatus || "",
        )
          .trim()
          .toLowerCase() === "deleted";
      setIsDeleted(Boolean(currentAudit?.delete) || persistedDeleted);
    }
  }, [scanIndex, scans, audits, visibleIndices]);

  // Ensure current scan image URL is available (on-demand presign)
  useEffect(() => {
    if (!currentScan) return;
    if (currentScan.imageUrl || currentScan.imageBase64) return;
    const key = currentScan.imageURL || currentScan._imageKey;
    if (!key) return;
    let cancelled = false;
    axios
      .get(`${apiBaseUrl}/api/image/presign`, { params: { key } })
      .then((res) => {
        if (cancelled) return;
        const url = res.data?.url;
        if (!url) return;
        setScans((prev) =>
          prev.map((s) =>
            s.scanId === currentScan.scanId ? { ...s, imageUrl: url } : s,
          ),
        );
      })
      .catch((error) => {
        console.error("Failed to presign image:", error);
      });
    return () => {
      cancelled = true;
    };
  }, [currentScan, apiBaseUrl]);

  const onDeleteScan = useCallback(() => {
    setAudits((prev) => ({
      ...prev,
      actions: prev.actions.map((a) =>
        a.scanId === currentScan.scanId ? { ...a, delete: !a.delete } : a,
      ),
    }));
    // Update local isDeleted state
    setIsDeleted((prev) => !prev);
  }, [currentScan?.scanId]);

  const handlePanSelect = useCallback(
    (panId) => {
      setSelectedPanId(panId);
      setAudits((prev) => ({
        ...prev,
        actions: prev.actions.map((a) =>
          a.scanId === currentScan.scanId ? { ...a, panId } : a,
        ),
      }));
    },
    [currentScan?.scanId],
  );

  function onSubmitAudit() {
    const endTime = moment().tz("America/Los_Angeles").format("YYYY-MM-DD");

    setAudits((prev) => ({
      ...prev,
      auditEndTime: endTime,
    }));
    setConfirmArmed(true);
    setOpenSummary(true);
  }

  function handleConfirm() {
    if (!confirmArmed || isSubmitting) return;
    setIsSubmitting(true);
    axios
      .post(`${apiBaseUrl}/api/submitAudit`, audits, { timeout: 30000 })
      .then(() => {
        setOpenSummary(false);

        // Show success message
        alert("Audit submitted successfully! Refreshing data...");

        // Clear the local audit state since it's now submitted
        setAudits(initAuditSessionRecord(restaurant, date, []));

        // Refresh scans to get updated data from the server
        setLoadingScans(true);
        setScans([]);
        return axios
          .get(`${apiBaseUrl}/api/scans_to_audit`, {
            params: {
              restaurantId: restaurant.id,
              date,
              includeBad: scanFilter !== "normal",
              limit: 1000,
              _ts: Date.now(),
            },
          })
          .then((res) => {
            const main = res.data?.scans || [];
            const flagged = res.data?.flagged || [];
            const isPropagating = res.data?.propagating || false;
            const isNoData = res.data?.noData || false;
            const _aiRunning = res.data?.aiRunning || false;
            const _aiCompletedAt = res.data?.aiCompletedAt || null;

            setPropagating(isPropagating);
            setNoData(isNoData);
            setAiRunning(_aiRunning);
            setAiCompletedAt(_aiCompletedAt);
            let combined = main;
            if (scanFilter === "all") {
              combined = [
                ...main,
                ...flagged.map((s) => ({ ...s, __flagged: true })),
              ];
            } else if (scanFilter === "invalidOnly") {
              combined = flagged.map((s) => ({ ...s, __flagged: true }));
            }

            // Update scans with fresh data from server
            setScans(
              combined.map((s) => ({
                ...s,
                _imageKey: s.imageURL,
              })),
            );

            // Reset scan index to 0 since the data has changed
            setScanIndex(0);
          })
          .then(() => {
            // Update pan wasAudited flags without re-downloading full pan data
            // Only update the flags for pans that were actually used in this audit
            const usedPanIds = new Set();
            audits.actions.forEach((action) => {
              if (action.panId && action.panId !== "") {
                usedPanIds.add(action.panId);
              }
            });

            if (usedPanIds.size > 0) {
              // Update local pan records with new wasAudited status
              setRegisteredPans((prev) =>
                prev.map((pan) => ({
                  ...pan,
                  wasAudited: pan.wasAudited || usedPanIds.has(pan.ID),
                })),
              );
              setFilteredPans((prev) =>
                prev.map((pan) => ({
                  ...pan,
                  wasAudited: pan.wasAudited || usedPanIds.has(pan.ID),
                })),
              );
            }
          })
          .finally(() => setLoadingScans(false));
      })
      .catch((error) => {
        console.error("Submit failed:", error);
        const message =
          error.code === "ECONNABORTED"
            ? "Request timed out. Please try again."
            : error.response?.data?.detail || error.message || "Unknown error";
        alert(`Submit failed: ${message}`);
      })
      .finally(() => {
        setIsSubmitting(false);
        setConfirmArmed(false);
      });
  }
  // Hotkeys: ←/→ navigate, D delete/restore, 1-9 quick-select first 9 filtered pans, Enter submit
  const hotkeyMap = useMemo(
    () => filteredPans.slice(0, 9).map((p) => p.ID),
    [filteredPans],
  );

  const handleKeyDown = useCallback(
    (e) => {
      if (!currentScan) return;
      if (e.key === "ArrowLeft") {
        e.preventDefault();
        handlePrev();
        return;
      }
      if (e.key === "ArrowRight") {
        e.preventDefault();
        handleNext();
        return;
      }
      if (e.key.toLowerCase() === "d") {
        e.preventDefault();
        onDeleteScan();
        return;
      }
      if (e.key === "Enter") {
        e.preventDefault();
        onSubmitAudit();
        return;
      }
      const num = Number(e.key);
      if (!Number.isNaN(num) && num >= 1 && num <= hotkeyMap.length) {
        e.preventDefault();
        const panId = hotkeyMap[num - 1];
        if (panId != null) {
          handlePanSelect(panId);
        }
      }
    },
    [
      currentScan,
      hotkeyMap,
      handlePrev,
      handleNext,
      onDeleteScan,
      handlePanSelect,
    ],
  );

  useEffect(() => {
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [handleKeyDown]);

  // Helper to find pan by ID coercing to string (handles type mismatches)
  const findPanById = useCallback(
    (id) => {
      if (id == null) return null;
      const target = String(id);
      return registeredPans.find((p) => String(p.ID) === target) || null;
    },
    [registeredPans],
  );

  // Parse external model IDs like "864bec...c5__158___1_2_Long___3.7_inch_deep_"
  const normalizeExternalPanIdParts = useCallback((raw) => {
    if (raw == null) return { uuid: null, numeric: null };
    const s = String(raw).trim();
    // Prefer a 32-hex prefix (UUID-like) if present
    const uuidMatch = s.match(/^([a-f0-9]{32})/i);
    const uuid = uuidMatch
      ? uuidMatch[1]
      : s.includes("__")
        ? s.split("__")[0]
        : null;
    // Prefer numeric token immediately following the first "__"
    let numeric = null;
    const secondToken = s.match(/^.*?__([0-9]+)/);
    if (secondToken && secondToken[1]) {
      numeric = secondToken[1];
    } else {
      const numMatch = s.match(/\b(\d{1,})\b/);
      numeric = numMatch ? numMatch[1] : null;
    }
    return { uuid, numeric };
  }, []);

  // Treat invalid/sentinel pan IDs as null
  const sanitizePanId = useCallback((val) => {
    if (val == null) return null;
    const s = String(val).trim().toLowerCase();
    if (
      s === "" ||
      s === "0" ||
      s === "unknown" ||
      s === "unrecognized" ||
      s === "none" ||
      s === "null" ||
      s === "undefined" ||
      s === "nan"
    ) {
      return null;
    }
    return String(val);
  }, []);

  // Try matching by a variety of possible pan ID fields
  const findPanByExternalId = useCallback(
    (rawId) => {
      if (rawId == null) return null;
      const { uuid, numeric } = normalizeExternalPanIdParts(rawId);
      const candidates = [uuid, numeric, String(rawId)];
      const fields = ["ID", "Id", "PanID", "UniqueID", "UUID"];
      for (const pan of registeredPans) {
        for (const f of fields) {
          if (pan[f] != null) {
            const v = String(pan[f]);
            if (candidates.includes(v)) return pan;
          }
        }
      }
      // As a last resort, try exact ID field against numeric
      if (numeric) {
        const byNum = registeredPans.find((p) => String(p.ID) === numeric);
        if (byNum) return byNum;
      }
      return null;
    },
    [registeredPans, normalizeExternalPanIdParts],
  );

  // Shorten long IDs for compact display (e.g., 47b84a…30f5)
  const shortenId = useCallback((id, head = 6, tail = 4) => {
    if (id == null) return "";
    const s = String(id);
    if (s.length <= head + tail + 1) return s;
    return `${s.slice(0, head)}…${s.slice(-tail)}`;
  }, []);

  // Format dimensions string from pan object, preferring raw fields over normalized values
  const formatPanDims = useCallback((pan) => {
    if (!pan) return "—";
    const getRaw = (obj, keys) => {
      for (const k of keys) {
        const v = obj?.[k];
        if (v != null && String(v).trim() !== "") return String(v).trim();
      }
      return null;
    };
    const round1 = (n) => Math.round(Number(n) * 10) / 10;
    const wRaw = getRaw(pan, ["Width", "width", "W"]);
    const lRaw = getRaw(pan, ["Length", "length", "L"]);
    let wStr = wRaw;
    let lStr = lRaw;
    if (!wStr && pan?.dimensions?.widthIn != null)
      wStr = `${round1(pan.dimensions.widthIn)} in`;
    if (!lStr && pan?.dimensions?.lengthIn != null)
      lStr = `${round1(pan.dimensions.lengthIn)} in`;
    const dRaw = getRaw(pan, ["Depth", "depth", "DepthInch"]);
    let dStr = dRaw;
    if (!dStr && pan?.dimensions?.depthIn != null)
      dStr = `${round1(pan.dimensions.depthIn)} in`;
    const sizeStr = lStr || wStr ? `${lStr ?? "—"} × ${wStr ?? "—"}` : null;
    return [sizeStr, dStr].filter(Boolean).join(", ") || "—";
  }, []);

  const aiSuggestedPan = useMemo(() => {
    if (!currentScan) return null;
    // prefer explicit GenAI ID if present, fall back to yolo/corner suggestion
    const g =
      currentScan.genAIPanId != null
        ? findPanByExternalId(sanitizePanId(currentScan.genAIPanId))
        : null;
    if (g) return g;
    const y = findPanByExternalId(
      sanitizePanId(
        currentScan.YOLOv8_Pan_ID ??
          currentScan.yoloPanId ??
          currentScan.YOLOv8_Best_Match_ID ??
          currentScan.YOLO_Pan_ID,
      ),
    );
    if (y) return y;
    return findPanByExternalId(
      sanitizePanId(
        currentScan.Corner_Best_Pan_ID ??
          currentScan.Corner_Best_Empty_Pan_Match ??
          currentScan.cornerPanId ??
          currentScan.Corner_Pan_ID,
      ),
    );
  }, [currentScan, findPanByExternalId, sanitizePanId]);

  const selectedPan = useMemo(
    () => findPanById(selectedPanId),
    [findPanById, selectedPanId],
  );

  // Selected pan specs: Width/Depth + Weight (consistent with scan measures)
  const selectedPanSpecs = useMemo(() => {
    if (!selectedPan) return "—";
    const round1 = (n) => (n == null ? null : Math.round(Number(n) * 10) / 10);
    const getRaw = (obj, keys) => {
      for (const k of keys) {
        const v = obj?.[k];
        if (v != null && String(v).trim() !== "") return v;
      }
      return null;
    };
    const deepParse = (val) => {
      if (val == null) return null;
      if (typeof val === "string") {
        try {
          return JSON.parse(val);
        } catch {
          return null;
        }
      }
      if (typeof val === "object") return val;
      return null;
    };
    const dataObj =
      deepParse(selectedPan?.Data) || deepParse(selectedPan?.data) || null;
    const getFromData = (keys) => {
      if (!dataObj) return null;
      for (const k of keys) {
        const candidates = [k, k.toLowerCase(), k.toUpperCase()];
        for (const ck of candidates) {
          if (dataObj[ck] != null && String(dataObj[ck]).trim() !== "")
            return dataObj[ck];
        }
      }
      return null;
    };

    const w =
      getRaw(selectedPan, ["Width", "width", "W"]) ??
      selectedPan?.dimensions?.widthIn ??
      getFromData(["Width", "WidthInches"]);
    const d =
      getRaw(selectedPan, ["Depth", "depth", "DepthInch"]) ??
      selectedPan?.dimensions?.depthIn ??
      getFromData(["Depth", "DepthInches"]);
    const weight = getRaw(selectedPan, ["Weight", "weight"]);
    const fmt1 = (n) => (n == null ? "—" : round1(n).toFixed(1));
    const fmtOz = (oz) => {
      if (oz == null) return "—";
      const num = Number(oz);
      return Number.isInteger(num) ? `${num}` : `${round1(num)}`;
    };
    return `Width: ${fmt1(w)} in • Depth: ${fmt1(d)} in • Weight: ${fmtOz(weight)} oz`;
  }, [selectedPan]);

  // Heuristic extraction of per-model suggestions
  // Robust field resolution for model predictions (case-insensitive, multiple variants)
  const genAIPanId = useMemo(() => {
    const s = currentScan || {};
    const direct = s.genAIPanId ?? s.genaiPanId ?? s.GenAI_Pan_ID;
    if (direct != null) {
      const parts = normalizeExternalPanIdParts(direct);
      return (
        sanitizePanId(parts.uuid) ??
        sanitizePanId(parts.numeric) ??
        sanitizePanId(direct)
      );
    }
    // search any key containing gen and pan and id
    for (const k of Object.keys(s)) {
      const lk = k.toLowerCase();
      if (lk.includes("gen") && lk.includes("pan") && lk.includes("id")) {
        const parts = normalizeExternalPanIdParts(s[k]);
        return (
          sanitizePanId(parts.uuid) ??
          sanitizePanId(parts.numeric) ??
          sanitizePanId(s[k])
        );
      }
    }
    return null;
  }, [currentScan, normalizeExternalPanIdParts, sanitizePanId]);
  const yoloPanId = useMemo(() => {
    const s = currentScan || {};
    const direct =
      s.YOLOv8_Pan_ID ?? s.yoloPanId ?? s.YOLOv8_Best_Match_ID ?? s.YOLO_Pan_ID;
    if (direct != null) {
      const parts = normalizeExternalPanIdParts(direct);
      return (
        sanitizePanId(parts.uuid) ??
        sanitizePanId(parts.numeric) ??
        sanitizePanId(direct)
      );
    }
    for (const k of Object.keys(s)) {
      const lk = k.toLowerCase();
      if (lk.includes("yolo") && (lk.includes("pan") || lk.includes("id"))) {
        const parts = normalizeExternalPanIdParts(s[k]);
        return (
          sanitizePanId(parts.uuid) ??
          sanitizePanId(parts.numeric) ??
          sanitizePanId(s[k])
        );
      }
    }
    return null;
  }, [currentScan, normalizeExternalPanIdParts, sanitizePanId]);
  const cornerPanId = useMemo(() => {
    const s = currentScan || {};
    const direct =
      s.Corner_Best_Pan_ID ??
      s.Corner_Best_Empty_Pan_Match ??
      s.cornerPanId ??
      s.Corner_Pan_ID;
    if (direct != null) {
      const parts = normalizeExternalPanIdParts(direct);
      return (
        sanitizePanId(parts.uuid) ??
        sanitizePanId(parts.numeric) ??
        sanitizePanId(direct)
      );
    }
    for (const k of Object.keys(s)) {
      const lk = k.toLowerCase();
      if (lk.includes("corner") && (lk.includes("pan") || lk.includes("id"))) {
        const parts = normalizeExternalPanIdParts(s[k]);
        return (
          sanitizePanId(parts.uuid) ??
          sanitizePanId(parts.numeric) ??
          sanitizePanId(s[k])
        );
      }
    }
    return null;
  }, [currentScan, normalizeExternalPanIdParts, sanitizePanId]);
  const genaiPan = useMemo(
    () => findPanByExternalId(genAIPanId),
    [genAIPanId, findPanByExternalId],
  );
  const yoloPan = useMemo(
    () => findPanByExternalId(yoloPanId),
    [yoloPanId, findPanByExternalId],
  );
  const cornerPan = useMemo(
    () => findPanByExternalId(cornerPanId),
    [cornerPanId, findPanByExternalId],
  );

  // Determine recommendation source to color/label the suggested pan card
  const recommendationMeta = useMemo(() => {
    const rawSource = (
      currentScan?.recommendationSource ||
      currentScan?.recommendation_source ||
      currentScan?.panAuditReason ||
      ""
    )
      .toString()
      .toLowerCase();

    let key = "genai";
    if (rawSource.includes("yolo")) key = "yolo";
    else if (rawSource.includes("corner")) key = "corner";
    else if (rawSource.includes("genai") || currentScan?.genAIPanId)
      key = "genai";

    const palette = {
      genai: { label: "GenAI", color: "secondary" },
      yolo: { label: "YOLO", color: "success" },
      corner: { label: "Corner", color: "warning" },
    };
    return palette[key] || { label: "AI", color: "info" };
  }, [currentScan]);

  // All scans that need manual attention (used in summary)
  const manualScanIds = useMemo(
    () =>
      (Array.isArray(scans) ? scans : [])
        .filter((s) => computeNeedsManualForFilter(s))
        .map((s) => s.scanId),
    [scans, computeNeedsManualForFilter],
  );

  // Debounced menu search
  useEffect(() => {
    let active = true;
    const q = menuQuery.trim();
    if (q.length === 0) {
      setMenuOptions([]);
      return;
    }
    setMenuLoading(true);
    const t = setTimeout(() => {
      axios
        .get(`${apiBaseUrl}/api/menu_items`, {
          params: { restaurantId: restaurant.id, date, q, limit: 50 },
        })
        .then((res) => {
          if (!active) return;
          setMenuOptions(res.data?.items || []);
        })
        .catch(() => {
          if (active) setMenuOptions([]);
        })
        .finally(() => {
          if (active) setMenuLoading(false);
        });
    }, 250);
    return () => {
      active = false;
      clearTimeout(t);
    };
  }, [menuQuery, restaurant?.id, date, apiBaseUrl]);

  // Reason codes → human explanations
  const reasonCatalog = useMemo(
    () => ({
      MultiplePossiblePans: {
        label: "Multiple possible pans",
        color: "warning",
        description:
          "The model returned several candidates with similar scores, so a human review is recommended.",
      },
      CornerDesignMismatch: {
        label: "Corner design mismatch",
        color: "error",
        description:
          "Corner/shape features do not match the expected registered pan design.",
      },
      LowConfidence: {
        label: "Low confidence",
        color: "warning",
        description:
          "Prediction confidence was below the acceptance threshold.",
      },
      NonFoodDetected: {
        label: "Non‑food detected",
        color: "error",
        description: "Detected a non‑food item or empty reading on the scale.",
      },
      WeightTooLow: {
        label: "Weight too low",
        color: "warning",
        description:
          "Net weight is low, which can make identification unreliable.",
      },
      UnrecognizedPan: {
        label: "Unrecognized pan",
        color: "info",
        description: "Pan was not found in the registered pans catalog.",
      },
    }),
    [],
  );

  const parseReasons = useCallback((raw) => {
    if (raw == null) return [];
    if (Array.isArray(raw)) return raw.map(String);
    const s = String(raw).trim();
    let list = [];
    if (s.startsWith("[")) {
      try {
        list = JSON.parse(s);
      } catch {
        list = s.split(/[;,|]/);
      }
    } else {
      list = s.split(/[;,|]/);
    }
    list = list.map((r) => String(r).trim()).filter(Boolean);
    // normalize common variations/typos
    list = list.map((code) => {
      const lc = code.toLowerCase();
      if (lc.startsWith("cornerdesignmismat")) return "CornerDesignMismatch";
      if (lc.startsWith("multiplepossiblepan")) return "MultiplePossiblePans";
      if (lc.includes("non") && lc.includes("food")) return "NonFoodDetected";
      if (lc.includes("weight") && lc.includes("low")) return "WeightTooLow";
      if (lc.includes("low") && lc.includes("confidence"))
        return "LowConfidence";
      if (lc.includes("unrecognized")) return "UnrecognizedPan";
      return code;
    });
    return Array.from(new Set(list));
  }, []);

  const reasonCodes = useMemo(
    () => parseReasons(currentScan?.panAuditReason),
    [currentScan, parseReasons],
  );

  // Compact detected size info for current scan
  const detectedScanSpecs = useMemo(() => {
    const s = currentScan || {};
    const getFirst = (...keys) => {
      for (const k of keys) {
        if (s[k] != null && s[k] !== "") return s[k];
      }
      return null;
    };
    const normalizeDepth = (v) => {
      if (v == null) return null;
      const n = Number(String(v).replace(/[^0-9.]/g, ""));
      return Number.isFinite(n) ? n : null;
    };
    const normalizeVolume = (v) => {
      if (v == null) return { cups: null, quarts: null };
      if (typeof v === "number") return { cups: v, quarts: v / 4 };
      const s = String(v).trim().toLowerCase();
      if (s.includes("%")) return { cups: null, quarts: null }; // percentage, skip
      const m = s.match(/([0-9]+(?:\.[0-9]+)?)\s*(cups?|c|quarts?|qt|oz)?/);
      if (!m) return { cups: null, quarts: null };
      const num = parseFloat(m[1]);
      const unit = (m[2] || "").toLowerCase();
      if (Number.isNaN(num)) return { cups: null, quarts: null };
      if (unit.startsWith("qt") || unit.startsWith("quart"))
        return { cups: num * 4, quarts: num };
      if (unit.startsWith("oz")) return { cups: num / 8, quarts: num / 8 / 4 };
      if (unit.startsWith("cup") || unit === "c" || unit === "")
        return { cups: num, quarts: num / 4 };
      return { cups: null, quarts: null };
    };
    const parts = [];
    const sizeStd = getFirst(
      "detectedSizeStandard",
      "DetectedSizeStandard",
      "Detected_Size_Standard",
    );
    if (sizeStd) parts.push(String(sizeStd));
    const depthIn = normalizeDepth(getFirst("detectedDepth", "DetectedDepth"));
    if (depthIn != null) parts.push(`${depthIn}”`);
    const { cups: volCups, quarts: volQuarts } = normalizeVolume(
      getFirst("Volume", "volume", "DetectedVolume"),
    );
    if (volCups != null) {
      const cupsStr = Math.round(volCups * 10) / 10;
      const qtsStr = Math.round((volQuarts ?? volCups / 4) * 10) / 10;
      parts.push(`${cupsStr} cups (${qtsStr} qt)`);
    }
    const weightOz = (() => {
      const w = getFirst("weight", "Weight");
      if (w == null) return null;
      const n = Number(w);
      return Number.isFinite(n) ? n : null;
    })();
    if (weightOz != null) parts.push(`${weightOz} oz`);
    return parts.length > 0 ? parts.join(" • ") : "—";
  }, [currentScan]);

  // Width/Depth + Weight compact line for current scan (fits UI)
  const scanMeasuresSpecs = useMemo(() => {
    const s = currentScan || {};

    const toNumber = (v) => {
      if (v == null) return null;
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };
    const getFirst = (...keys) => {
      for (const k of keys) {
        if (s[k] != null && s[k] !== "") return s[k];
      }
      return null;
    };

    const deepParse = (val) => {
      if (val == null) return null;
      if (typeof val === "string") {
        try {
          return JSON.parse(val);
        } catch {
          return null;
        }
      }
      if (typeof val === "object") return val;
      return null;
    };

    // Extract width/depth (in inches) from scan or nested GenAI fields
    const tryExtractDims = () => {
      const directWidth = toNumber(getFirst("Width", "width", "WidthInches"));
      const directDepth = toNumber(
        getFirst(
          "Depth",
          "depth",
          "DetectedDepth",
          "detectedDepth",
          "DepthInches",
        ),
      );
      if (directWidth != null || directDepth != null)
        return { w: directWidth, d: directDepth };

      const candidates = [deepParse(s.GenAIData), deepParse(s.GenAIResponse)];
      const keysWidth = ["widthinches", "width"];
      const keysDepth = ["depthinches", "depth", "detecteddepth"];

      const searchDims = (obj) => {
        if (!obj) return { w: null, d: null };
        let w = null,
          d = null;
        const visit = (node) => {
          if (node == null) return;
          if (Array.isArray(node)) {
            for (const it of node) visit(it);
            return;
          }
          if (typeof node === "object") {
            for (const [k, v] of Object.entries(node)) {
              const lk = k.toString().toLowerCase();
              if (keysWidth.includes(lk) && w == null) {
                const nv = toNumber(
                  typeof v === "string" ? v.replace(/[^0-9.]/g, "") : v,
                );
                if (nv != null) w = nv;
              }
              if (keysDepth.includes(lk) && d == null) {
                const nv = toNumber(
                  typeof v === "string" ? v.replace(/[^0-9.]/g, "") : v,
                );
                if (nv != null) d = nv;
              }
              if (typeof v === "object") visit(v);
            }
          }
        };
        visit(obj);
        return { w, d };
      };

      for (const cand of candidates) {
        const { w, d } = searchDims(cand);
        if (w != null || d != null) return { w, d };
      }
      return { w: null, d: null };
    };

    const { w: widthIn, d: depthIn } = tryExtractDims();

    // Weight (oz)
    const weightNum = toNumber(getFirst("weight", "Weight"));
    const formatOz = (n) => {
      if (n == null) return "—";
      return Number.isInteger(n) ? `${n}` : `${Math.round(n * 10) / 10}`;
    };

    const fmt1 = (n) =>
      n == null ? "—" : (Math.round(n * 10) / 10).toFixed(1);
    return `Width: ${fmt1(widthIn)} in • Depth: ${fmt1(depthIn)} in • Weight: ${formatOz(weightNum)} oz`;
  }, [currentScan]);

  useEffect(() => {
    if (!propagating || !restaurant?.id || !date) return;

    const pollInterval = setInterval(() => {
      const includeBad = scanFilter !== "normal";
      axios
        .get(`${apiBaseUrl}/api/scans_to_audit`, {
          params: {
            restaurantId: restaurant.id,
            date,
            includeBad,
            limit: 1000,
            _ts: Date.now(),
          },
        })
        .then((res) => {
          const main = res.data?.scans || [];
          const flagged = res.data?.flagged || [];
          const isPropagating = res.data?.propagating || false;
          const isNoData = res.data?.noData || false;
          const _aiRunning = res.data?.aiRunning || false;
          const _aiCompletedAt = res.data?.aiCompletedAt || null;

          setPropagating(isPropagating);
          setNoData(isNoData);
          setAiRunning(_aiRunning);
          setAiCompletedAt(_aiCompletedAt);

          if (!isPropagating && !isNoData) {
            // Immediately refresh scans once propagation completes
            fetchScansForFilter(scanFilter);
          }
        })
        .catch(() => {
          // On error, stop polling
          setPropagating(false);
        });
    }, 5000); // Poll every 5 seconds

    return () => clearInterval(pollInterval);
  }, [
    propagating,
    restaurant?.id,
    date,
    scanFilter,
    apiBaseUrl,
    fetchScansForFilter,
  ]);

  const handleRunAI = async () => {
    if (!date) return;
    try {
      await axios.post(`${apiBaseUrl}/api/pan_ai/run`, {
        date,
        restaurantId: restaurant?.id || null,
      });
      setAiRunning(true);
    } catch (e) {
      alert("Failed to start AI workflow");
    }
  };

  return (
    <Box sx={{ p: 2 }}>
      <Paper
        elevation={2}
        sx={{ mb: 2, p: 1.5, position: "sticky", top: 0, zIndex: 10 }}
      >
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="space-between"
          spacing={2}
          sx={{ flexWrap: "wrap", rowGap: 1 }}
        >
          <Stack direction="row" spacing={2} alignItems="center">
            <Typography variant="h6">
              {restaurant?.name || "Restaurant"}
            </Typography>
            <Divider orientation="vertical" flexItem />
            <Typography variant="body1">{date}</Typography>
            <Chip
              label={`${currentVisiblePos >= 0 ? currentVisiblePos + 1 : 0}/${visibleIndices.length}`}
              size="small"
              color="default"
            />
            {scans.length === 0 && (
              <Chip label="No scans" size="small" color="warning" />
            )}
            {isDeleted && (
              <Chip
                label="Marked for deletion"
                size="small"
                color="error"
                variant="filled"
              />
            )}
          </Stack>
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{ display: { xs: "none", sm: "block" } }}
          >
            Shortcuts: Left/Right navigate • 1–9 choose pan • D delete/restore •
            Enter submit
          </Typography>
          <Stack
            direction="row"
            spacing={2}
            alignItems="center"
            sx={{
              flexWrap: "wrap",
              justifyContent: { xs: "flex-start", md: "flex-end" },
              rowGap: 1,
            }}
          >
            <Chip
              size="small"
              label={
                aiRunning
                  ? "AI: running"
                  : aiCompletedAt
                    ? "AI: completed"
                    : "AI: idle"
              }
              color={
                aiRunning ? "warning" : aiCompletedAt ? "success" : "default"
              }
            />
            <Button
              size="small"
              variant="outlined"
              disabled={aiRunning}
              onClick={handleRunAI}
            >
              {aiRunning ? "Running…" : "Run AI"}
            </Button>
          </Stack>
          <Stack
            direction="row"
            spacing={1}
            sx={{ flexWrap: "wrap", rowGap: 1 }}
          >
            <Button
              variant="text"
              onClick={() => {
                // Ensure no submission happens implicitly when navigating away
                setOpenSummary(false);
                setConfirmArmed(false);
                navigate("/");
              }}
            >
              Change
            </Button>
            <Tooltip title="Previous (Left)">
              <span>
                <Button
                  variant="outlined"
                  onClick={handlePrev}
                  disabled={loadingScans || scans.length === 0}
                >
                  Prev
                </Button>
              </span>
            </Tooltip>
            <Tooltip title={isDeleted ? "Restore (D)" : "Delete (D)"}>
              <span>
                <Button
                  variant={isDeleted ? "contained" : "outlined"}
                  color={isDeleted ? "success" : "error"}
                  onClick={onDeleteScan}
                  disabled={loadingScans || scans.length === 0}
                >
                  {isDeleted ? "Restore" : "Delete"}
                </Button>
              </span>
            </Tooltip>
            <FormControl
              size="small"
              sx={{ minWidth: { xs: 140, sm: 160, md: 200 } }}
            >
              <InputLabel id="view-filter-label">View</InputLabel>
              <Select
                labelId="view-filter-label"
                label="View"
                value={scanFilter}
                disabled={loadingScans}
                onChange={(e) => setScanFilter(e.target.value)}
              >
                <MenuItem value="normal">Normal only</MenuItem>
                <MenuItem value="all">All (include invalid)</MenuItem>
                <MenuItem value="invalidOnly">Invalid only</MenuItem>
              </Select>
            </FormControl>
            <FormControl
              size="small"
              sx={{ minWidth: { xs: 140, sm: 160, md: 200 } }}
              disabled={
                loadingScans ||
                scanFilter === "invalidOnly" ||
                scanFilter === "all"
              }
            >
              <InputLabel id="manual-filter-label">Show</InputLabel>
              <Select
                labelId="manual-filter-label"
                label="Show"
                value={scanViewMode}
                disabled={
                  loadingScans ||
                  scanFilter === "invalidOnly" ||
                  scanFilter === "all"
                }
                onChange={(e) => setScanViewMode(e.target.value)}
              >
                <MenuItem value="all">All scans</MenuItem>
                <MenuItem value="manual">Needs manual only</MenuItem>
                <MenuItem value="automated">Automated only</MenuItem>
              </Select>
            </FormControl>
            <Tooltip title="Next (Right)">
              <span>
                <Button
                  variant="outlined"
                  onClick={handleNext}
                  disabled={loadingScans || scans.length === 0}
                >
                  Next
                </Button>
              </span>
            </Tooltip>
            <Tooltip title="Submit (Enter)">
              <span>
                <Button
                  variant="contained"
                  color="primary"
                  onClick={onSubmitAudit}
                  disabled={loadingScans || scans.length === 0}
                >
                  Submit
                </Button>
              </span>
            </Tooltip>
          </Stack>
        </Stack>
        {(propagating || noData) && (
          <Box sx={{ mt: 1 }}>
            {propagating && (
              <Alert
                severity="info"
                icon={false}
                sx={{ display: "flex", alignItems: "center", gap: 1 }}
              >
                <CircularProgress size={16} sx={{ mr: 1 }} />
                Downloading and processing scans… This can take a few minutes.
                Please wait.
              </Alert>
            )}
            {!propagating && noData && (
              <Alert severity="warning">
                We finished processing, but no scans were found for this date.
              </Alert>
            )}
          </Box>
        )}
        {(loadingScans || loadingPans) && (
          <Box sx={{ mt: 1 }}>
            <LinearProgress />
          </Box>
        )}
      </Paper>

      {/* Main area fits viewport: 2 rows (content + gallery). On small screens, gallery grows to 200px */}
      <Box
        sx={{
          height: "calc(100vh - 96px)",
          display: "grid",
          gridTemplateRows: {
            xs: "1fr 200px",
            sm: "1fr 160px",
            md: "1fr 150px",
          },
          gap: 1,
          overflow: "hidden",
        }}
      >
        {/* Row 1: Info | Scan | Compare */}
        <Box
          sx={{
            display: "grid",
            gridTemplateColumns: {
              xs: "1fr",
              sm: "240px 1fr",
              md: "260px 1fr 320px",
              lg: "280px 1fr 360px",
            },
            gridTemplateRows: { xs: "auto 1fr auto", sm: "1fr" },
            gap: 1,
            minHeight: 0,
          }}
        >
          {/* Info panel */}
          <Paper
            sx={{
              p: 1.25,
              height: "100%",
              overflowX: "hidden",
              overflowY: "auto",
              display: "grid",
              gridTemplateRows: "auto auto auto 1fr",
              rowGap: 1,
              order: { xs: 2, sm: 1 },
              maxHeight: "100%",
            }}
          >
            {/* Venue information */}
            <Box sx={{ minHeight: "fit-content" }}>
              <Typography variant="subtitle2" gutterBottom>
                Venue
              </Typography>
              <Typography variant="body2" color="text.secondary">
                {restaurant?.venueName || restaurant?.VenueName || "N/A"}
              </Typography>
            </Box>
            {/* Model suggestions — responsive sizing across breakpoints */}
            <Box sx={{ minHeight: "fit-content", display: "block" }}>
              <Typography variant="subtitle2" gutterBottom>
                Model Suggestions
              </Typography>
              <Stack spacing={0.75}>
                {/* GenAI */}
                <Paper
                  variant="outlined"
                  sx={{
                    p: { xs: 0.5, sm: 0.6, md: 0.75 },
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    minHeight: { xs: 28, sm: 30, md: 32 },
                  }}
                >
                  <Stack
                    direction="row"
                    spacing={1}
                    alignItems="center"
                    sx={{ minWidth: 0 }}
                  >
                    <Chip label="GenAI" color="secondary" size="small" />
                    <Typography
                      variant="caption"
                      noWrap
                      sx={{
                        maxWidth: {
                          xs: 100,
                          sm: 140,
                          md: 160,
                          lg: 180,
                          xl: 220,
                        },
                      }}
                    >
                      Pan ID: {genAIPanId ? shortenId(genAIPanId) : "N/A"}
                    </Typography>
                  </Stack>
                  <Button
                    size="small"
                    onClick={() => genAIPanId && handlePanSelect(genAIPanId)}
                    disabled={!genAIPanId}
                    variant="text"
                    sx={{ fontSize: { xs: 11, sm: 12 } }}
                  >
                    Use
                  </Button>
                </Paper>
                {/* YOLO */}
                <Paper
                  variant="outlined"
                  sx={{
                    p: { xs: 0.5, sm: 0.6, md: 0.75 },
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    minHeight: { xs: 28, sm: 30, md: 32 },
                  }}
                >
                  <Stack
                    direction="row"
                    spacing={1}
                    alignItems="center"
                    sx={{ minWidth: 0 }}
                  >
                    <Chip label="YOLO" color="success" size="small" />
                    <Typography
                      variant="caption"
                      noWrap
                      sx={{
                        maxWidth: {
                          xs: 100,
                          sm: 140,
                          md: 160,
                          lg: 180,
                          xl: 220,
                        },
                      }}
                    >
                      Pan ID: {yoloPanId ? shortenId(yoloPanId) : "N/A"}
                    </Typography>
                  </Stack>
                  <Button
                    size="small"
                    onClick={() => yoloPanId && handlePanSelect(yoloPanId)}
                    disabled={!yoloPanId}
                    variant="text"
                    sx={{ fontSize: { xs: 11, sm: 12 } }}
                  >
                    Use
                  </Button>
                </Paper>
                {/* Corner */}
                <Paper
                  variant="outlined"
                  sx={{
                    p: { xs: 0.5, sm: 0.6, md: 0.75 },
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                    minHeight: { xs: 28, sm: 30, md: 32 },
                  }}
                >
                  <Stack
                    direction="row"
                    spacing={1}
                    alignItems="center"
                    sx={{ minWidth: 0 }}
                  >
                    <Chip label="Corner" color="warning" size="small" />
                    <Typography
                      variant="caption"
                      noWrap
                      sx={{
                        maxWidth: {
                          xs: 100,
                          sm: 140,
                          md: 160,
                          lg: 180,
                          xl: 220,
                        },
                      }}
                    >
                      Pan ID: {cornerPanId ? shortenId(cornerPanId) : "N/A"}
                    </Typography>
                  </Stack>
                  <Button
                    size="small"
                    onClick={() => cornerPanId && handlePanSelect(cornerPanId)}
                    disabled={!cornerPanId}
                    variant="text"
                    sx={{ fontSize: { xs: 11, sm: 12 } }}
                  >
                    Use
                  </Button>
                </Paper>
              </Stack>
            </Box>
            <Typography variant="subtitle1" gutterBottom>
              Scan Details
            </Typography>
            {!currentScan ? (
              <Typography color="text.secondary" variant="body2">
                No scan selected
              </Typography>
            ) : (
              <Stack
                spacing={0.5}
                sx={{
                  fontSize: 12,
                  maxHeight: "40vh",
                  overflowY: "auto",
                  "@media (min-width:1920px)": {
                    maxHeight: "unset",
                    overflowY: "visible",
                  },
                }}
              >
                <Typography variant="caption">
                  Scan ID: {currentScan.scanId}
                </Typography>
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{
                    display: "block",
                    whiteSpace: "normal",
                    "@media (min-width:1920px)": {
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                    },
                  }}
                >
                  Detected: {detectedScanSpecs}
                </Typography>
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{
                    display: "block",
                    whiteSpace: "normal",
                    "@media (min-width:1920px)": {
                      whiteSpace: "nowrap",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                    },
                  }}
                >
                  {scanMeasuresSpecs}
                </Typography>
                <Typography variant="caption">
                  Detected Pan: {currentScan.identifiedPan}
                </Typography>
                <Typography variant="caption">
                  Reported Menu: {currentScan.reportedMenuItemName}
                </Typography>
                <Divider sx={{ my: 1 }} />
                <Typography variant="caption" color="text.secondary">
                  AI Suggestion
                </Typography>
                <Typography variant="caption">
                  Pan ID: {genAIPanId ?? "—"}
                </Typography>
                <Typography variant="caption">
                  Conf: {currentScan.genAIPanConfidence ?? "—"}
                </Typography>
                {/* Compact reason indicator (color only) */}
                <Box
                  sx={{
                    mt: 0.25,
                    display: "flex",
                    alignItems: "center",
                    gap: 0.5,
                    flexWrap: "wrap",
                  }}
                >
                  <Typography variant="caption" color="text.secondary">
                    Reasons
                  </Typography>
                  {reasonCodes.length === 0 ? (
                    <Typography variant="caption">—</Typography>
                  ) : (
                    reasonCodes.map((code) => {
                      const meta = reasonCatalog[code] || {
                        label: code,
                        color: "info",
                      };
                      return (
                        <Chip
                          key={code}
                          label={meta.label}
                          color={meta.color}
                          size="small"
                          variant="outlined"
                        />
                      );
                    })
                  )}
                </Box>
                <Divider sx={{ my: 1 }} />
                <Typography variant="caption" color="text.secondary">
                  Auditor
                </Typography>
                <Typography variant="caption">
                  Pan ID:{" "}
                  {getScanAction(audits, currentScan.scanId)?.panId ||
                    "Not Selected"}
                </Typography>
                <Box
                  sx={{ display: "grid", gridTemplateColumns: "1fr", gap: 0.5 }}
                >
                  <Typography variant="caption">Menu Item</Typography>
                  <Autocomplete
                    freeSolo
                    size="small"
                    loading={menuLoading}
                    options={menuOptions}
                    inputValue={menuQuery}
                    getOptionLabel={(opt) =>
                      typeof opt === "string" ? opt : opt?.name || ""
                    }
                    renderInput={(params) => (
                      <TextField
                        {...params}
                        placeholder="Search menu items…"
                        size="small"
                        onKeyDown={(e) => {
                          // Prevent global hotkeys while typing in the menu box
                          e.stopPropagation();
                        }}
                        onBlur={() => {
                          const text = (menuQuery || "").trim();
                          if (!text) return;
                          setAudits((prev) => ({
                            ...prev,
                            actions: prev.actions.map((a) =>
                              a.scanId === currentScan.scanId &&
                              a.menuItemId == null
                                ? { ...a, menuItemName: text }
                                : a,
                            ),
                          }));
                        }}
                      />
                    )}
                    onInputChange={(_, value) => {
                      setMenuQuery(value);
                    }}
                    onChange={(_, value) => {
                      const picked =
                        typeof value === "string"
                          ? { id: null, name: value }
                          : value;
                      setAudits((prev) => ({
                        ...prev,
                        actions: prev.actions.map((a) =>
                          a.scanId === currentScan.scanId
                            ? { ...a, menuItemId: picked?.id || null }
                            : a,
                        ),
                      }));
                    }}
                  />
                </Box>
                <Box>
                  {getScanAction(audits, currentScan.scanId)?.delete ? (
                    <Chip
                      size="small"
                      color="error"
                      label="Marked for deletion"
                    />
                  ) : (
                    <Chip
                      size="small"
                      color="success"
                      label="Keep"
                      variant="outlined"
                    />
                  )}
                </Box>
              </Stack>
            )}
          </Paper>

          {/* Scan canvas with overlay nav */}
          <Paper
            sx={{
              p: 0,
              position: "relative",
              height: "100%",
              order: { xs: 1, sm: 2 },
            }}
          >
            <IconButton
              onClick={handlePrev}
              size="large"
              sx={{
                position: "absolute",
                top: "50%",
                left: 8,
                transform: "translateY(-50%)",
                bgcolor: "rgba(255,255,255,0.8)",
              }}
            >
              <ArrowBackIos />
            </IconButton>
            <Box
              sx={{
                width: "100%",
                height: "100%",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                bgcolor: "grey.50",
                textAlign: "center",
                p: 2,
              }}
            >
              {loadingScans ? (
                <CircularProgress />
              ) : propagating ? (
                <Box>
                  <CircularProgress sx={{ mb: 2 }} />
                  <Typography variant="h6">
                    Downloading and processing scans
                  </Typography>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mb: 1 }}
                  >
                    This may take a few minutes. Please wait...
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    If you see an old or low-quality image here, it will update
                    automatically once the download completes.
                  </Typography>
                </Box>
              ) : noData ? (
                <Box>
                  <Typography variant="h6" color="warning.main">
                    No scans found for this date
                  </Typography>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mb: 2 }}
                  >
                    We've checked S3 and processed the data, but there are no
                    scan files available for {date}.
                  </Typography>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mb: 2 }}
                  >
                    This could mean no scans were uploaded for this date, or
                    there was an issue with the upload process.
                  </Typography>
                  <Button variant="contained" onClick={() => navigate("/")}>
                    Try Different Date
                  </Button>
                </Box>
              ) : scans.length === 0 ? (
                <Box>
                  <Typography variant="h6">No scans to audit</Typography>
                  <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ mb: 1 }}
                  >
                    Try a different date or restaurant.
                  </Typography>
                  <Button variant="contained" onClick={() => navigate("/")}>
                    Change selection
                  </Button>
                </Box>
              ) : (
                <Box
                  component="img"
                  src={currentScan?.imageUrl || currentScan?.imageBase64}
                  alt={`Scan ${currentScan?.scanId}`}
                  sx={{
                    maxWidth: "100%",
                    maxHeight: "100%",
                    objectFit: "contain",
                  }}
                />
              )}
              {isDeleted && !loadingScans && (
                <Box
                  aria-label="Marked for deletion overlay"
                  sx={{
                    position: "absolute",
                    inset: 0,
                    border: (t) => `2px solid ${t.palette.error.main}`,
                    backgroundColor: "rgba(211,47,47,0.12)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    pointerEvents: "none",
                  }}
                >
                  <Typography
                    sx={{
                      color: "error.main",
                      fontWeight: "bold",
                      fontSize: 28,
                      letterSpacing: 2,
                      textTransform: "uppercase",
                      px: 2,
                      py: 0.5,
                      bgcolor: "rgba(255,255,255,0.85)",
                      borderRadius: 1,
                    }}
                  >
                    Marked for deletion
                  </Typography>
                </Box>
              )}
            </Box>
            <IconButton
              onClick={handleNext}
              size="large"
              sx={{
                position: "absolute",
                top: "50%",
                right: 8,
                transform: "translateY(-50%)",
                bgcolor: "rgba(255,255,255,0.8)",
              }}
            >
              <ArrowForwardIos />
            </IconButton>
          </Paper>

          {/* Compare panel */}
          <Paper
            sx={{
              p: 1,
              height: "100%",
              display: "grid",
              gridTemplateRows: {
                xs: "1fr 1.6fr",
                md: "1fr 1.8fr",
                lg: "1fr 2fr",
              },
              gap: 1,
              order: { xs: 3, sm: 3 },
            }}
          >
            <Box>
              <Stack
                direction="row"
                alignItems="center"
                spacing={1}
                sx={{ mb: 0.5 }}
              >
                <Chip
                  label={recommendationMeta.label}
                  size="small"
                  color={recommendationMeta.color}
                />
                <Typography variant="caption" color="text.secondary">
                  Pan ID: {currentScan?.genAIPanId ?? "—"}
                </Typography>
              </Stack>
              <Box
                sx={{
                  height: "calc(100% - 22px)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  bgcolor: "grey.50",
                  borderRadius: 1,
                  border: 1,
                  borderColor: (t) => t.palette[recommendationMeta.color]?.main,
                }}
              >
                {aiSuggestedPan?.imageUrl || aiSuggestedPan?.imageBase64 ? (
                  <Box
                    component="img"
                    src={aiSuggestedPan.imageUrl || aiSuggestedPan.imageBase64}
                    alt={`Pan ${aiSuggestedPan?.ID}`}
                    sx={{
                      maxWidth: "100%",
                      maxHeight: "100%",
                      objectFit: "contain",
                    }}
                  />
                ) : (
                  <Typography color="text.secondary" variant="caption">
                    No image
                  </Typography>
                )}
              </Box>
            </Box>
            <Box>
              <Stack
                direction="row"
                alignItems="center"
                spacing={1}
                sx={{ mb: 0.5 }}
              >
                <Chip label="Selected" color="primary" size="small" />
                {selectedPan?.wasAudited && (
                  <Chip
                    label="Audited"
                    color="primary"
                    size="small"
                    variant="outlined"
                  />
                )}
                <Typography variant="caption" color="text.secondary">
                  Pan ID: {selectedPanId ?? "—"}
                </Typography>
              </Stack>
              <Typography
                variant="caption"
                color="text.secondary"
                sx={{
                  mb: 0.5,
                  display: "block",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
              >
                {selectedPanSpecs}
              </Typography>
              <Box
                sx={{
                  height: "calc(100% - 22px)",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  bgcolor: "grey.50",
                  borderRadius: 1,
                  position: "relative",
                }}
              >
                {selectedPan?.imageUrl || selectedPan?.imageBase64 ? (
                  <ButtonBase
                    onClick={() => setOpenSelectedZoom(true)}
                    sx={{
                      width: "100%",
                      height: "100%",
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "center",
                    }}
                  >
                    <Box
                      component="img"
                      src={selectedPan.imageUrl || selectedPan.imageBase64}
                      alt={`Pan ${selectedPan?.ID}`}
                      sx={{
                        maxWidth: "100%",
                        maxHeight: "100%",
                        objectFit: "contain",
                      }}
                    />
                    <Typography
                      variant="caption"
                      sx={{
                        position: "absolute",
                        right: 6,
                        bottom: 6,
                        bgcolor: "rgba(0,0,0,0.55)",
                        color: "#fff",
                        px: 0.5,
                        borderRadius: 0.5,
                      }}
                    >
                      Click to zoom
                    </Typography>
                  </ButtonBase>
                ) : (
                  <Typography color="text.secondary" variant="caption">
                    Not selected
                  </Typography>
                )}
              </Box>
            </Box>
          </Paper>
        </Box>

        {/* Row 2: Filters + one-row pan gallery (horizontal scroll only) */}
        <Paper sx={{ p: 1, display: "grid", gridTemplateRows: "auto 1fr" }}>
          <Stack
            direction="row"
            alignItems="center"
            spacing={1.5}
            sx={{ mb: 0.5, flexWrap: "wrap" }}
          >
            <Typography variant="subtitle2">Pick a Pan</Typography>
            {/* Always show availability chips */}
            <Stack direction="row" spacing={1} alignItems="center">
              <Chip
                label={genaiPan ? "GenAI" : "GenAI: N/A"}
                color="secondary"
                size="small"
                variant={genaiPan ? "filled" : "outlined"}
                onClick={
                  genaiPan ? () => handlePanSelect(genAIPanId) : undefined
                }
              />
              <Chip
                label={yoloPan ? "YOLO" : "YOLO: N/A"}
                color="success"
                size="small"
                variant={yoloPan ? "filled" : "outlined"}
                onClick={yoloPan ? () => handlePanSelect(yoloPanId) : undefined}
              />
              <Chip
                label={cornerPan ? "Corner" : "Corner: N/A"}
                color="warning"
                size="small"
                variant={cornerPan ? "filled" : "outlined"}
                onClick={
                  cornerPan ? () => handlePanSelect(cornerPanId) : undefined
                }
              />
            </Stack>
            <FormControl size="small" sx={{ minWidth: { xs: 100, sm: 120 } }}>
              <InputLabel id="shape-filter-label" shrink>
                Shape
              </InputLabel>
              <Select
                id="shape-filter"
                labelId="shape-filter-label"
                value={filterPanShape}
                label="Shape"
                onChange={(e) => {
                  setFilterPanShape(e.target.value);
                  if (e.target.value === "" || e.target.value === "3") {
                    setFilterPanSize("");
                  }
                }}
                displayEmpty
              >
                <MenuItem value="">
                  <em>All</em>
                </MenuItem>
                <MenuItem value="1">Rectangular</MenuItem>
                <MenuItem value="3">Oval</MenuItem>
              </Select>
            </FormControl>
            <FormControl size="small" sx={{ minWidth: { xs: 100, sm: 120 } }}>
              <InputLabel id="size-filter-label" shrink>
                Size
              </InputLabel>
              <Select
                id="size-filter"
                labelId="size-filter-label"
                value={filterPanSize}
                label="Size"
                onChange={(e) => setFilterPanSize(e.target.value)}
                displayEmpty
              >
                <MenuItem value="">
                  <em>All</em>
                </MenuItem>
                <MenuItem value="Full">Full</MenuItem>
                <MenuItem value="1/2">1/2</MenuItem>
                <MenuItem value="1/2 Long">1/2 Long</MenuItem>
                <MenuItem value="1/3">1/3</MenuItem>
                <MenuItem value="1/4">1/4</MenuItem>
                <MenuItem value="1/6">1/6</MenuItem>
              </Select>
            </FormControl>
          </Stack>
          <Box
            sx={{
              display: "flex",
              overflowX: "auto",
              overflowY: "hidden",
              gap: 1,
              py: 0.5,
              alignItems: "stretch",
              "&::-webkit-scrollbar": { height: 6 },
              "&::-webkit-scrollbar-thumb": {
                bgcolor: "grey.400",
                borderRadius: 3,
              },
            }}
          >
            {loadingPans &&
              Array.from({ length: 8 }).map((_, i) => (
                <Paper
                  key={`sk-${i}`}
                  sx={{ flex: "0 0 auto", width: 140, height: 120, p: 0.5 }}
                >
                  <Skeleton variant="rectangular" width="100%" height={80} />
                  <Skeleton variant="text" sx={{ fontSize: 12, mt: 0.5 }} />
                </Paper>
              ))}
            {!loadingPans &&
              filteredPans.map((pan, idx) => {
                const isSelected = selectedPanId === pan.ID;
                const isGenAI = genaiPan?.ID === pan.ID;
                const isYOLO = yoloPan?.ID === pan.ID;
                const isCorner = cornerPan?.ID === pan.ID;
                const isSuggested = isGenAI || isYOLO || isCorner;
                return (
                  <Paper
                    key={pan.ID}
                    onClick={() => handlePanSelect(pan.ID)}
                    sx={{
                      flex: "0 0 auto",
                      width: 140,
                      height: 120,
                      p: 0.5,
                      textAlign: "center",
                      cursor: "pointer",
                      position: "relative",
                      borderWidth: 2,
                      borderStyle:
                        isSuggested && !isSelected ? "dashed" : "solid",
                      borderColor: (t) => {
                        if (isSelected) return t.palette.primary.main;
                        if (isGenAI) return t.palette.secondary.main;
                        if (isYOLO) return t.palette.success.main;
                        if (isCorner) return t.palette.warning.main;
                        return "transparent";
                      },
                      transition: "border-color 0.15s",
                    }}
                    elevation={isSelected ? 6 : 1}
                  >
                    {idx < 9 && (
                      <Box
                        sx={{
                          position: "absolute",
                          top: 4,
                          left: 4,
                          bgcolor: "rgba(0,0,0,0.6)",
                          color: "#fff",
                          borderRadius: 1,
                          px: 0.5,
                          fontSize: 10,
                        }}
                      >
                        {idx + 1}
                      </Box>
                    )}
                    {pan.wasAudited && (
                      <Chip
                        size="small"
                        label="Audited"
                        color="primary"
                        sx={{ position: "absolute", top: 4, left: 36 }}
                      />
                    )}
                    {isGenAI && (
                      <Chip
                        size="small"
                        label="GenAI"
                        color="secondary"
                        sx={{ position: "absolute", top: 4, right: 4 }}
                      />
                    )}
                    {isYOLO && (
                      <Chip
                        size="small"
                        label="YOLO"
                        color="success"
                        sx={{ position: "absolute", bottom: 4, left: 4 }}
                      />
                    )}
                    {isCorner && (
                      <Chip
                        size="small"
                        label="Corner"
                        color="warning"
                        sx={{ position: "absolute", bottom: 4, right: 4 }}
                      />
                    )}
                    <ButtonBase
                      sx={{
                        width: "100%",
                        height: 80,
                        mb: 0.5,
                        bgcolor: "grey.100",
                      }}
                    >
                      <Box
                        component="img"
                        src={pan.imageUrl || pan.imageBase64}
                        alt={`Pan ${pan.ID}`}
                        sx={{ maxWidth: "100%", maxHeight: "100%" }}
                      />
                    </ButtonBase>
                    <Typography variant="caption" display="block">
                      {pan["dbSizeStandard"] || ""},{" "}
                      {pan["Depth"] ? pan["Depth"] + '"' : ""}
                    </Typography>
                  </Paper>
                );
              })}
            {!loadingPans && filteredPans.length === 0 && (
              <Typography color="text.secondary" sx={{ m: 2 }}>
                No pans match your filters.
              </Typography>
            )}
          </Box>
        </Paper>
      </Box>

      <SummaryDialog
        open={openSummary}
        onClose={() => {
          setOpenSummary(false);
          setConfirmArmed(false);
        }}
        audits={audits}
        manualScanIds={manualScanIds}
        onConfirm={handleConfirm}
        confirming={isSubmitting}
      />

      {/* Selected Pan Zoom Dialog */}
      <Dialog
        open={openSelectedZoom}
        onClose={() => setOpenSelectedZoom(false)}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>Selected Pan</DialogTitle>
        <DialogContent
          dividers
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            bgcolor: "grey.50",
          }}
        >
          {selectedPan?.imageUrl || selectedPan?.imageBase64 ? (
            <Box
              component="img"
              src={selectedPan.imageUrl || selectedPan.imageBase64}
              alt={`Pan ${selectedPan?.ID}`}
              sx={{ maxWidth: "100%", maxHeight: "70vh", objectFit: "contain" }}
            />
          ) : (
            <Typography color="text.secondary">No image</Typography>
          )}
        </DialogContent>
        <DialogActions>
          <Button
            onClick={() => setOpenSelectedZoom(false)}
            variant="contained"
          >
            Close
          </Button>
        </DialogActions>
      </Dialog>

      {/* Change Date & Restaurant Dialog */}
      <Dialog
        open={openChangeDialog}
        onClose={() => setOpenChangeDialog(false)}
        maxWidth="sm"
        fullWidth
      >
        <DialogTitle>Change date or restaurant</DialogTitle>
        <DialogContent dividers>
          <Stack spacing={2}>
            <LocalizationProvider dateAdapter={AdapterDateFns}>
              <DatePicker
                label="Audit date"
                value={dateLocal}
                onChange={setDateLocal}
                disableFuture
                renderInput={(params) => <TextField {...params} fullWidth />}
              />
            </LocalizationProvider>
            <Autocomplete
              fullWidth
              loading={loadingRestaurants}
              options={restaurants}
              getOptionLabel={(option) => option?.name || ""}
              value={restaurantLocal}
              onChange={(_, value) => setRestaurantLocal(value)}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Restaurant"
                  placeholder={
                    loadingRestaurants
                      ? "Loading restaurants…"
                      : "Select restaurant"
                  }
                  helperText={restaurantsError || ""}
                />
              )}
            />
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setOpenChangeDialog(false)}>Cancel</Button>
          <Button
            variant="contained"
            disabled={!dateLocal || !restaurantLocal}
            onClick={() => {
              const dateStr =
                dateLocal instanceof Date
                  ? `${dateLocal.getFullYear()}-${String(dateLocal.getMonth() + 1).padStart(2, "0")}-${String(dateLocal.getDate()).padStart(2, "0")}`
                  : date;
              setOpenChangeDialog(false);
              navigate("/audit", {
                state: { date: dateStr, restaurant: restaurantLocal },
              });
            }}
          >
            Apply
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
