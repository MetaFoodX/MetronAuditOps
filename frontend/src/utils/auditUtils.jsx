import moment from "moment-timezone";

// Cache keys for Home page state
const HOME_CACHE_KEYS = {
  SELECTED_DATE: "home_selected_date",
  SELECTED_RESTAURANT: "home_selected_restaurant",
};

/**
 * Caches the Home page state, specifically the selected date.
 * Always clears the restaurant selection to ensure fresh restaurant data.
 *
 * @param {Date|null} date - The date to cache, or null to clear the cache
 */
export function cacheHomeState(date) {
  try {
    if (date) {
      localStorage.setItem(HOME_CACHE_KEYS.SELECTED_DATE, date.toISOString());
    } else {
      localStorage.removeItem(HOME_CACHE_KEYS.SELECTED_DATE);
    }
    // Always clear restaurant selection when caching
    localStorage.removeItem(HOME_CACHE_KEYS.SELECTED_RESTAURANT);
  } catch (error) {
    console.warn("Failed to cache Home page state:", error);
  }
}

/**
 * Retrieves cached Home page state.
 * Only returns valid dates (not future dates, not invalid dates).
 * Automatically cleans up invalid cached data.
 *
 * @returns {Object} Object containing the cached date or null
 * @returns {Date|null} returns.date - The cached date or null if none/invalid
 */
export function getCachedHomeState() {
  try {
    const cachedDate = localStorage.getItem(HOME_CACHE_KEYS.SELECTED_DATE);
    if (cachedDate) {
      const date = new Date(cachedDate);
      // Only return valid dates (not future dates, not invalid dates)
      if (!isNaN(date.getTime()) && date <= new Date()) {
        return { date };
      } else {
        // Clear invalid cached date
        localStorage.removeItem(HOME_CACHE_KEYS.SELECTED_DATE);
      }
    }
    return { date: null };
  } catch (error) {
    console.warn("Failed to retrieve cached Home page state:", error);
    // Clear potentially corrupted cache
    try {
      localStorage.removeItem(HOME_CACHE_KEYS.SELECTED_DATE);
    } catch (clearError) {
      console.warn("Failed to clear corrupted cache:", clearError);
    }
    return { date: null };
  }
}

/**
 * Clears all cached Home page state.
 * Useful for resetting the user experience or debugging.
 */
export function clearHomeCache() {
  try {
    localStorage.removeItem(HOME_CACHE_KEYS.SELECTED_DATE);
    localStorage.removeItem(HOME_CACHE_KEYS.SELECTED_RESTAURANT);
  } catch (error) {
    console.warn("Failed to clear Home page cache:", error);
  }
}

export function initAuditSessionRecord(restaurant, date, scanReports) {
  const session = {
    restaurantId: restaurant.id,
    date: date,
    auditStartTime: moment().tz("America/Los_Angeles").format("YYYY-MM-DD"),
    auditEndTime: null,
    actions: scanReports.map((scan) => ({
      scanId: scan.scanId,
      delete: false,
      panId: null,
      menuItemId: null,
    })),
  };

  return session;
}

export function getFilteredPans(shape, sizeStandard, panList) {
  const normalizeSize = (v) => {
    if (v == null || v === "") return null;
    return String(v).trim().toLowerCase();
  };
  const wantShape = shape == null || shape === "" ? null : parseInt(shape);
  const wantSize =
    sizeStandard == null || sizeStandard === ""
      ? null
      : normalizeSize(sizeStandard);

  return panList.filter((pan) => {
    // Use dbShape and dbSizeStandard for filtering (these are always present)
    const panShape = pan["dbShape"];
    const panSize = normalizeSize(pan["dbSizeStandard"]);
    const shapeOk = wantShape == null ? true : panShape == wantShape; // loose equals to allow string/number
    const sizeOk = wantSize == null ? true : panSize === wantSize; // case-insensitive, trimmed
    return shapeOk && sizeOk;
  });
}

export function getScanAction(audits, scanId) {
  return audits.actions?.find((e) => e.scanId === scanId);
}
