import React, { useState, useEffect, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { format, isAfter, startOfDay } from "date-fns";
import {
  Container,
  Card,
  CardContent,
  Typography,
  TextField,
  MenuItem,
  Button,
  Box,
  Stack,
  CircularProgress,
  Alert,
  Chip,
} from "@mui/material";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import { AdapterDateFns } from "@mui/x-date-pickers/AdapterDateFns";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import Autocomplete from "@mui/material/Autocomplete";
import { cacheHomeState, getCachedHomeState } from "../utils/auditUtils";

export default function Home() {
  const [date, setDate] = useState(null);
  const [selectedRestaurant, setSelectedRestaurant] = useState(null);
  const [loadingRestaurants, setLoadingRestaurants] = useState(false);
  const [restaurantsError, setRestaurantsError] = useState("");
  const [restaurantsWithScans, setRestaurantsWithScans] = useState([]);
  const [dateLoadedFromCache, setDateLoadedFromCache] = useState(false);
  const navigate = useNavigate();
  const apiBaseUrl = useMemo(() => {
    const envUrl = import.meta.env.VITE_API_BASE_URL;
    if (envUrl && String(envUrl).trim() !== "") return envUrl;
    // Default to same-origin proxy (nginx forwards /api/* to backend)
    return "";
  }, []);

  // Load cached date on component mount
  useEffect(() => {
    const cachedState = getCachedHomeState();
    if (cachedState.date) {
      setDate(cachedState.date);
      setDateLoadedFromCache(true);
    }
  }, []);

  // Clear any selected restaurant when date changes to avoid stale selection
  useEffect(() => {
    setSelectedRestaurant(null);
  }, [date]);

  // Cache date changes
  useEffect(() => {
    if (date) {
      cacheHomeState(date);
    }
  }, [date]);

  // Cleanup cache when component unmounts (optional - keeps cache for navigation back)
  // useEffect(() => {
  //   return () => {
  //     // Uncomment the line below if you want to clear cache on unmount
  //     // cacheHomeState(null);
  //   };
  // }, []);

  // Fetch restaurants with scan counts when date changes
  useEffect(() => {
    if (!date) {
      setRestaurantsWithScans([]);
      return;
    }

    setLoadingRestaurants(true);
    setRestaurantsError("");
    const dateString = format(date, "yyyy-MM-dd");

    axios
      .get(`${apiBaseUrl}/api/restaurants/with-scans`, {
        params: { date: dateString },
      })
      .then((res) => {
        setRestaurantsWithScans(res.data.restaurants || []);
      })
      .catch(() =>
        setRestaurantsError("Failed to load restaurants with scan data"),
      )
      .finally(() => setLoadingRestaurants(false));
  }, [date, apiBaseUrl]);

  const handleStart = () => {
    if (!date || !selectedRestaurant) return;
    const today = startOfDay(new Date());
    const chosen = startOfDay(date);
    if (isAfter(chosen, today)) {
      alert("You cannot audit a future date.");
      return;
    }
    const dateString = format(date, "yyyy-MM-dd");

    // Clear the cache when starting an audit for a fresh start next time
    cacheHomeState(null);

    navigate("/audit", {
      state: { date: dateString, restaurant: selectedRestaurant },
    });
  };

  const futureDateSelected = (() => {
    if (!date) return false;
    const today = startOfDay(new Date());
    const chosen = startOfDay(date);
    return isAfter(chosen, today);
  })();

  return (
    <Box
      sx={{
        minHeight: "100vh",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        px: { xs: 1, sm: 2 },
        background: "linear-gradient(135deg, #f5f7fa 0%, #e6ecf5 100%)",
      }}
    >
      <Card
        sx={{ width: "100%", maxWidth: 560, boxShadow: 6, borderRadius: 3 }}
      >
        <CardContent sx={{ p: { xs: 3, md: 4 } }}>
          <Stack spacing={3} alignItems="stretch">
            <Stack spacing={1} alignItems="center">
              <Box
                component="img"
                src="/metafoodx_logo.png"
                alt="MetaFoodX"
                sx={{ height: { xs: 36, sm: 48 } }}
              />
              <Typography
                variant="h5"
                fontWeight={700}
                sx={{ fontSize: { xs: 20, sm: 24 } }}
              >
                Scan Audit
              </Typography>
              <Typography variant="body2" color="text.secondary" align="center">
                Choose a date and a restaurant to begin reviewing scans.
              </Typography>
            </Stack>

            {restaurantsError && (
              <Alert severity="error">{restaurantsError}</Alert>
            )}
            <LocalizationProvider dateAdapter={AdapterDateFns}>
              <DatePicker
                label="Audit date"
                value={date}
                onChange={(newValue) => {
                  setDate(newValue);
                  // Clear cache if date is manually cleared
                  if (!newValue) {
                    cacheHomeState(null);
                  }
                  // Reset cache indicator when user manually changes date
                  setDateLoadedFromCache(false);
                }}
                disableFuture
                maxDate={new Date()}
                renderInput={(params) => (
                  <TextField
                    {...params}
                    fullWidth
                    helperText={
                      futureDateSelected
                        ? "Future dates are not allowed"
                        : params?.helperText
                    }
                  />
                )}
              />
              {dateLoadedFromCache && date && (
                <Typography
                  variant="caption"
                  color="text.secondary"
                  sx={{ mt: 0.5, display: "block" }}
                >
                  Date restored from previous session
                </Typography>
              )}
            </LocalizationProvider>

            <Autocomplete
              fullWidth
              disabled={!date}
              loading={!!date && loadingRestaurants}
              options={date ? restaurantsWithScans : []}
              getOptionLabel={(option) => option?.name || ""}
              isOptionEqualToValue={(option, value) =>
                (option?.id ?? option?.ID) === (value?.id ?? value?.ID)
              }
              value={selectedRestaurant}
              onChange={(_, value) => setSelectedRestaurant(value)}
              renderInput={(params) => (
                <TextField
                  {...params}
                  label="Restaurant"
                  placeholder={
                    !date
                      ? "Select a date first"
                      : loadingRestaurants
                        ? "Loading restaurants…"
                        : "Select restaurant with scans..."
                  }
                  disabled={!date}
                  InputProps={{
                    ...params.InputProps,
                    endAdornment: (
                      <>
                        {date && loadingRestaurants ? (
                          <CircularProgress color="inherit" size={16} />
                        ) : null}
                        {params.InputProps.endAdornment}
                      </>
                    ),
                  }}
                />
              )}
              renderOption={(props, option) => (
                <li {...props} key={option?.id ?? option?.ID ?? option?.name}>
                  <Box
                    sx={{
                      width: "100%",
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                    }}
                  >
                    <Typography variant="body1">
                      {option?.name ?? "Unnamed"}
                    </Typography>
                    {date && option?.scanCount !== undefined && (
                      <Box
                        sx={{ display: "flex", alignItems: "center", gap: 1 }}
                      >
                        <Chip
                          label={`${option.scanCount} scans`}
                          size="small"
                          color="primary"
                          variant="outlined"
                        />
                        {option.flaggedScanCount > 0 && (
                          <Chip
                            label={`${option.flaggedScanCount} flagged`}
                            size="small"
                            color="warning"
                            variant="outlined"
                          />
                        )}
                        {option.activeAuditors > 0 && (
                          <Chip
                            label={`${option.activeAuditors} auditor${option.activeAuditors > 1 ? "s" : ""} active`}
                            size="small"
                            color="error"
                            variant="outlined"
                          />
                        )}
                      </Box>
                    )}
                  </Box>
                </li>
              )}
            />

            {date &&
              restaurantsWithScans.length === 0 &&
              !loadingRestaurants && (
                <Alert severity="info">
                  No restaurants have scans on {format(date, "MMMM d, yyyy")}.
                  Try selecting a different date.
                </Alert>
              )}

            {selectedRestaurant && date && selectedRestaurant.scanCount && (
              <Alert
                severity={
                  selectedRestaurant.activeAuditors > 0 ? "warning" : "success"
                }
              >
                {selectedRestaurant.name} has {selectedRestaurant.scanCount}{" "}
                scans to audit on {format(date, "MMMM d, yyyy")}
                {selectedRestaurant.flaggedScanCount > 0 &&
                  ` (including ${selectedRestaurant.flaggedScanCount} flagged scans)`}
                {selectedRestaurant.activeAuditors > 0 &&
                  ` • ${selectedRestaurant.activeAuditors} auditor${selectedRestaurant.activeAuditors > 1 ? "s" : ""} currently active for this date.`}
              </Alert>
            )}

            <Button
              size="large"
              variant="contained"
              disabled={
                !date ||
                !selectedRestaurant ||
                loadingRestaurants ||
                futureDateSelected ||
                selectedRestaurant?.activeAuditors > 0
              }
              onClick={handleStart}
            >
              Start Audit
            </Button>
          </Stack>
        </CardContent>
      </Card>
    </Box>
  );
}
