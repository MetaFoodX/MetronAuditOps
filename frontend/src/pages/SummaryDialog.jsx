import React from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Button,
  Typography,
} from "@mui/material";

export default function SummaryDialog({
  open,
  onClose,
  audits,
  onConfirm,
  manualScanIds = [],
  confirming = false,
}) {
  // Only show scans that have actual changes (pan selection, menu item, or delete)
  const changedScans = (audits?.actions || []).filter(
    (action) =>
      action.delete ||
      (action.panId && action.panId !== "") ||
      (action.menuItemId && action.menuItemId !== "") ||
      (action.menuItemName && action.menuItemName !== ""),
  );

  return (
    <Dialog open={open} onClose={onClose} fullWidth maxWidth="md">
      <DialogTitle sx={{ bgcolor: "primary.main", color: "#fff" }}>
        Audit Summary - {changedScans.length} Changes
      </DialogTitle>

      <DialogContent dividers sx={{ p: 0 }}>
        <TableContainer component={Paper} sx={{ maxHeight: 400 }}>
          <Table stickyHeader>
            <TableHead>
              <TableRow sx={{ backgroundColor: "grey.100" }}>
                <TableCell sx={{ fontWeight: "bold" }}>Audited</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Scan ID</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Pan ID</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Menu Item</TableCell>
                <TableCell sx={{ fontWeight: "bold" }}>Deleted?</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {changedScans.map((a) => (
                <TableRow
                  key={a.scanId}
                  sx={{
                    "&:nth-of-type(odd)": { backgroundColor: "grey.50" },
                    "&:hover": {
                      backgroundColor: "primary.light",
                      cursor: "pointer",
                    },
                  }}
                >
                  <TableCell>
                    <Typography color="success.main">Audited</Typography>
                  </TableCell>
                  <TableCell>{a.scanId}</TableCell>
                  <TableCell>{a.panId}</TableCell>
                  <TableCell>{a.menuItemName || a.menuItemId || ""}</TableCell>
                  <TableCell>
                    {a.delete ? (
                      <Typography color="error">Yes</Typography>
                    ) : (
                      <Typography color="success.main">No</Typography>
                    )}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </DialogContent>

      <DialogActions sx={{ p: 2 }}>
        <Button onClick={onClose} disabled={confirming}>
          Cancel
        </Button>
        <Button
          variant="contained"
          color="primary"
          onClick={onConfirm}
          disabled={confirming}
        >
          Confirm
        </Button>
      </DialogActions>
    </Dialog>
  );
}
