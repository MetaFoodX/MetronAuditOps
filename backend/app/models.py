from enum import Enum

from datetime import datetime
from pydantic import BaseModel
from typing import Any, Dict, List, Optional


class AuditActionType(str, Enum):
    """Types of audit actions that can be performed"""

    DELETE = "delete"
    PAN_CHANGE = "pan_change"
    MENU_ITEM_CHANGE = "menu_item_change"
    VENUE_CHANGE = "venue_change"
    MEAL_PERIOD_CHANGE = "meal_period_change"


class AuditAction(BaseModel):
    """Individual audit action to be performed"""

    scan_id: str
    action_type: AuditActionType
    original_value: Optional[str] = None
    new_value: Optional[str] = None
    reason: Optional[str] = None


class AuditSession(BaseModel):
    """Audit session information"""

    session_id: str
    restaurant_id: int
    date: str
    auditor_id: Optional[str] = None
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str  # "in_progress", "completed", "failed"
    total_scans: int
    audited_scans: int
    actions_count: int


class AuditConfirmationRequest(BaseModel):
    """Request to confirm and apply audit actions"""

    session_id: Optional[str] = None
    restaurant_id: Optional[int] = None
    date: Optional[str] = None
    auditor_id: Optional[str] = None
    actions: List[AuditAction]
    confirm_all: bool = True
    notes: Optional[str] = None


class AuditConfirmationResponse(BaseModel):
    """Response after confirming audit actions"""

    success: bool
    session_id: str
    applied_actions: int
    failed_actions: int
    errors: List[str]
    timestamp: datetime
    crud_operations: Optional[Dict[str, Any]] = None


class ScanAuditData(BaseModel):
    """Scan audit data from DynamoDB"""

    scan_id: str
    restaurant_id: int
    date: str
    audit_status: Optional[str] = (
        None  # "deleted", "pan_updated", "menu_item_updated", "failed"
    )
    audit_action: Optional[str] = None
    audit_result: Optional[str] = None
    auditor_id: Optional[str] = None
    audit_session_id: Optional[str] = None
    audited_at: Optional[datetime] = None
    is_audited: Optional[str] = None
    original_value: Optional[str] = None
    new_value: Optional[str] = None
    audit_error: Optional[str] = None


class AuditSessionSummary(BaseModel):
    """Summary of audit session results"""

    session_id: str
    restaurant_id: int
    date: str
    total_scans: int
    audited_scans: int
    deleted_scans: int
    updated_scans: int
    failed_actions: int
    success_rate: float
    audit_progress: float
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_minutes: Optional[float] = None


class ComprehensiveAuditStatus(BaseModel):
    """Comprehensive audit status for restaurant and date"""

    restaurant_id: int
    date: str
    statistics: Dict[str, Any]
    audit_sessions: List[Dict[str, Any]]
    scan_audit_status: List[Dict[str, Any]]
