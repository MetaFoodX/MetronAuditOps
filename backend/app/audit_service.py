"""
Audit Service
============

Service for managing audit sessions and coordinating audit operations
between the UI, DynamoDB, and Skoopin server.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.dynamo_service import DynamoDBService
from app.models import AuditAction, AuditActionType, AuditSession
from app.skoopin_service import SkoopinService

logger = logging.getLogger(__name__)


class AuditService:
    """
    Service for managing audit sessions and operations
    """

    def __init__(
        self, skoopin_service: SkoopinService, dynamo_service: DynamoDBService
    ):
        self.skoopin_service = skoopin_service
        self.dynamo_service = dynamo_service

    def create_audit_session(
        self, restaurant_id: int, date: str, auditor_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new audit session

        Args:
            restaurant_id: Restaurant ID being audited
            date: Date being audited (YYYY-MM-DD)
            auditor_id: Optional auditor ID

        Returns:
            Session creation result
        """
        try:
            # Get scans for this restaurant/date to determine total count
            scans = self.dynamo_service.get_scans_by_restaurant_day(restaurant_id, date)
            total_scans = len(scans) if scans else 0

            # Create audit session
            session_id = self.dynamo_service.create_audit_session(
                restaurant_id=restaurant_id,
                date=date,
                total_scans=total_scans,
                auditor_id=auditor_id,
            )

            logger.info(
                f"Created audit session {session_id} for restaurant {restaurant_id} on {date}"
            )

            return {
                "success": True,
                "session_id": session_id,
                "restaurant_id": restaurant_id,
                "date": date,
                "total_scans": total_scans,
                "created_at": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to create audit session: {e}")
            raise

    def get_audit_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get audit session details and progress

        Args:
            session_id: Session ID to retrieve

        Returns:
            Session data with progress information
        """
        try:
            session = self.dynamo_service.get_audit_session(session_id)
            if not session:
                return None

            # Get progress information
            progress = self.dynamo_service.get_audit_progress(session_id)

            return {"session": session, "progress": progress}

        except Exception as e:
            logger.error(f"Failed to get audit session {session_id}: {e}")
            raise

    def apply_audit_actions(
        self, session_id: str, actions: List[AuditAction]
    ) -> Dict[str, Any]:
        """
        Apply audit actions to Skoopin server and update DynamoDB

        Args:
            session_id: Audit session ID
            actions: List of audit actions to apply

        Returns:
            Results of applying the actions
        """
        try:
            # Validate session exists
            session = self.dynamo_service.get_audit_session(session_id)
            if not session:
                raise ValueError("Audit session not found")

            # Convert audit actions to the format expected by SkoopinService
            actions_to_apply = []
            for action in actions:
                action_data = {
                    "scan_id": action.scan_id,
                    "action_type": action.action_type.value,
                    "new_value": action.new_value,
                }
                actions_to_apply.append(action_data)

            # Apply audit actions to Skoopin server
            logger.info(
                f"Applying {len(actions_to_apply)} audit actions for session {session_id}"
            )
            restaurant_id = session["restaurantId"]
            date = session["date"]
            auditor_id = session.get("auditorId")

            fix_results = self.skoopin_service.apply_audit_actions(
                actions_to_apply, restaurant_id
            )

            # Update audit session with results
            session_updates = {
                "status": "completed" if fix_results["success"] else "failed",
                "endTime": datetime.utcnow().isoformat(),
                "actionsCount": fix_results["applied_actions"],
            }

            self.dynamo_service.update_audit_session(session_id, session_updates)

            # Update scan audit status in DynamoDB for each action (successful or failed)
            successful_actions = 0
            for i, result in enumerate(fix_results["action_results"]):
                # Get the original action to extract scan details
                original_action = actions[i]

                # Prepare comprehensive audit data
                audit_data = {
                    "auditSessionId": session_id,
                    "auditorId": auditor_id,
                    "auditedAt": datetime.utcnow().isoformat(),
                    "isAudited": "true",
                }

                # Set audit status based on action type and result
                if result["success"]:
                    successful_actions += 1

                    if original_action.action_type.value == "delete":
                        audit_data.update(
                            {
                                "auditStatus": "deleted",
                                "auditAction": "deleted",
                                "auditResult": "success",
                                "originalValue": original_action.original_value,
                                "newValue": None,
                            }
                        )
                    elif original_action.action_type.value == "pan_change":
                        audit_data.update(
                            {
                                "auditStatus": "pan_updated",
                                "auditAction": "pan_change",
                                "auditResult": "success",
                                "auditorPanId": original_action.new_value,
                                "originalValue": original_action.original_value,
                                "newValue": original_action.new_value,
                            }
                        )
                    elif original_action.action_type.value == "menu_item_change":
                        audit_data.update(
                            {
                                "auditStatus": "menu_item_updated",
                                "auditAction": "menu_item_change",
                                "auditResult": "success",
                                "auditorMenuItemId": original_action.new_value,
                                "originalValue": original_action.original_value,
                                "newValue": original_action.new_value,
                            }
                        )
                    elif original_action.action_type.value == "venue_change":
                        audit_data.update(
                            {
                                "auditStatus": "venue_updated",
                                "auditAction": "venue_change",
                                "auditResult": "success",
                                "auditorVenueId": original_action.new_value,
                                "originalValue": original_action.original_value,
                                "newValue": original_action.new_value,
                            }
                        )
                    elif original_action.action_type.value == "meal_period_change":
                        audit_data.update(
                            {
                                "auditStatus": "meal_period_updated",
                                "auditAction": "meal_period_change",
                                "auditResult": "success",
                                "auditorMealPeriodId": original_action.new_value,
                                "originalValue": original_action.original_value,
                                "newValue": original_action.new_value,
                            }
                        )
                else:
                    # Action failed - still track the attempt
                    audit_data.update(
                        {
                            "auditStatus": "failed",
                            "auditAction": original_action.action_type.value,
                            "auditResult": "failed",
                            "auditError": result.get("error", "Unknown error"),
                            "originalValue": original_action.original_value,
                            "newValue": original_action.new_value,
                        }
                    )

                # Update scan audit status in DynamoDB
                self.dynamo_service.update_scan_audit_status(
                    restaurant_id=restaurant_id,
                    date=date,
                    scan_id=original_action.scan_id,
                    audit_data=audit_data,
                )

            return {
                "success": fix_results["success"],
                "session_id": session_id,
                "applied_actions": fix_results["applied_actions"],
                "failed_actions": fix_results["failed_actions"],
                "errors": fix_results["errors"],
                "timestamp": datetime.utcnow(),
            }

        except Exception as e:
            logger.error(f"Failed to apply audit actions for session {session_id}: {e}")
            raise

    def validate_audit_actions(self, actions: List[AuditAction]) -> Dict[str, Any]:
        """
        Validate audit actions before applying them

        Args:
            actions: List of audit actions to validate

        Returns:
            Validation results
        """
        validation_results = {"valid": True, "errors": [], "warnings": []}

        for i, action in enumerate(actions):
            # Check required fields
            if not action.scan_id:
                validation_results["errors"].append(
                    {"index": i, "error": "Missing scan_id"}
                )
                validation_results["valid"] = False

            # Check action type validity
            if action.action_type not in AuditActionType:
                validation_results["errors"].append(
                    {
                        "index": i,
                        "scan_id": action.scan_id,
                        "error": f"Invalid action type: {action.action_type}",
                    }
                )
                validation_results["valid"] = False

            # Check value requirements based on action type
            if action.action_type in [
                AuditActionType.PAN_CHANGE,
                AuditActionType.MENU_ITEM_CHANGE,
                AuditActionType.VENUE_CHANGE,
                AuditActionType.MEAL_PERIOD_CHANGE,
            ]:
                if not action.new_value:
                    validation_results["errors"].append(
                        {
                            "index": i,
                            "scan_id": action.scan_id,
                            "error": f"Missing new_value for action type {action.action_type}",
                        }
                    )
                    validation_results["valid"] = False

            # Check for duplicate scan actions
            scan_actions = [a for a in actions if a.scan_id == action.scan_id]
            if len(scan_actions) > 1:
                validation_results["warnings"].append(
                    {
                        "scan_id": action.scan_id,
                        "warning": f"Multiple actions for scan {action.scan_id}",
                    }
                )

        return validation_results

    def get_audit_summary(self, session_id: str) -> Dict[str, Any]:
        """
        Get a summary of audit session results

        Args:
            session_id: Session ID to summarize

        Returns:
            Audit summary
        """
        try:
            session = self.dynamo_service.get_audit_session(session_id)
            if not session:
                return {"error": "Session not found"}

            progress = self.dynamo_service.get_audit_progress(session_id)

            # Get restaurant name
            restaurant_id = session["restaurantId"]
            restaurants = self.skoopin_service.get_restaurants()
            restaurant_name = "Unknown"
            for restaurant in restaurants:
                if restaurant["id"] == restaurant_id:
                    restaurant_name = restaurant["name"]
                    break

            return {
                "session_id": session_id,
                "restaurant_id": restaurant_id,
                "restaurant_name": restaurant_name,
                "date": session["date"],
                "status": session["status"],
                "progress": progress,
                "start_time": session["startTime"],
                "end_time": session.get("endTime"),
                "total_scans": session["totalScans"],
                "audited_scans": session["auditedScans"],
                "actions_count": session["actionsCount"],
            }

        except Exception as e:
            logger.error(f"Failed to get audit summary for session {session_id}: {e}")
            return {"error": str(e)}

    def update_audit_progress(self, session_id: str, audited_scans: int) -> bool:
        """
        Update audit progress for a session

        Args:
            session_id: Session ID to update
            audited_scans: Number of scans audited so far

        Returns:
            True if successful, False otherwise
        """
        try:
            updates = {
                "auditedScans": audited_scans,
                "updatedAt": datetime.utcnow().isoformat(),
            }

            return self.dynamo_service.update_audit_session(session_id, updates)

        except Exception as e:
            logger.error(
                f"Failed to update audit progress for session {session_id}: {e}"
            )
            return False
