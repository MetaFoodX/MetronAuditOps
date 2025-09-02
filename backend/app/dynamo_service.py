import logging
import uuid
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.utils.config import get_config
from app.utils.dynamo_client import get_dynamodb


class DynamoDBService:
    def __init__(self):
        self.db = get_dynamodb()
        self.dynamo_config = get_config().get("dynamodb")
        self.scan_audit_table = self.db.Table(
            self.dynamo_config.get("table_names")["scan_audit"]
        )
        self.audit_session_table = self.db.Table(
            self.dynamo_config.get("table_names")["audit_session"]
        )
        self.users_table = self.db.Table(self.dynamo_config.get("table_names")["users"])
        self.logger = logging.getLogger(__name__)

    def test_connection(self, table_name):
        try:
            table_attr_name = f"{table_name}_table"
            table = getattr(self, table_attr_name)
            return f"Table '{table.name}' is {self.audit_session_table.table_status}"
        except Exception as e:
            return f"Connection failed: {str(e)}"

    def get_scans_by_restaurant_day(self, restaurantID, date):
        partition_key = f"{restaurantID}#{date}"
        try:
            response = self.scan_audit_table.query(
                KeyConditionExpression=Key("RestaurantDate").eq(partition_key)
            )

            return response.get("Items", [])
        except Exception as e:
            return f"Failed to query records: {str(e)}"

    def get_all_users(self):
        try:
            response = self.users_table.scan()

            return response.get("Items", [])
        except Exception as e:
            return f"Failed to scan users table: {str(e)}"

    # ========== AUDIT SESSION MANAGEMENT ==========

    def create_audit_session(
        self,
        restaurant_id: int,
        date: str,
        total_scans: int,
        auditor_id: Optional[str] = None,
    ) -> str:
        """
        Create a new audit session

        Args:
            restaurant_id: Restaurant ID being audited
            date: Date being audited (YYYY-MM-DD)
            total_scans: Total number of scans in the session
            auditor_id: Optional auditor ID

        Returns:
            Session ID of the created session
        """
        try:
            session_id = str(uuid.uuid4())
            current_time = datetime.utcnow().isoformat()

            session_data = {
                "auditReportId": session_id,  # Primary key
                "tsEventType": "audit_session",  # Sort key
                "sessionId": session_id,
                "restaurantId": restaurant_id,
                "date": date,
                "auditorId": auditor_id,
                "startTime": current_time,
                "endTime": None,
                "status": "in_progress",
                "totalScans": total_scans,
                "auditedScans": 0,
                "actionsCount": 0,
                "createdAt": current_time,
                "updatedAt": current_time,
            }

            self.audit_session_table.put_item(Item=session_data)
            self.logger.info(
                f"Created audit session {session_id} for restaurant {restaurant_id} on {date}"
            )

            return session_id

        except Exception as e:
            self.logger.error(f"Failed to create audit session: {e}")
            raise

    def get_audit_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get audit session by ID

        Args:
            session_id: Session ID to retrieve

        Returns:
            Session data or None if not found
        """
        try:
            response = self.audit_session_table.get_item(
                Key={"auditReportId": session_id, "tsEventType": "audit_session"}
            )

            return response.get("Item")

        except Exception as e:
            self.logger.error(f"Failed to get audit session {session_id}: {e}")
            return None

    def update_audit_session(self, session_id: str, updates: Dict[str, Any]) -> bool:
        """
        Update audit session with new data

        Args:
            session_id: Session ID to update
            updates: Dictionary of fields to update

        Returns:
            True if successful, False otherwise
        """
        try:
            # Add updated timestamp
            updates["updatedAt"] = datetime.utcnow().isoformat()

            # Build update expression
            update_expression = "SET "
            expression_values = {}
            expression_names = {}

            for key, value in updates.items():
                attr_name = f"#{key}"
                attr_value = f":{key}"

                update_expression += f"{attr_name} = {attr_value}, "
                expression_names[attr_name] = key
                expression_values[attr_value] = value

            # Remove trailing comma and space
            update_expression = update_expression.rstrip(", ")

            self.audit_session_table.update_item(
                Key={"auditReportId": session_id, "tsEventType": "audit_session"},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=expression_values,
            )

            self.logger.info(f"Updated audit session {session_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update audit session {session_id}: {e}")
            return False

    def complete_audit_session(self, session_id: str, final_actions_count: int) -> bool:
        """
        Mark audit session as completed

        Args:
            session_id: Session ID to complete
            final_actions_count: Total number of actions taken

        Returns:
            True if successful, False otherwise
        """
        try:
            updates = {
                "status": "completed",
                "endTime": datetime.utcnow().isoformat(),
                "actionsCount": final_actions_count,
            }

            return self.update_audit_session(session_id, updates)

        except Exception as e:
            self.logger.error(f"Failed to complete audit session {session_id}: {e}")
            return False

    def get_audit_sessions_by_restaurant(
        self, restaurant_id: int, limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get audit sessions for a specific restaurant

        Args:
            restaurant_id: Restaurant ID
            limit: Maximum number of sessions to return

        Returns:
            List of audit sessions
        """
        try:
            # For now, scan the table and filter by restaurant_id
            # In production, you should create a GSI on restaurantId
            response = self.audit_session_table.scan(
                FilterExpression=Attr("restaurantId").eq(restaurant_id), Limit=limit
            )

            return response.get("Items", [])

        except Exception as e:
            self.logger.error(
                f"Failed to get audit sessions for restaurant {restaurant_id}: {e}"
            )
            return []

    def get_active_sessions_for_date(
        self, restaurant_id: int, date: str
    ) -> List[Dict[str, Any]]:
        """
        Return in-progress sessions for a restaurant on a given date.
        Note: Uses a scan; consider adding a GSI for production scale.
        """
        try:
            response = self.audit_session_table.scan(
                FilterExpression=(
                    Attr("restaurantId").eq(restaurant_id)
                    & Attr("date").eq(date)
                    & Attr("status").eq("in_progress")
                )
            )
            return response.get("Items", [])
        except Exception as e:
            self.logger.error(
                f"Failed to get active sessions for restaurant {restaurant_id} date {date}: {e}"
            )
            return []

    def update_scan_audit_status(
        self, restaurant_id: int, date: str, scan_id: str, audit_data: Dict[str, Any]
    ) -> bool:
        """
        Update scan audit status in DynamoDB

        Args:
            restaurant_id: Restaurant ID
            date: Date of the scan
            scan_id: Scan ID
            audit_data: Audit data to update

        Returns:
            True if successful, False otherwise
        """
        try:
            partition_key = f"{restaurant_id}#{date}"
            current_time = datetime.utcnow().isoformat()

            # Add audit tracking fields
            audit_data.update(
                {
                    "isAudited": "true",
                    "auditedAt": current_time,
                    "updatedAt": current_time,
                }
            )

            # Build update expression
            update_expression = "SET "
            expression_values = {}
            expression_names = {}

            for key, value in audit_data.items():
                attr_name = f"#{key}"
                attr_value = f":{key}"

                update_expression += f"{attr_name} = {attr_value}, "
                expression_names[attr_name] = key
                expression_values[attr_value] = value

            # Remove trailing comma and space
            update_expression = update_expression.rstrip(", ")

            self.scan_audit_table.update_item(
                Key={"RestaurantDate": partition_key, "scanId": scan_id},
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_names,
                ExpressionAttributeValues=expression_values,
            )

            self.logger.info(f"Updated audit status for scan {scan_id}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update scan audit status for {scan_id}: {e}")
            return False

    def get_audit_progress(self, session_id: str) -> Dict[str, Any]:
        """
        Get audit progress for a session

        Args:
            session_id: Session ID

        Returns:
            Progress data including counts and percentages
        """
        try:
            session = self.get_audit_session(session_id)
            if not session:
                return {"error": "Session not found"}

            total_scans = session.get("totalScans", 0)
            audited_scans = session.get("auditedScans", 0)
            actions_count = session.get("actionsCount", 0)

            progress_percentage = (
                (audited_scans / total_scans * 100) if total_scans > 0 else 0
            )

            return {
                "session_id": session_id,
                "total_scans": total_scans,
                "audited_scans": audited_scans,
                "actions_count": actions_count,
                "progress_percentage": round(progress_percentage, 2),
                "status": session.get("status", "unknown"),
            }

        except Exception as e:
            self.logger.error(
                f"Failed to get audit progress for session {session_id}: {e}"
            )
            return {"error": str(e)}
