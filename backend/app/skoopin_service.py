import boto3
import logging
import requests
from typing import Any, Dict, List, Optional

from app.utils.config import get_config


class SkoopinService:
    def __init__(self):
        region = "us-west-2"
        config = get_config()
        app_config = config.get("skoopin_server")
        if not app_config:
            raise ValueError("skoopin server configuration is missing in config.yaml")

        self.refresh_token = app_config.get("refresh_token")
        self.serverAddress = app_config.get("server_address")
        self.client_id = app_config.get("client_id")
        self.max_cache_size = app_config.get("max_cache_size", 1000)

        if not all([self.refresh_token, self.serverAddress, self.client_id]):
            raise ValueError("Incomplete app configuration in the config file")

        self.region = region
        self.client = boto3.client("cognito-idp", region_name=self.region)
        self.logger = logging.getLogger(__name__)
        # Simple in-process circuit breaker to avoid cascading stalls
        self._cb_fail_count = 0
        self._cb_breaker_until = 0.0
        self._CB_THRESHOLD = 5
        self._CB_COOLDOWN_SECONDS = 60

    def _circuit_open(self) -> bool:
        import time as _t

        return _t.time() < self._cb_breaker_until

    def _record_failure(self):
        import time as _t

        self._cb_fail_count += 1
        if self._cb_fail_count >= self._CB_THRESHOLD:
            self._cb_breaker_until = _t.time() + self._CB_COOLDOWN_SECONDS
            try:
                self.logger.warning(
                    "Circuit breaker opened for SkoopinService external calls"
                )
            except Exception:
                pass

    def _record_success(self):
        self._cb_fail_count = 0
        self._cb_breaker_until = 0.0

    def refresh_access_token(self):
        try:
            resp = self.client.initiate_auth(
                AuthParameters={
                    "REFRESH_TOKEN": self.refresh_token,
                },
                ClientId=self.client_id,
                AuthFlow="REFRESH_TOKEN_AUTH",
            )
            res = resp.get("AuthenticationResult")
            access_token = res["AccessToken"]
            return access_token
        except Exception as e:
            self.logger.error(f"Error refreshing access token: {e}")
            raise

    def get_venues(self, restaurant_id):
        if self._circuit_open():
            self.logger.warning("Circuit open: get_venues short-circuiting")
            return {}
        access_token = self.refresh_access_token()
        url = f"{self.serverAddress}/venues?RestaurantID={restaurant_id}"
        authorization_val = "Bearer " + str(access_token)
        try:
            response = requests.get(
                url,
                headers={"Authorization": authorization_val, "X-Device-Type": "MiniPC"},
                timeout=(3, 10),
            )
            response.raise_for_status()
            j = response.json()
            data = j.get("data", [])
            self._record_success()
            venue_dict = {entry["ID"]: entry["Name"] for entry in data}
            return venue_dict
        except Exception as e:
            self.logger.error(f"Error in Venue: {e}")
            self._record_failure()
            return {}

    def get_pans(self, resturaunt_id):
        if self._circuit_open():
            self.logger.warning("Circuit open: get_pans short-circuiting")
            return []
        access_token = self.refresh_access_token()
        url = self.serverAddress + f"/pans/?RestaurantID={resturaunt_id}"
        authorization_val = "Bearer " + str(access_token)
        try:
            response = requests.get(
                url,
                headers={"Authorization": authorization_val, "X-Device-Type": "MiniPC"},
                timeout=(3, 15),
            )
            response.raise_for_status()
            j = response.json()
            body = j.get("data", [])
            self._record_success()
            return body
        except Exception as e:
            self.logger.error(f"Error getting pans: {e}")
            self._record_failure()
            return []

    def get_pan_onboard_scans(self, restaurantId):
        if self._circuit_open():
            self.logger.warning("Circuit open: get_pan_onboard_scans short-circuiting")
            return []
        access_token = self.refresh_access_token()
        url = f"{self.serverAddress}/scans"
        try:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            params = {
                "RestaurantID": restaurantId,
                "Type": 6,
            }
            resp = requests.get(url, headers=headers, params=params, timeout=(3, 20))
            resp.raise_for_status()
            j = resp.json()
            data = j.get("data", [])
            self._record_success()
            return data
        except Exception as e:
            self.logger.error(f"Error getting pan onboard scans: {e}")
            self._record_failure()
            return []

    def get_restaurants(self):
        if self._circuit_open():
            self.logger.warning("Circuit open: get_restaurants short-circuiting")
            return []
        access_token = self.refresh_access_token()
        url = f"{self.serverAddress}/restaurants"
        authorization_val = "Bearer " + str(access_token)
        try:
            response = requests.get(
                url,
                headers={"Authorization": authorization_val, "X-Device-Type": "MiniPC"},
                timeout=(3, 10),
            )
            response.raise_for_status()
            data = response.json().get("data", [])
            self._record_success()
            restaurant_list = []
            for restaurant in data:
                restaurant_id = restaurant.get("ID")
                restaurnt_name = restaurant.get("Name")
                if restaurant_id:
                    restaurant_list.append(
                        {"id": restaurant_id, "name": restaurnt_name}
                    )
            return restaurant_list
        except Exception as e:
            self.logger.error(f"Error getting restaurants: {e}")
            self._record_failure()
            return []

    def get_scanned_images(self, RestaurantID, menu_item, StartDate, EndDate):
        if self._circuit_open():
            self.logger.warning("Circuit open: get_scanned_images short-circuiting")
            return []
        url = f"{self.serverAddress}/scans"
        access_token = self.refresh_access_token()
        try:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            params = {
                "RestaurantID": RestaurantID,
                "StartDate": StartDate,
                "EndDate": EndDate,
            }
            if menu_item:
                params["MenuItemID"] = menu_item
            resp = requests.get(url, headers=headers, params=params, timeout=(3, 20))
            resp.raise_for_status()
            data = resp.json().get("data", [])
            self._record_success()
            filtered_data = {}
            for item in data:
                if item["MenuItemName"] is not None and item["Type"] == 1:
                    venue_id = item["VenueID"]
                    if venue_id not in filtered_data:
                        filtered_data[venue_id] = []

                    filtered_data[venue_id].append(
                        {
                            "MenuItemID": item["MenuItemID"],
                            "MenuItemName": item["MenuItemName"],
                            "StationID": item["StationID"],
                            "ImageURL": item["ImageURL"],
                            "DepthImageURL": item["DepthImageURL"],
                        }
                    )

            return filtered_data
        except Exception as e:
            self.logger.error(f"Error getting scanned images: {e}")
            return []

    # ========== AUDIT FIXING METHODS ==========

    def delete_scan(self, scan_id: str) -> Dict[str, Any]:
        """
        Delete a scan record from Skoopin server (soft delete)

        Args:
            scan_id: The scan ID to delete

        Returns:
            Dict with success status and response data
        """
        try:
            access_token = self.refresh_access_token()
            url = f"{self.serverAddress}/scans/{scan_id}"
            headers = {"Authorization": f"Bearer {access_token}"}

            # Add detailed logging
            print(f"🔍 Deleting scan {scan_id}")
            print(f"🔍 URL: {url}")
            self.logger.info(f"Deleting scan {scan_id}")
            self.logger.info(f"URL: {url}")

            response = requests.delete(url, headers=headers, timeout=15)

            print(f"🔍 Response status: {response.status_code}")
            print(f"🔍 Response text: {response.text}")
            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(f"Response text: {response.text}")

            if response.status_code == 200:
                self.logger.info(f"Successfully deleted scan {scan_id}")
                return {
                    "success": True,
                    "scan_id": scan_id,
                    "response": response.json(),
                }
            else:
                self.logger.error(
                    f"Failed to delete scan {scan_id}: {response.status_code}"
                )
                return {
                    "success": False,
                    "scan_id": scan_id,
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            self.logger.error(f"Error deleting scan {scan_id}: {e}")
            return {"success": False, "scan_id": scan_id, "error": str(e)}

    def update_scan_pan(self, scan_id: str, pan_id: Optional[str]) -> Dict[str, Any]:
        """
        Update the pan ID for a scan

        Args:
            scan_id: The scan ID to update
            pan_id: New pan ID (None to clear)

        Returns:
            Dict with success status and response data
        """
        try:
            access_token = self.refresh_access_token()
            url = f"{self.serverAddress}/scans/{scan_id}"
            headers = {"Authorization": f"Bearer {access_token}"}

            payload = {"PanID": pan_id}

            # Add detailed logging
            print(f"🔍 Updating pan for scan {scan_id} to {pan_id}")
            print(f"🔍 URL: {url}")
            print(f"🔍 Payload: {payload}")
            self.logger.info(f"Updating pan for scan {scan_id} to {pan_id}")
            self.logger.info(f"URL: {url}")
            self.logger.info(f"Payload: {payload}")

            response = requests.patch(url, json=payload, headers=headers, timeout=15)

            print(f"🔍 Response status: {response.status_code}")
            print(f"🔍 Response text: {response.text}")
            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(f"Response text: {response.text}")

            if response.status_code == 200:
                self.logger.info(
                    f"Successfully updated pan for scan {scan_id} to {pan_id}"
                )
                return {
                    "success": True,
                    "scan_id": scan_id,
                    "field": "PanID",
                    "new_value": pan_id,
                    "response": response.json(),
                }
            else:
                self.logger.error(
                    f"Failed to update pan for scan {scan_id}: {response.status_code}"
                )
                self.logger.error(f"Response text: {response.text}")
                return {
                    "success": False,
                    "scan_id": scan_id,
                    "field": "PanID",
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            self.logger.error(f"Error updating pan for scan {scan_id}: {e}")
            return {
                "success": False,
                "scan_id": scan_id,
                "field": "PanID",
                "error": str(e),
            }

    def update_scan_menu_item(self, scan_id: str, menu_item_id: str) -> Dict[str, Any]:
        """
        Update the menu item for a scan

        Args:
            scan_id: The scan ID to update
            menu_item_id: New menu item ID

        Returns:
            Dict with success status and response data
        """
        try:
            access_token = self.refresh_access_token()

            # First get the menu item details
            menu_item = self.get_menu_item(access_token, menu_item_id)
            if not menu_item:
                return {
                    "success": False,
                    "scan_id": scan_id,
                    "field": "MenuItemID",
                    "error": f"Menu item {menu_item_id} not found",
                }

            # Update the scan with menu item details
            url = f"{self.serverAddress}/scans/{scan_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            payload = {"MenuItemID": menu_item["ID"], "MenuItemName": menu_item["Name"]}
            response = requests.patch(url, json=payload, headers=headers, timeout=15)

            if response.status_code == 200:
                self.logger.info(
                    f"Successfully updated menu item for scan {scan_id} to {menu_item['Name']}"
                )
                return {
                    "success": True,
                    "scan_id": scan_id,
                    "field": "MenuItemID",
                    "new_value": menu_item_id,
                    "menu_item_name": menu_item["Name"],
                    "response": response.json(),
                }
            else:
                self.logger.error(
                    f"Failed to update menu item for scan {scan_id}: {response.status_code}"
                )
                return {
                    "success": False,
                    "scan_id": scan_id,
                    "field": "MenuItemID",
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            self.logger.error(f"Error updating menu item for scan {scan_id}: {e}")
            return {
                "success": False,
                "scan_id": scan_id,
                "field": "MenuItemID",
                "error": str(e),
            }

    def update_scan_venue(self, scan_id: str, venue_id: str) -> Dict[str, Any]:
        """
        Update the venue for a scan

        Args:
            scan_id: The scan ID to update
            venue_id: New venue ID

        Returns:
            Dict with success status and response data
        """
        try:
            access_token = self.refresh_access_token()
            url = f"{self.serverAddress}/scans/{scan_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            payload = {"VenueID": venue_id}
            response = requests.patch(url, json=payload, headers=headers, timeout=15)

            if response.status_code == 200:
                self.logger.info(
                    f"Successfully updated venue for scan {scan_id} to {venue_id}"
                )
                return {
                    "success": True,
                    "scan_id": scan_id,
                    "field": "VenueID",
                    "new_value": venue_id,
                    "response": response.json(),
                }
            else:
                self.logger.error(
                    f"Failed to update venue for scan {scan_id}: {response.status_code}"
                )
                return {
                    "success": False,
                    "scan_id": scan_id,
                    "field": "VenueID",
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            self.logger.error(f"Error updating venue for scan {scan_id}: {e}")
            return {
                "success": False,
                "scan_id": scan_id,
                "field": "VenueID",
                "error": str(e),
            }

    def update_scan_meal_period(
        self, scan_id: str, meal_period_id: str
    ) -> Dict[str, Any]:
        """
        Update the meal period for a scan

        Args:
            scan_id: The scan ID to update
            meal_period_id: New meal period ID

        Returns:
            Dict with success status and response data
        """
        try:
            access_token = self.refresh_access_token()
            url = f"{self.serverAddress}/scans/{scan_id}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }

            payload = {"ServicePeriodID": meal_period_id}
            response = requests.patch(url, json=payload, headers=headers, timeout=15)

            if response.status_code == 200:
                self.logger.info(
                    f"Successfully updated meal period for scan {scan_id} to {meal_period_id}"
                )
                return {
                    "success": True,
                    "scan_id": scan_id,
                    "field": "ServicePeriodID",
                    "new_value": meal_period_id,
                    "response": response.json(),
                }
            else:
                self.logger.error(
                    f"Failed to update meal period for scan {scan_id}: {response.status_code}"
                )
                return {
                    "success": False,
                    "scan_id": scan_id,
                    "field": "ServicePeriodID",
                    "error": f"HTTP {response.status_code}: {response.text}",
                }

        except Exception as e:
            self.logger.error(f"Error updating meal period for scan {scan_id}: {e}")
            return {
                "success": False,
                "scan_id": scan_id,
                "field": "ServicePeriodID",
                "error": str(e),
            }

    def get_menu_item(
        self, access_token: str, menu_item_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get menu item details by ID

        Args:
            access_token: Valid access token
            menu_item_id: Menu item ID to retrieve

        Returns:
            Menu item data or None if not found
        """
        try:
            url = f"{self.serverAddress}/menuitems/{menu_item_id}"
            headers = {"Authorization": f"Bearer {access_token}"}

            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                return response.json().get("data", {})
            else:
                self.logger.error(
                    f"Failed to get menu item {menu_item_id}: {response.status_code}"
                )
                return None

        except Exception as e:
            self.logger.error(f"Error getting menu item {menu_item_id}: {e}")
            return None

    def get_scan_by_short_id(
        self, short_id: str, restaurant_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Get scan details by short ID (e.g., S0495878)

        Args:
            short_id: Short scan ID (e.g., S0495878)
            restaurant_id: Restaurant ID

        Returns:
            Scan data or None if not found
        """
        try:
            access_token = self.refresh_access_token()
            url = f"{self.serverAddress}/scans"
            headers = {"Authorization": f"Bearer {access_token}"}

            # Query parameters to find scan by short ID and restaurant
            params = {"RestaurantID": restaurant_id, "current": 1, "pageSize": 2000}

            self.logger.info(
                f"Looking up scan with short ID {short_id} for restaurant {restaurant_id}"
            )
            self.logger.info(f"URL: {url}")
            self.logger.info(f"Params: {params}")

            response = requests.get(url, headers=headers, params=params, timeout=20)

            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(
                f"Response text: {response.text[:500]}..."
            )  # First 500 chars

            if response.status_code == 200:
                data = response.json()
                scans = data.get("data", [])
                self.logger.info(
                    f"Found {len(scans)} scans for restaurant {restaurant_id}"
                )
                self.logger.info(f"Looking for scan with ShortID: {short_id}")

                # Find scan by ShortID (check both "ShortID" and "Short ID" fields)
                for scan in scans:
                    scan_short_id = scan.get("ShortID") or scan.get("Short ID")
                    if scan_short_id == short_id:
                        self.logger.info(
                            f"Found matching scan: ID={scan.get('ID')}, ShortID={scan_short_id}, Status={scan.get('Status')}"
                        )
                        return scan

                self.logger.warning(
                    f"No scan found with short ID {short_id} for restaurant {restaurant_id}"
                )
                return None
            else:
                self.logger.error(
                    f"Failed to get scan {short_id}: {response.status_code}"
                )
                return None

        except Exception as e:
            self.logger.error(f"Error getting scan {short_id}: {e}")
            return None

    def apply_audit_actions(
        self, actions: List[Dict[str, Any]], restaurant_id: int = None
    ) -> Dict[str, Any]:
        """
        Apply multiple audit actions in batch

        Args:
            actions: List of audit actions to apply
            restaurant_id: Restaurant ID (needed to resolve short scan IDs)

        Returns:
            Dict with results for all actions
        """
        print(
            f"🔍 Starting to apply {len(actions)} audit actions for restaurant {restaurant_id}"
        )
        self.logger.info(
            f"Starting to apply {len(actions)} audit actions for restaurant {restaurant_id}"
        )

        results = {
            "success": True,
            "applied_actions": 0,
            "failed_actions": 0,
            "errors": [],
            "action_results": [],
        }

        for i, action in enumerate(actions):
            scan_id = action.get("scan_id")
            action_type = action.get("action_type")
            new_value = action.get("new_value")

            print(
                f"🔍 Processing action {i+1}: {action_type} for scan {scan_id} with value {new_value}"
            )
            self.logger.info(
                f"Processing action {i+1}: {action_type} for scan {scan_id} with value {new_value}"
            )

            if not scan_id or not action_type:
                results["errors"].append(
                    {"action": action, "error": "Missing scan_id or action_type"}
                )
                results["failed_actions"] += 1
                continue

            # Resolve short scan ID to full scan ID if needed
            scan_details = None
            full_scan_id = scan_id
            if scan_id.startswith("S") and restaurant_id:
                print(f"🔍 Resolving short scan ID {scan_id} to full scan ID")
                self.logger.info(f"Resolving short scan ID {scan_id} to full scan ID")
                scan_details = self.get_scan_by_short_id(scan_id, restaurant_id)
                if scan_details:
                    full_scan_id = scan_details.get("ID")
                    print(f"🔍 Resolved {scan_id} to {full_scan_id}")
                    self.logger.info(f"Resolved {scan_id} to {full_scan_id}")
                    if not full_scan_id:
                        results["errors"].append(
                            {
                                "action": action,
                                "error": f"Could not resolve full scan ID for {scan_id}",
                            }
                        )
                        results["failed_actions"] += 1
                        continue
                else:
                    # If scan not found in Skoopin and action is delete, treat as successful
                    if action_type == "delete":
                        print(
                            f"🔍 Scan {scan_id} not found in Skoopin, treating delete as successful"
                        )
                        self.logger.info(
                            f"Scan {scan_id} not found in Skoopin, treating delete as successful"
                        )
                        results["action_results"].append(
                            {
                                "success": True,
                                "scan_id": scan_id,
                                "action_type": "delete",
                                "message": "Scan not found in Skoopin (already deleted or doesn't exist)",
                            }
                        )
                        results["applied_actions"] += 1
                        continue
                    else:
                        results["errors"].append(
                            {
                                "action": action,
                                "error": f"Could not find scan with short ID {scan_id}",
                            }
                        )
                        results["failed_actions"] += 1
                        continue
            else:
                print(f"🔍 Using scan ID as-is: {scan_id}")
                self.logger.info(f"Using scan ID as-is: {scan_id}")

            # Check scan status before applying actions (like reference script)
            if scan_details is not None and scan_details.get("Status") != 1:
                error_msg = f"Scan {scan_id} has status {scan_details.get('Status')}, cannot apply {action_type}"
                print(f"🔍 {error_msg}")
                self.logger.warning(error_msg)
                results["errors"].append({"action": action, "error": error_msg})
                results["failed_actions"] += 1
                continue

            # Apply the specific action using full scan ID
            print(f"🔍 Applying {action_type} to scan {full_scan_id}")
            self.logger.info(f"Applying {action_type} to scan {full_scan_id}")

            if action_type == "delete":
                result = self.delete_scan(full_scan_id)
            elif action_type == "pan_change" or action_type == "updatePan":
                print(
                    f"🔍 Calling update_scan_pan with scan_id={full_scan_id}, pan_id={new_value}"
                )
                result = self.update_scan_pan(full_scan_id, new_value)
            elif action_type == "menu_item_change":
                result = self.update_scan_menu_item(full_scan_id, new_value)
            elif action_type == "venue_change":
                result = self.update_scan_venue(full_scan_id, new_value)
            elif action_type == "meal_period_change":
                result = self.update_scan_meal_period(full_scan_id, new_value)
            else:
                print(f"🔍 Unknown action type: {action_type}")
                result = {
                    "success": False,
                    "scan_id": full_scan_id,
                    "error": f"Unknown action type: {action_type}",
                }

            print(f"🔍 Action result: {result}")
            self.logger.info(f"Action result: {result}")
            results["action_results"].append(result)

            if result["success"]:
                results["applied_actions"] += 1
            else:
                results["failed_actions"] += 1
                results["errors"].append(result)

        # Overall success if at least one action was applied
        results["success"] = results["applied_actions"] > 0
        self.logger.info(
            f"Audit actions completed. Success: {results['success']}, Applied: {results['applied_actions']}, Failed: {results['failed_actions']}"
        )

        return results
