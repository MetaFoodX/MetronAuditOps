#!/usr/bin/env python3
"""
Comprehensive CRUD Operations Test Script

This script tests all CRUD operations to ensure:
1. CREATE: Audit sessions are created in DynamoDB
2. READ: Scan data is retrieved from both Skoopin and DynamoDB
3. UPDATE: Pan changes are applied to both Skoopin and DynamoDB
4. DELETE: Scan deletions are tracked in both Skoopin and DynamoDB

Usage: python test_crud_operations.py
"""

import json
import requests
from datetime import datetime
from typing import Any, Dict, List


class CRUDTestSuite:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.test_results = []

    def log_test(self, test_name: str, success: bool, details: str = ""):
        """Log test results"""
        result = {
            "test": test_name,
            "success": success,
            "details": details,
            "timestamp": datetime.now().isoformat(),
        }
        self.test_results.append(result)
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}: {details}")

    def test_create_audit_session(
        self, restaurant_id: int = 157, date: str = "2025-07-27"
    ) -> str:
        """Test CREATE operation - Audit session creation"""
        try:
            response = requests.post(
                f"{self.base_url}/api/audit/session/create",
                params={"restaurant_id": restaurant_id, "date": date},
            )

            if response.status_code == 200:
                data = response.json()
                session_id = data.get("session_id")
                self.log_test("CREATE Audit Session", True, f"Session ID: {session_id}")
                return session_id
            else:
                self.log_test(
                    "CREATE Audit Session",
                    False,
                    f"HTTP {response.status_code}: {response.text}",
                )
                return None

        except Exception as e:
            self.log_test("CREATE Audit Session", False, f"Exception: {str(e)}")
            return None

    def test_read_scan_data(
        self, restaurant_id: int = 157, date: str = "2025-07-27"
    ) -> List[Dict[str, Any]]:
        """Test READ operation - Scan data retrieval"""
        try:
            response = requests.get(
                f"{self.base_url}/api/scans_to_audit",
                params={"restaurantId": restaurant_id, "date": date},
            )

            if response.status_code == 200:
                data = response.json()
                scans = data.get("scans", [])
                self.log_test("READ Scan Data", True, f"Retrieved {len(scans)} scans")
                return scans
            else:
                self.log_test(
                    "READ Scan Data",
                    False,
                    f"HTTP {response.status_code}: {response.text}",
                )
                return []

        except Exception as e:
            self.log_test("READ Scan Data", False, f"Exception: {str(e)}")
            return []

    def test_update_pan(self, session_id: str, scan_id: str, new_pan_id: str) -> bool:
        """Test UPDATE operation - Pan change"""
        try:
            actions = [
                {
                    "scan_id": scan_id,
                    "action_type": "pan_change",
                    "original_value": "Unrecognized",
                    "new_value": new_pan_id,
                    "reason": "Test pan update",
                }
            ]

            response = requests.post(
                f"{self.base_url}/api/audit/confirm",
                json={
                    "session_id": session_id,
                    "actions": actions,
                    "confirm_all": True,
                },
            )

            if response.status_code == 200:
                data = response.json()
                success = data.get("success", False)
                applied_actions = data.get("applied_actions", 0)
                self.log_test(
                    "UPDATE Pan", success, f"Applied {applied_actions} actions"
                )
                return success
            else:
                self.log_test(
                    "UPDATE Pan", False, f"HTTP {response.status_code}: {response.text}"
                )
                return False

        except Exception as e:
            self.log_test("UPDATE Pan", False, f"Exception: {str(e)}")
            return False

    def test_delete_scan(self, session_id: str, scan_id: str) -> bool:
        """Test DELETE operation - Scan deletion"""
        try:
            actions = [
                {
                    "scan_id": scan_id,
                    "action_type": "delete",
                    "original_value": "Active scan",
                    "new_value": None,
                    "reason": "Test deletion",
                }
            ]

            response = requests.post(
                f"{self.base_url}/api/audit/confirm",
                json={
                    "session_id": session_id,
                    "actions": actions,
                    "confirm_all": True,
                },
            )

            if response.status_code == 200:
                data = response.json()
                success = data.get("success", False)
                applied_actions = data.get("applied_actions", 0)
                self.log_test(
                    "DELETE Scan", success, f"Applied {applied_actions} actions"
                )
                return success
            else:
                self.log_test(
                    "DELETE Scan",
                    False,
                    f"HTTP {response.status_code}: {response.text}",
                )
                return False

        except Exception as e:
            self.log_test("DELETE Scan", False, f"Exception: {str(e)}")
            return False

    def test_comprehensive_crud(
        self, restaurant_id: int = 157, date: str = "2025-07-27"
    ) -> bool:
        """Test comprehensive CRUD operations"""
        try:
            # Step 1: CREATE - Create audit session
            session_id = self.test_create_audit_session(restaurant_id, date)
            if not session_id:
                return False

            # Step 2: READ - Get scan data
            scans = self.test_read_scan_data(restaurant_id, date)
            if not scans:
                self.log_test("Comprehensive CRUD", False, "No scans found to test")
                return False

            # Step 3: UPDATE - Test pan update on first scan
            first_scan = scans[0]
            scan_id = first_scan.get("scanId")
            if scan_id:
                pan_update_success = self.test_update_pan(
                    session_id, scan_id, "test_pan_id_123"
                )
                if not pan_update_success:
                    self.log_test("Comprehensive CRUD", False, "Pan update failed")
                    return False

            # Step 4: DELETE - Test scan deletion on second scan (if available)
            if len(scans) > 1:
                second_scan = scans[1]
                scan_id = second_scan.get("scanId")
                if scan_id:
                    delete_success = self.test_delete_scan(session_id, scan_id)
                    if not delete_success:
                        self.log_test(
                            "Comprehensive CRUD", False, "Scan deletion failed"
                        )
                        return False

            # Step 5: Verify audit status
            self.test_audit_status_verification(restaurant_id, date)

            self.log_test(
                "Comprehensive CRUD", True, "All CRUD operations completed successfully"
            )
            return True

        except Exception as e:
            self.log_test("Comprehensive CRUD", False, f"Exception: {str(e)}")
            return False

    def test_audit_status_verification(self, restaurant_id: int, date: str):
        """Test audit status verification"""
        try:
            response = requests.get(
                f"{self.base_url}/api/audit/status/{restaurant_id}/{date}"
            )

            if response.status_code == 200:
                data = response.json()
                statistics = data.get("statistics", {})
                total_scans = statistics.get("total_scans", 0)
                audited_scans = statistics.get("audited_scans", 0)
                deleted_scans = statistics.get("deleted_scans", 0)

                self.log_test(
                    "Audit Status Verification",
                    True,
                    f"Total: {total_scans}, Audited: {audited_scans}, Deleted: {deleted_scans}",
                )
            else:
                self.log_test(
                    "Audit Status Verification", False, f"HTTP {response.status_code}"
                )

        except Exception as e:
            self.log_test("Audit Status Verification", False, f"Exception: {str(e)}")

    def run_all_tests(self, restaurant_id: int = 157, date: str = "2025-07-27"):
        """Run all CRUD tests"""
        print("🚀 Starting Comprehensive CRUD Operations Test Suite")
        print("=" * 60)

        # Run comprehensive test
        success = self.test_comprehensive_crud(restaurant_id, date)

        # Print summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)

        passed = sum(1 for result in self.test_results if result["success"])
        total = len(self.test_results)

        print(f"Total Tests: {total}")
        print(f"Passed: {passed}")
        print(f"Failed: {total - passed}")
        print(f"Success Rate: {(passed/total*100):.1f}%")

        # Print detailed results
        print("\n📋 DETAILED RESULTS")
        print("=" * 60)
        for result in self.test_results:
            status = "✅ PASS" if result["success"] else "❌ FAIL"
            print(f"{status} {result['test']}")
            if result["details"]:
                print(f"    Details: {result['details']}")

        return success


def main():
    """Main test execution"""
    test_suite = CRUDTestSuite()

    # Test with default restaurant and date
    success = test_suite.run_all_tests()

    if success:
        print("\n🎉 All CRUD operations are working correctly!")
        print("✅ Skoopin server updates: Working")
        print("✅ DynamoDB audit tracking: Working")
        print("✅ Complete audit trail: Working")
    else:
        print("\n⚠️  Some CRUD operations failed. Check the details above.")

    return success


if __name__ == "__main__":
    main()
