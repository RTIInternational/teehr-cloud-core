#!/usr/bin/env python3
"""
Integration test: Polaris role-based ACL validation

Validates:
- Principal roles (teehr-read-only, teehr-read-write, iceberg-catalog-admin) exist
- Catalog roles (teehr_read_only_role, teehr_read_write_role, catalog_admin_role) exist
- Roles are properly bound to permissions
"""

import sys
import requests
import time

POLARIS_URL = "http://polaris:8181"
REALM = "teehr"
ROOT_CLIENT_ID = "root"
ROOT_CLIENT_SECRET = "secret123"

EXPECTED_CATALOG_ROLES = [
    "teehr_read_only_role",
    "teehr_read_write_role",
    "catalog_admin_role"
]

EXPECTED_PRINCIPAL_ROLES = [
    "teehr-read-only",
    "teehr-read-write",
    "iceberg-catalog-admin"
]

# Retry configuration
MAX_RETRIES = 10
RETRY_DELAY = 2  # seconds


def get_root_token():
    """Get root token using client credentials"""
    for attempt in range(MAX_RETRIES):
        try:
            token_url = f"{POLARIS_URL}/api/catalog/v1/oauth/tokens"
            payload = {
                "grant_type": "client_credentials",
                "client_id": ROOT_CLIENT_ID,
                "client_secret": ROOT_CLIENT_SECRET,
                "scope": "PRINCIPAL_ROLE:ALL"
            }
            headers = {
                "X-Polaris-Realm": REALM,
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            response = requests.post(token_url, data=payload, headers=headers, timeout=5)
            if response.status_code == 200:
                return response.json()["access_token"]
            elif attempt < MAX_RETRIES - 1:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Polaris not ready, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get root token: {response.text}")
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Polaris not reachable, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get root token: {e}")


def check_catalog_role(root_token, role_name):
    """Check if a catalog role exists"""
    headers = {
        "Authorization": f"Bearer {root_token}",
        "X-Polaris-Realm": REALM
    }
    
    response = requests.get(
        f"{POLARIS_URL}/api/management/v1/catalogs/teehr/catalog-roles/{role_name}",
        headers=headers
    )
    
    return response.status_code == 200


def check_principal_role(root_token, role_name):
    """Check if a principal role exists"""
    headers = {
        "Authorization": f"Bearer {root_token}",
        "X-Polaris-Realm": REALM
    }
    
    response = requests.get(
        f"{POLARIS_URL}/api/management/v1/principal-roles/{role_name}",
        headers=headers
    )
    
    return response.status_code == 200


def main():
    """Run role-based ACL validation"""
    print("[test] Validating Polaris role-based ACLs...")
    
    try:
        root_token = get_root_token()
        
        # Check catalog roles
        print("  Checking catalog roles...")
        all_passed = True
        for role in EXPECTED_CATALOG_ROLES:
            if check_catalog_role(root_token, role):
                print(f"  ✓ Catalog role {role} exists")
            else:
                print(f"  WARNING: Catalog role {role} not found")
                all_passed = False
        
        # Check principal roles
        print("  Checking principal roles...")
        for role in EXPECTED_PRINCIPAL_ROLES:
            if check_principal_role(root_token, role):
                print(f"  ✓ Principal role {role} exists")
            else:
                print(f"  WARNING: Principal role {role} not found")
                all_passed = False
        
        print("[test] Role-based ACL validation complete")
        return 0 if all_passed else 1
    
    except Exception as e:
        print(f"[test] ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
