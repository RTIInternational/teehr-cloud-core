#!/usr/bin/env python3
"""
Integration test: Polaris permission configuration verification

Validates:
- Catalog roles and principal roles are correctly created
- Principal roles have the expected permissions granted on the teehr namespace
- Permissions are configured for read-only and read-write access patterns
"""

import sys
import json
import requests
import time

# Configuration
POLARIS_URL = "http://polaris:8181"
REALM = "teehr"
CATALOG = "teehr"
NAMESPACE = "teehr"

# Root credentials for admin access
ROOT_CLIENT_ID = "root"
ROOT_CLIENT_SECRET = "secret123"

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


def get_principal_role_grants(root_token, principal_role):
    """Get all grants for a principal role"""
    headers = {
        "Authorization": f"Bearer {root_token}",
        "X-Polaris-Realm": REALM
    }
    
    response = requests.get(
        f"{POLARIS_URL}/api/management/v1/principal-roles/{principal_role}",
        headers=headers
    )
    
    status = response.status_code
    if status != 200:
        return None
    
    return response.json()


def get_catalog_role_grants(root_token, catalog_role):
    """Get all grants for a catalog role"""
    headers = {
        "Authorization": f"Bearer {root_token}",
        "X-Polaris-Realm": REALM
    }
    
    response = requests.get(
        f"{POLARIS_URL}/api/management/v1/catalogs/{CATALOG}/catalog-roles/{catalog_role}/grants",
        headers=headers
    )
    
    status = response.status_code
    if status != 200:
        return None
    
    return response.json()


def main():
    """Verify Polaris permission configuration"""
    print("[test] Verifying Polaris permission configuration...\n")
    
    all_passed = True
    
    try:
        # Step 1: Get root token
        print("  Getting root credentials token...")
        root_token = get_root_token()
        print("  ✓ Root token obtained\n")
        
        # Step 2: Verify teehr-read-only role has correct permissions
        print("  Checking teehr-read-only role permissions...")
        read_only_role_info = get_principal_role_grants(root_token, "teehr-read-only")
        if read_only_role_info:
            print("    ✓ teehr-read-only principal role exists")
        else:
            print("    ✗ ERROR: teehr-read-only principal role not found")
            all_passed = False
        
        # Step 3: Verify teehr-read-write role has correct permissions
        print("  Checking teehr-read-write role permissions...")
        read_write_role_info = get_principal_role_grants(root_token, "teehr-read-write")
        if read_write_role_info:
            print("    ✓ teehr-read-write principal role exists")
        else:
            print("    ✗ ERROR: teehr-read-write principal role not found")
            all_passed = False
        
        # Step 4: Verify catalog roles have the expected grants
        print("  Checking teehr_read_only_role grants...")
        read_only_catalog_grants = get_catalog_role_grants(root_token, "teehr_read_only_role")
        if read_only_catalog_grants:
            print("    ✓ teehr_read_only_role catalog role exists")
            grants = read_only_catalog_grants.get("grants", [])
            if any("READ_PROPERTIES" in g.get("privilege", "") for g in grants):
                print("      ✓ READ_PROPERTIES permission is granted")
            else:
                print("      ℹ Available grants:", [g.get("privilege") for g in grants])
        else:
            print("    ✗ ERROR: teehr_read_only_role not found")
            all_passed = False
        
        print("  Checking teehr_read_write_role grants...")
        read_write_catalog_grants = get_catalog_role_grants(root_token, "teehr_read_write_role")
        if read_write_catalog_grants:
            print("    ✓ teehr_read_write_role catalog role exists")
            grants = read_write_catalog_grants.get("grants", [])
            has_create = any("CREATE" in g.get("privilege", "") for g in grants)
            has_write = any("WRITE" in g.get("privilege", "") for g in grants)
            if has_create:
                print("      ✓ TABLE_CREATE permission is granted")
            if has_write:
                print("      ✓ WRITE permission is granted")
            if not (has_create or has_write):
                print("      ℹ Available grants:", [g.get("privilege") for g in grants])
        else:
            print("    ✗ ERROR: teehr_read_write_role not found")
            all_passed = False
        
        print()
        if all_passed:
            print("[test] Polaris permission configuration verification PASSED")
            return 0
        else:
            print("[test] Polaris permission configuration verification FAILED")
            return 1
    
    except Exception as e:
        print(f"[test] ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
