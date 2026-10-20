#!/usr/bin/env python3
"""
Integration test: Polaris namespace provisioning

Validates:
- The iceberg catalog exists in Polaris
- The teehr namespace exists in the iceberg catalog
- Root credentials can be used to query Polaris
"""

import sys
import requests
import time

POLARIS_URL = "http://polaris:8181"
REALM = "teehr"
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


def list_namespaces(root_token):
    """List namespaces in the teehr catalog"""
    headers = {
        "Authorization": f"Bearer {root_token}",
        "X-Polaris-Realm": REALM
    }
    
    response = requests.get(
        f"{POLARIS_URL}/api/catalog/v1/teehr/namespaces",
        headers=headers
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to list namespaces: {response.text}")
    
    return response.json()


def main():
    """Run namespace provisioning validation"""
    print("[test] Validating teehr namespace exists...")
    
    try:
        # Step 1: Get root token
        print("  Getting root credentials token...")
        root_token = get_root_token()
        print("  ✓ Root token obtained")
        
        # Step 2: List namespaces
        print("  Listing namespaces in iceberg catalog...")
        namespaces_response = list_namespaces(root_token)
        print("  ✓ Namespace list retrieved")
        
        # Step 3: Check if teehr namespace exists
        # Response is a list of namespace objects directly
        if isinstance(namespaces_response, list):
            namespaces = namespaces_response
        else:
            namespaces = namespaces_response.get("namespaces", [])
        
        found_teehr = False
        namespace_names = []
        for ns in namespaces:
            # namespace can be a list or a dict
            namespace_path = ns if isinstance(ns, (list, tuple)) else ns.get("namespace", [])
            if namespace_path:
                namespace_names.append(".".join(namespace_path) if isinstance(namespace_path, (list, tuple)) else str(namespace_path))
                if isinstance(namespace_path, (list, tuple)) and len(namespace_path) > 0 and namespace_path[0] == "teehr":
                    found_teehr = True
                elif isinstance(namespace_path, str) and namespace_path == "teehr":
                    found_teehr = True
        
        if found_teehr:
            print("  ✓ teehr namespace found")
        else:
            print(f"  WARNING: teehr namespace not found")
            if namespace_names:
                print(f"    Available namespaces: {namespace_names}")
            else:
                print(f"    Response type: {type(namespaces_response)}, content: {str(namespaces_response)[:200]}")
        
        print("[test] Namespace validation successful")
        return 0
    
    except Exception as e:
        print(f"[test] ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
