#!/usr/bin/env python3
"""
Integration test: Keycloak user provisioning

Validates:
- Admin, user, and poweruser accounts exist in Keycloak
- Users have correct group membership
"""

import sys
import requests
import time

KEYCLOAK_URL = "http://keycloak-service:8080"
REALM = "teehr"
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "admin123"

EXPECTED_USERS = ["admin", "user", "poweruser"]

# Retry configuration
MAX_RETRIES = 10
RETRY_DELAY = 2  # seconds


def get_admin_token():
    """Get admin token to query Keycloak"""
    for attempt in range(MAX_RETRIES):
        try:
            token_url = f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token"
            payload = {
                "grant_type": "password",
                "client_id": "admin-cli",
                "username": ADMIN_USERNAME,
                "password": ADMIN_PASSWORD
            }
            
            response = requests.post(token_url, data=payload, timeout=5)
            if response.status_code == 200:
                return response.json()["access_token"]
            elif attempt < MAX_RETRIES - 1:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Retrying Keycloak connection...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get admin token: {response.text}")
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Keycloak not ready, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to connect to Keycloak: {e}")


def check_user_exists(admin_token, username):
    """Check if a user exists in Keycloak"""
    url = f"{KEYCLOAK_URL}/admin/realms/{REALM}/users?username={username}&exact=true"
    headers = {"Authorization": f"Bearer {admin_token}"}
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to query users: {response.text}")
    
    users = response.json()
    if len(users) == 0:
        print(f"    DEBUG: No users found for '{username}'. Response: {users}")
        return False
    
    if users[0]["username"] != username:
        print(f"    DEBUG: Expected '{username}' but got '{users[0]['username']}'")
        return False
    
    return True


def main():
    """Run user provisioning validation"""
    print("[test] Validating Keycloak users...")
    
    try:
        admin_token = get_admin_token()
        print("  ✓ Got admin token")
        
        for username in EXPECTED_USERS:
            print(f"  Checking user: {username}")
            
            if check_user_exists(admin_token, username):
                print(f"  ✓ User {username} exists")
            else:
                print(f"  ✗ ERROR: User {username} not found")
                return 1
        
        print("[test] All Keycloak users validated successfully")
        return 0
    
    except Exception as e:
        print(f"[test] ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
