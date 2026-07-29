#!/usr/bin/env python3
"""
Integration test: Spark session authentication with Polaris + Keycloak

Validates:
- Keycloak token acquisition for test users (poweruser, user)
- Polaris accepts Keycloak JWT tokens with correct realm and warehouse
- Different users can obtain tokens and authenticate with Polaris
"""

import sys
import json
import requests
import time
import base64

# Configuration
KEYCLOAK_URL = "http://keycloak-service:8080"
POLARIS_URL = "http://polaris:8181"
REALM = "teehr"
CATALOG = "teehr"
NAMESPACE = "teehr"

TEST_USERS = {
    "poweruser": {"password": "poweruser", "should_write": True},
    "user": {"password": "user", "should_write": False}
}

# Retry configuration
MAX_RETRIES = 10
RETRY_DELAY = 2  # seconds


def get_keycloak_token(username, password):
    """Get a JWT token from Keycloak for a user"""
    for attempt in range(MAX_RETRIES):
        try:
            token_url = f"{KEYCLOAK_URL}/realms/{REALM}/protocol/openid-connect/token"
            payload = {
                "grant_type": "password",
                "client_id": "jupyterhub",
                "client_secret": "local-jupyterhub-client-secret",
                "username": username,
                "password": password,
                "scope": "openid"
            }
            
            response = requests.post(token_url, data=payload, timeout=5)
            if response.status_code == 200:
                return response.json()["access_token"]
            elif response.status_code == 401 and attempt < MAX_RETRIES - 1:
                print(f"    Attempt {attempt + 1}/{MAX_RETRIES}: Keycloak not ready, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get token for {username}: {response.text}")
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Attempt {attempt + 1}/{MAX_RETRIES}: Connection failed, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get token for {username}: {e}")


def validate_token_with_polaris(token):
    """Validate that Polaris accepts the token"""
    for attempt in range(MAX_RETRIES):
        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "X-Polaris-Realm": REALM
            }
            response = requests.get(
                f"{POLARIS_URL}/api/catalog/v1/config",
                headers=headers,
                params={"warehouse": CATALOG},
                timeout=5
            )
            if response.status_code == 200:
                return True
            elif response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                print(f"    Attempt {attempt + 1}/{MAX_RETRIES}: Polaris error, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"    DEBUG: HTTP {response.status_code}: {response.text[:300]}")
                return False
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Attempt {attempt + 1}/{MAX_RETRIES}: Connection failed, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                return False
    return False


def check_token_roles(token, username):
    """Verify expected roles in JWT token"""
    try:
        parts = token.split(".")
        # Add padding if needed
        payload_part = parts[1]
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += "=" * padding
        
        payload = json.loads(base64.urlsafe_b64decode(payload_part))
        roles = payload.get("realm_access", {}).get("roles", [])
        
        # Check if user has expected roles
        if username == "poweruser":
            if "teehr-read-write" in roles:
                print(f"  ✓ User has teehr-read-write role")
                return True
            else:
                print(f"  WARNING: User missing teehr-read-write role (has: {roles})")
                return False
        elif username == "user":
            if "teehr-read-only" in roles:
                print(f"  ✓ User has teehr-read-only role")
                return True
            else:
                print(f"  WARNING: User missing teehr-read-only role (has: {roles})")
                return False
    except Exception as e:
        print(f"  WARNING: Could not decode token roles: {e}")
        return False


def main():
    """Run Spark session authentication tests"""
    print("[test] Starting Spark session authentication tests...")
    print()
    
    all_passed = True
    
    for username, config in TEST_USERS.items():
        print(f"Testing user: {username}")
        
        try:
            # Step 1: Get Keycloak token
            print("  Getting Keycloak token...")
            token = get_keycloak_token(username, config["password"])
            print("  ✓ Token obtained")
            
            # Step 2: Validate token with Polaris
            print("  Validating token with Polaris...")
            if validate_token_with_polaris(token):
                print("  ✓ Polaris accepted token")
            else:
                print("  ✗ ERROR: Polaris rejected token")
                all_passed = False
                continue
            
            # Step 3: Verify role in token
            print("  Checking token roles...")
            check_token_roles(token, username)
        
        except Exception as e:
            print(f"  ✗ ERROR: {e}")
            all_passed = False
        
        print()
    
    print("[test] Spark session auth tests", "PASSED" if all_passed else "FAILED")
    
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
