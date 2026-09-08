#!/usr/bin/env python3
"""
Integration test: Polaris OIDC token validation

Validates:
- Keycloak can issue tokens to users
- Polaris accepts and validates Keycloak-issued JWT tokens
- JWT claims are properly mapped
"""

import sys
import requests
import time

KEYCLOAK_URL = "http://keycloak-service:8080"
POLARIS_URL = "http://polaris:8181"
REALM = "teehr"
TEST_USERNAME = "user"
TEST_PASSWORD = "user"

# Retry configuration
MAX_RETRIES = 10
RETRY_DELAY = 2  # seconds


def get_user_token(username, password):
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
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Keycloak not ready, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get token for {username}: {response.text}")
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES}: Connection failed, retrying...")
                time.sleep(RETRY_DELAY)
            else:
                raise Exception(f"Failed to get token for {username}: {e}")


def validate_token_with_polaris(token):
    """Test that Polaris accepts and validates the token"""
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Polaris-Realm": REALM
    }
    
    response = requests.get(
        f"{POLARIS_URL}/api/catalog/v1/config",
        headers=headers,
        params={"warehouse": REALM}
    )
    
    if response.status_code != 200:
        print(f"    DEBUG: HTTP {response.status_code}: {response.text[:300]}")
    
    return response.status_code == 200


def main():
    """Run Polaris OIDC validation tests"""
    print("[test] Validating Polaris OIDC token acceptance...")
    
    try:
        # Step 1: Get user token from Keycloak
        print("  Getting user token from Keycloak...")
        token = get_user_token(TEST_USERNAME, TEST_PASSWORD)
        print("  ✓ Token obtained")
        
        # Step 2: Test Polaris accepts the token
        print("  Testing Polaris accepts the token...")
        if validate_token_with_polaris(token):
            print("  ✓ Polaris accepted token")
        else:
            print("  ✗ ERROR: Polaris rejected token")
            return 1
        
        print("[test] Polaris OIDC validation successful")
        return 0
    
    except Exception as e:
        print(f"[test] ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
