ARG BASE_IMAGE=python:3.11-slim
FROM ${BASE_IMAGE}

WORKDIR /app

# Create tests directory
RUN mkdir -p /app/tests

# Copy test scripts
COPY keycloak_users_test.py /app/tests/
COPY polaris_oidc_test.py /app/tests/
COPY polaris_namespace_test.py /app/tests/
COPY polaris_roles_test.py /app/tests/
COPY polaris_auth_token_test.py /app/tests/
COPY polaris_permissions_config_test.py /app/tests/
COPY spark_permission_test.py /app/tests/
COPY spark_authmanager_test.py /app/tests/
