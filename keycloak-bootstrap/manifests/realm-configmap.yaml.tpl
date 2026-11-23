apiVersion: v1
kind: ConfigMap
metadata:
  name: keycloak-realm-bootstrap
  labels:
    app: keycloak-bootstrap
data:
  teehr-realm.json: |
    {
      "realm": "teehr",
      "enabled": true,
      "accessTokenLifespan": 300,
      "offlineSessionIdleTimeout": 31536000,
      "offlineSessionMaxLifespanEnabled": true,
      "offlineSessionMaxLifespan": 31536000,
      "loginTheme": "teehr",
      "registrationAllowed": true,
      "loginWithEmailAllowed": true,
      "duplicateEmailsAllowed": false,
      "resetPasswordAllowed": true,
      "verifyEmail": true,
      "smtpServer": {
        "host": "$(env:SMTP_HOST)",
        "port": "$(env:SMTP_PORT)",
        "from": "$(env:SMTP_FROM)",
        "fromDisplayName": "$(env:SMTP_FROM_DISPLAY_NAME)",
        "replyTo": "$(env:SMTP_REPLY_TO)",
        "replyToDisplayName": "$(env:SMTP_REPLY_TO_DISPLAY_NAME)",
        "auth": "true",
        "user": "$(env:SMTP_USERNAME)",
        "password": "$(env:SMTP_PASSWORD)",
        "starttls": "$(env:SMTP_STARTTLS)",
        "ssl": "$(env:SMTP_SSL)"
      },
      "roles": {
        "realm": [
          { "name": "admin" },
          { "name": "basic-user" },
          { "name": "jupyter-user" },
          { "name": "teehr-read-only" },
          { "name": "teehr-read-write" },
          { "name": "iceberg-catalog-admin" },
          { "name": "teehr-platform-write", "description": "Write the public and platform system projects (ingest)" },
          { "name": "control-plane-admin", "description": "Edit every project in the control plane. Very few people." },
          { "name": "control-plane-viewer", "description": "See every project and its members in the control plane; change nothing" },
          { "name": "polaris-service-admin", "description": "Polaris service_admin via token (with a named-principal binding). The control plane's service account only." },
          { "name": "icechunk-read-only" }
        ]
      },
      "groups": [
        {
          "name": "basic-user",
          "realmRoles": ["basic-user"]
        },
        {
          "name": "jupyter-user",
          "realmRoles": ["jupyter-user"]
        },
        {
          "name": "jupyter-admin",
          "realmRoles": ["jupyter-user"]
        },
        {
          "name": "teehr-read-only",
          "realmRoles": ["teehr-read-only"]
        },
        {
          "name": "teehr-read-write",
          "realmRoles": ["teehr-read-write", "teehr-read-only"]
        },
        {
          "name": "key-management-admin",
          "realmRoles": ["admin"]
        },
        {
          "name": "prefect-admin",
          "realmRoles": ["admin"]
        },
        {
          "name": "webapi-admin",
          "realmRoles": ["admin"],
          "clientRoles": {
            "realm-management": [
              "manage-users",
              "query-users",
              "view-users",
              "query-groups",
              "view-realm"
            ]
          }
        },
        {
          "name": "iceberg-catalog-admins",
          "realmRoles": ["iceberg-catalog-admin"]
        },
        {
          "name": "control-plane-admins",
          "realmRoles": ["control-plane-admin"]
        },
        {
          "name": "control-plane-viewers",
          "realmRoles": ["control-plane-viewer"]
        },
        {
          "name": "icechunk-read-only",
          "realmRoles": ["icechunk-read-only"]
        }
      ],
      "defaultGroups": [
        "/basic-user",
        "/teehr-read-only",
        "/icechunk-read-only"
      ],
      "clients": [
        {
          "clientId": "teehr-frontend",
          "enabled": true,
          "protocol": "openid-connect",
          "publicClient": true,
          "redirectUris": [
            "https://dashboards.${var.hostname}/*",
            "https://api.${var.hostname}/docs/oauth2-redirect"
          ],
          "webOrigins": [
            "https://dashboards.${var.hostname}",
            "https://api.${var.hostname}"
          ],
          "attributes": {
            "pkce.code.challenge.method": "S256"
          },
          "protocolMappers": [
            {
              "name": "audience-teehr-control-plane-jobs",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-audience-mapper",
              "consentRequired": false,
              "config": {
                "included.custom.audience": "teehr-control-plane-jobs",
                "id.token.claim": "false",
                "access.token.claim": "true"
              }
            }
          ]
        },
        {
          "clientId": "teehr-control-plane",
          "name": "Project control plane (service account)",
          "description": "The control plane's own identity, for the Keycloak admin API and Polaris. On where the control plane is (local; var.controlPlane.enabled).",
          "enabled": ${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? "true" : "false"},
          "protocol": "openid-connect",
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "standardFlowEnabled": false,
          "directAccessGrantsEnabled": false,
          "secret": "${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? '$(env:CONTROL_PLANE_CLIENT_SECRET)' : ''}",
          "protocolMappers": [
            {
              "name": "realm-roles",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-realm-role-mapper",
              "consentRequired": false,
              "config": {
                "multivalued": "true",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "realm_access.roles",
                "jsonType.label": "String"
              }
            }
          ]
        },
        {
          "clientId": "teehr-control-plane-jobs",
          "name": "Project control plane (load jobs act as their caller)",
          "description": "Token exchange only (ADR-0014): turns a caller's token into a refresh token a job acts as them with. Separate from teehr-control-plane so one secret isn't both the admin identity and the act-as-users identity.",
          "enabled": ${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? "true" : "false"},
          "protocol": "openid-connect",
          "publicClient": false,
          "serviceAccountsEnabled": false,
          "standardFlowEnabled": false,
          "directAccessGrantsEnabled": false,
          "attributes": {
            "standard.token.exchange.enabled": "true",
            "standard.token.exchange.enableRefreshRequestedTokenType": "SAME_SESSION",
            "oauth2.device.authorization.grant.enabled": "true"
          },
          "secret": "${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? '$(env:CONTROL_PLANE_JOBS_CLIENT_SECRET)' : ''}",
          "protocolMappers": [
            {
              "name": "realm-roles",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-realm-role-mapper",
              "consentRequired": false,
              "config": {
                "multivalued": "true",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "realm_access.roles",
                "jsonType.label": "String"
              }
            }
          ]
        },
        {
          "clientId": "teehr-control-plane-docs",
          "name": "Control plane API docs (Swagger login)",
          "enabled": ${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? "true" : "false"},
          "protocol": "openid-connect",
          "publicClient": true,
          "standardFlowEnabled": true,
          "directAccessGrantsEnabled": false,
          "redirectUris": [
            "https://control-plane.${var.hostname}/docs/oauth2-redirect"
          ],
          "webOrigins": [
            "https://control-plane.${var.hostname}"
          ],
          "attributes": {
            "pkce.code.challenge.method": "S256"
          },
          "protocolMappers": [
            {
              "name": "audience-teehr-control-plane-jobs",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-audience-mapper",
              "consentRequired": false,
              "config": {
                "included.custom.audience": "teehr-control-plane-jobs",
                "id.token.claim": "false",
                "access.token.claim": "true"
              }
            }
          ]
        },
        {
          "clientId": "teehr-frontend-local-dev",
          "enabled": ${var.enableLocalDevFrontendClient ? "true" : "false"},
          "protocol": "openid-connect",
          "publicClient": true,
          "redirectUris": [
            "http://localhost:8080/*",
            "http://127.0.0.1:8080/*"
          ],
          "webOrigins": [
            "http://localhost:8080",
            "http://127.0.0.1:8080"
          ],
          "attributes": {
            "pkce.code.challenge.method": "S256"
          },
          "protocolMappers": [
            {
              "name": "audience-teehr-control-plane-jobs",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-audience-mapper",
              "consentRequired": false,
              "config": {
                "included.custom.audience": "teehr-control-plane-jobs",
                "id.token.claim": "false",
                "access.token.claim": "true"
              }
            }
          ]
        },
        {
          "clientId": "teehr-api",
          "enabled": true,
          "protocol": "openid-connect",
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "attributes": {
            "standard.token.exchange.enabled": "true"
          },
          "secret": "$(env:TEEHR_API_CLIENT_SECRET)"
        },
        {
          "clientId": "jupyterhub",
          "enabled": true,
          "protocol": "openid-connect",
          "publicClient": false,
          "secret": "$(env:JUPYTERHUB_CLIENT_SECRET)",
          "attributes": {
            "post.logout.redirect.uris": "https://hub.${var.hostname}"
          },
          "protocolMappers": [
            {
              "name": "groups",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-group-membership-mapper",
              "consentRequired": false,
              "config": {
                "full.path": "false",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "groups"
              }
            },
            {
              "name": "realm-roles",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-realm-role-mapper",
              "consentRequired": false,
              "config": {
                "multivalued": "true",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "realm_access.roles",
                "jsonType.label": "String"
              }
            },
            {
              "name": "audience-teehr-api",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-audience-mapper",
              "consentRequired": false,
              "config": {
                "included.client.audience": "teehr-api",
                "id.token.claim": "false",
                "access.token.claim": "true"
              }
            },
            {
              "name": "audience-teehr-control-plane-jobs",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-audience-mapper",
              "consentRequired": false,
              "config": {
                "included.custom.audience": "teehr-control-plane-jobs",
                "id.token.claim": "false",
                "access.token.claim": "true"
              }
            }
          ],
          "redirectUris": [
            "https://hub.${var.hostname}/hub/oauth_callback"
          ],
          "webOrigins": [
            "https://hub.${var.hostname}"
          ]
        },
        {
          "clientId": "prefect-oauth2-proxy",
          "enabled": true,
          "protocol": "openid-connect",
          "publicClient": false,
          "secret": "$(env:PREFECT_OAUTH2_CLIENT_SECRET)",
          "attributes": {
            "post.logout.redirect.uris": "https://prefect.${var.hostname}/*"
          },
          "protocolMappers": [
            {
              "name": "groups",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-group-membership-mapper",
              "consentRequired": false,
              "config": {
                "full.path": "false",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "groups"
              }
            }
          ],
          "redirectUris": [
            "https://prefect.${var.hostname}/oauth2/callback"
          ],
          "webOrigins": [
            "https://prefect.${var.hostname}"
          ]
        },
        {
          "clientId": "trino-polaris",
          "enabled": true,
          "protocol": "openid-connect",
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "secret": "$(env:TRINO_POLARIS_CLIENT_SECRET)",
          "protocolMappers": [
            {
              "name": "realm-roles",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-realm-role-mapper",
              "consentRequired": false,
              "config": {
                "multivalued": "true",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "realm_access.roles",
                "jsonType.label": "String"
              }
            }
          ]
        },
        {
          "clientId": "prefect-polaris",
          "enabled": true,
          "protocol": "openid-connect",
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "secret": "$(env:PREFECT_POLARIS_CLIENT_SECRET)",
          "protocolMappers": [
            {
              "name": "realm-roles",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-realm-role-mapper",
              "consentRequired": false,
              "config": {
                "multivalued": "true",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "realm_access.roles",
                "jsonType.label": "String"
              }
            }
          ]
        },
        {
          "clientId": "teehr-platform-automation",
          "name": "Platform automation (v1 flows)",
          "description": "The v1 Prefect flows' service account, a write member of the system projects (ADR-0020). Refresh tokens for client credentials so a run it starts can act as it (ADR-0014). On where the control plane is (local; var.controlPlane.enabled).",
          "enabled": ${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? "true" : "false"},
          "protocol": "openid-connect",
          "publicClient": false,
          "serviceAccountsEnabled": true,
          "standardFlowEnabled": false,
          "directAccessGrantsEnabled": false,
          "attributes": {
            "client_credentials.use_refresh_token": "true"
          },
          "secret": "${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? '$(env:PLATFORM_AUTOMATION_CLIENT_SECRET)' : ''}",
          "protocolMappers": [
            {
              "name": "realm-roles",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-usermodel-realm-role-mapper",
              "consentRequired": false,
              "config": {
                "multivalued": "true",
                "id.token.claim": "true",
                "access.token.claim": "true",
                "userinfo.token.claim": "true",
                "claim.name": "realm_access.roles",
                "jsonType.label": "String"
              }
            },
            {
              "name": "audience-teehr-control-plane-jobs",
              "protocol": "openid-connect",
              "protocolMapper": "oidc-audience-mapper",
              "consentRequired": false,
              "config": {
                "included.custom.audience": "teehr-control-plane-jobs",
                "id.token.claim": "false",
                "access.token.claim": "true"
              }
            }
          ]
        }
      ],
      "users": [
        {
          "username": "service-account-trino-polaris",
          "enabled": true,
          "serviceAccountClientId": "trino-polaris",
          "realmRoles": ["iceberg-catalog-admin"]
        },
        {
          "username": "service-account-teehr-control-plane",
          "enabled": true,
          "serviceAccountClientId": "teehr-control-plane",
          "realmRoles": ${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? '["polaris-service-admin"]' : '[]'},
          "clientRoles": {
            "realm-management": ${(environment.name == 'local' || (var.controlPlane.enabled || false)) ? '["manage-realm", "view-realm", "manage-users", "view-users", "query-users"]' : '[]'}
          }
        },
        {
          "username": "service-account-prefect-polaris",
          "enabled": true,
          "serviceAccountClientId": "prefect-polaris",
          "realmRoles": ["teehr-read-write", "teehr-platform-write"]
        }
      ]
    }
