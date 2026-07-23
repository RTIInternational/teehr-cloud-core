apiVersion: v1
kind: ConfigMap
metadata:
  name: keycloak-local-users-bootstrap
  labels:
    app: keycloak-local-users-bootstrap
data:
  teehr-local-users.json: |
    {
      "realm": "teehr",
      "users": [
        {
          "username": "admin",
          "enabled": true,
          "email": "admin@example.local",
          "emailVerified": true,
          "firstName": "Local",
          "lastName": "Admin",
          "credentials": [
            {
              "type": "password",
              "value": "admin",
              "temporary": false
            }
          ],
          "groups": [
            "/basic-user",
            "/iceberg-user",
            "/iceberg-catalog-admins",
            "/jupyter-admin",
            "/key-management-admin",
            "/prefect-admin",
            "/webapi-admin"
          ]
        },
        {
          "username": "user",
          "enabled": true,
          "email": "user@example.local",
          "emailVerified": true,
          "firstName": "Local",
          "lastName": "User",
          "credentials": [
            {
              "type": "password",
              "value": "user",
              "temporary": false
            }
          ],
          "groups": [
            "/basic-user",
            "/jupyter-user"
          ]
        }
      ]
    }