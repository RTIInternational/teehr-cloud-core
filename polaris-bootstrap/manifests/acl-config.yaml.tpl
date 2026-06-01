apiVersion: v1
kind: ConfigMap
metadata:
  name: polaris-acl-config
data:
  warehouse: "${var.polaris.catalogWarehouse}"
  acl-config.json: |
    {
      "realm": "teehr",
      "catalog": "teehr",
      "catalog_admin_keycloak_role": "iceberg-catalog-admin",
      "namespaces": [
        {
          "name": "public",
          "roles": [
            {
              "keycloak_role": "iceberg-namespace-public-read",
              "polaris_principal_role": "public_reader",
              "polaris_catalog_role": "public_read_role",
              "privileges": ["TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST"]
            },
            {
              "keycloak_role": "iceberg-namespace-public-write",
              "polaris_principal_role": "public_writer",
              "polaris_catalog_role": "public_write_role",
              "privileges": ["TABLE_WRITE_DATA", "TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST", "CREATE_TABLE"]
            }
          ]
        },
        {
          "name": "restricted",
          "roles": [
            {
              "keycloak_role": "iceberg-namespace-restricted-read",
              "polaris_principal_role": "restricted_reader",
              "polaris_catalog_role": "restricted_read_role",
              "privileges": ["TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST"]
            },
            {
              "keycloak_role": "iceberg-namespace-restricted-write",
              "polaris_principal_role": "restricted_writer",
              "polaris_catalog_role": "restricted_write_role",
              "privileges": ["TABLE_WRITE_DATA", "TABLE_READ_DATA", "TABLE_LIST", "NAMESPACE_LIST", "CREATE_TABLE"]
            }
          ]
        }
      ]
    }
