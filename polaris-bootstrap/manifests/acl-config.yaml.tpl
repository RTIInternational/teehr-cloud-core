apiVersion: v1
kind: ConfigMap
metadata:
  name: polaris-acl-config
data:
  acl-config.json: |
    {
      "realms": [
        {
          "realm": "${var.polaris.defaultRealm}",
          "catalog": "${var.polaris.defaultRealm}",
          "warehouse": "${var.polaris.catalogWarehouse}",
          "storage_type": "S3",
          "allowed_locations": [
            "${var.polaris.catalogWarehouse}"
          ],
          "admin": {
            "principal_role": "iceberg-catalog-admin",
            "catalog_role": "catalog_admin_role",
            "privileges": [
              "CATALOG_MANAGE_CONTENT",
              "CATALOG_MANAGE_METADATA"
            ]
          },
          "namespace_policies": [
            {
              "namespace": "public",
              "roles": [
                {
                  "principal_role": "iceberg-namespace-public-read",
                  "catalog_role": "public_read_role",
                  "grants": [
                    {
                      "type": "namespace",
                      "privileges": [
                        "NAMESPACE_READ_PROPERTIES"
                      ]
                    }
                  ]
                },
                {
                  "principal_role": "iceberg-namespace-public-write",
                  "catalog_role": "public_write_role",
                  "grants": [
                    {
                      "type": "namespace",
                      "privileges": [
                        "NAMESPACE_READ_PROPERTIES",
                        "NAMESPACE_WRITE_PROPERTIES",
                        "TABLE_CREATE",
                        "TABLE_DROP"
                      ]
                    }
                  ]
                }
              ]
            },
            {
              "namespace": "restricted",
              "roles": [
                {
                  "principal_role": "iceberg-namespace-restricted-read",
                  "catalog_role": "restricted_read_role",
                  "grants": [
                    {
                      "type": "namespace",
                      "privileges": [
                        "NAMESPACE_READ_PROPERTIES"
                      ]
                    }
                  ]
                },
                {
                  "principal_role": "iceberg-namespace-restricted-write",
                  "catalog_role": "restricted_write_role",
                  "grants": [
                    {
                      "type": "namespace",
                      "privileges": [
                        "NAMESPACE_READ_PROPERTIES",
                        "NAMESPACE_WRITE_PROPERTIES",
                        "TABLE_CREATE",
                        "TABLE_DROP"
                      ]
                    }
                  ]
                }
              ]
            }
          ],
          "table_policies": [],
          "principals": [
            {
              "name": "spark-polaris",
              "principal_role": "iceberg-catalog-admin"
            }
          ]
        }
      ]
    }
