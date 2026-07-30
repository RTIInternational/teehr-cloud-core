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
          "s3_endpoint": "${var.polaris.catalogS3Endpoint}",
          "path_style_access": "${var.polaris.catalogS3PathStyleAccess}",
          "s3_region": "${var.polaris.catalogS3Region}",
          "sts_unavailable": ${var.polaris.storageStsUnavailable},
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
              "namespace": "teehr",
              "roles": [
                {
                  "principal_role": "teehr-read-only",
                  "catalog_role": "teehr_read_only_role",
                  "grants": [
                    {
                      "type": "namespace",
                      "privileges": [
                        "NAMESPACE_READ_PROPERTIES",
                        "TABLE_LIST",
                        "TABLE_READ_PROPERTIES",
                        "TABLE_READ_DATA"
                      ]
                    }
                  ]
                },
                {
                  "principal_role": "teehr-read-write",
                  "catalog_role": "teehr_read_write_role",
                  "grants": [
                    {
                      "type": "namespace",
                      "privileges": [
                        "NAMESPACE_READ_PROPERTIES",
                        "NAMESPACE_WRITE_PROPERTIES",
                        "TABLE_CREATE",
                        "TABLE_DROP",
                        "TABLE_LIST",
                        "TABLE_READ_PROPERTIES",
                        "TABLE_WRITE_PROPERTIES",
                        "TABLE_READ_DATA",
                        "TABLE_WRITE_DATA"
                      ]
                    }
                  ]
                }
              ]
            }
          ],
          "table_policies": [],
          "principals": []
        }
      ]
    }
