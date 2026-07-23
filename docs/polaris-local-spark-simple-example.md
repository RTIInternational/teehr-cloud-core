# Polaris Local Spark Simple Example

This is a minimal local-development flow for Polaris on KinD with MinIO using Spark.

## Goal

- Authenticate as `admin`
- Connect Spark to Polaris REST catalog
- Create a namespace
- Create and query a table

## Prerequisites

- Cluster is deployed and healthy: `garden deploy`
- A Jupyter single-user pod is running (for example `jupyter-admin`)
- Local Keycloak users exist (`admin` / `admin` by default)

## 1. Refresh Polaris bootstrap config

Run this once after changing Polaris/MinIO/catalog settings:

```bash
kubectl -n teehr-hub delete job polaris-bootstrap --ignore-not-found=true
kubectl -n teehr-hub apply -f ./polaris-bootstrap/manifests/bootstrap-job.yaml
kubectl -n teehr-hub wait --for=condition=Complete job/polaris-bootstrap --timeout=600s
```

## 2. Run the minimal Spark example

```bash
bash ./scripts/run_jupyter_polaris_spark_example.sh
```

Optional: keep created objects instead of dropping them.

```bash
EXTRA_ARGS="--keep --namespace spark_demo_manual --table demo_table" bash ./scripts/run_jupyter_polaris_spark_example.sh
```

## 3. Expected behavior

The script prints:

- effective Spark Polaris/MinIO config
- namespace listing
- namespace creation
- table creation, insert, and select

## 4. Known failure signature and meaning

If table creation fails with `UnknownHostException` and a host like `warehouse.minio`, then:

- Spark-to-Polaris auth is working
- namespace operations are working
- Polaris server-side object store write is using virtual-host style DNS
- local MinIO path-style behavior is not being honored for that write path

Root cause observed in this repo:

- catalog-level `s3.*` properties were present, but Polaris table-write path required `table-default.s3.*`
- once `table-default.s3.endpoint`, `table-default.s3.path-style-access`, and `table-default.s3.region` were added, Spark table create succeeded

Check quickly:

```bash
kubectl -n teehr-hub logs deploy/polaris --since=10m | grep -E 'UnknownHostException|warehouse.minio|Unable to execute HTTP request'
```

Verify active catalog settings (from a Jupyter pod):

```bash
kubectl -n teehr-hub exec jupyter-admin -c notebook -- python -c "import requests; t=requests.post('http://keycloak-service:8080/realms/teehr/protocol/openid-connect/token',data={'grant_type':'password','client_id':'jupyterhub','client_secret':'local-jupyterhub-client-secret','username':'admin','password':'admin','scope':'openid profile email'},timeout=20).json()['access_token']; h={'Authorization':f'Bearer {t}','X-Polaris-Realm':'teehr'}; print(requests.get('http://polaris:8181/api/management/v1/catalogs/teehr',headers=h,timeout=20).json())"
```

Look for:

- `storageConfigInfo.endpoint = http://minio:9000`
- `storageConfigInfo.pathStyleAccess = true`
- catalog properties containing both:
	- `s3.path-style-access = true`
	- `table-default.s3.path-style-access = true`
	- `table-default.s3.endpoint = http://minio:9000`

## 5. Durable fix in manifests

The bootstrap job now writes both catalog-level and table-default S3 properties during catalog create/update.

- `s3.endpoint` and `table-default.s3.endpoint`
- `s3.path-style-access` and `table-default.s3.path-style-access`
- `s3.region` and `table-default.s3.region`
- `s3.remote-signing-enabled` and `table-default.s3.remote-signing-enabled`

After pulling these changes, rerun bootstrap:

```bash
kubectl -n teehr-hub delete job polaris-bootstrap --ignore-not-found=true
kubectl -n teehr-hub apply -f ./polaris-bootstrap/manifests/bootstrap-job.yaml
kubectl -n teehr-hub wait --for=condition=Complete job/polaris-bootstrap --timeout=600s
```

## 6. Files for this simple flow

- `examples/developer/polaris_spark_namespace_table_example.py`
- `scripts/run_jupyter_polaris_spark_example.sh`
- `examples/developer/setup_utils.py`
- `polaris-bootstrap/manifests/bootstrap-job.yaml`
- `polaris-bootstrap/manifests/acl-config.yaml.tpl`
