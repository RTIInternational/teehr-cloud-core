# TEEHR Cloud Core
`TEEHR Cloud Core` is a core set of cloud services built around the Tools for Exploratory Evaluation in Hydrologic Research (TEEHR) to support large scale data analytics for the purpose of evaluating hydrologic model performance.

TEEHR Cloud Core is meant to be included as a submodule in repositories for project/client specific deployments.  It utilizes Kubernetes to orchestrate the services that make up the system.  The primary components are:
- JupyterHub
- Prefect
- Keycloak
- Apache Iceberg
- Apache Spark
- Trino
- FastAPI

Cleint specific application such as dashboards are to be managed in the client or project specific repsoitory such as `teehr-hub` or `teehr-fved`.


## Local Core Development
For local development developers can run a local instance of the cluster using KinD.  You will need to have the following installed.  The versions shown are known to work for our developers.  Subsequent minor and bug fix releases should also work but your milage may vary.

First clone the repo:
```bash
git clone https://github.com/RTIInternational/teehr-cloud-core.git
```

KinD https://kind.sigs.k8s.io/
```bash
% kind version
kind v0.27.0 go1.24.0 darwin/arm64
```

Garden https://docs.garden.io/getting-started/quickstart
```bash
% garden version
garden version: 0.13.54
```

kubctl https://kubernetes.io/docs/tasks/tools/
```bash
% kubectl version
Client Version: v1.33.0
Kustomize Version: v5.5.0
Server Version: v1.33.0
```

Optional, but recommended, k9s https://k9scli.io/
```bash
 % k9s version
 ____  __ ________       
|    |/  /   __   \______
|       /\____    /  ___/
|    \   \  /    /\___  \
|____|\__ \/____//____  /
         \/           \/ 

Version:    v0.50.2
Commit:     bc22b8705304b86c2f4c417a088accdfed13fdf8
Date:       2025-04-10T15:32:12Z
```

After you have the dependencies above installed you should be able to create a kind cluster by running the following from the repo root.
```bash
./kind/create_kind_cluster.sh 
```

NOTE: From this point on, some operations will expect additional secrets to be provided by the developer. These are typically provided in `secrets/secrets.local.private.yaml` ([example](https://github.com/RTIInternational/teehr-hub/blob/main/secrets/secrets.local.private.yaml.example)). Coordinate with the team for internal development standards.

If the kind cluster creation is successful, you can then run the following to deploy the application to the local cluster:

We do not include a `project.garden.yml` file in this repository as there is potneitla for it to conflic with the version that must be provided in the deployment repos.  Instead we provide an example and it is up to the developer to create a copy with the correct name.  To do so and run the deployment, run the following.
```bash
cp example.project.garden.yml project.garden.yml
garden deploy
```

This should create all the services in the cluster.  To test, open a browser and go to `https://api.teehr.local.app.garden`. Two notes:
1) We use a self-sign certificate for local development so you will have to accept it in your browser. Specifically, you will need to do so for the API before the dashboards will work by going to `api.teehr.local.app.garden` and accepting the self-signed cert.
2) Note you may need to edit your `/etc/hosts` file to have this address point to localhost.  You likely need the following entries in your `/etc/hosts` file.

```bash
# Add for TEEHR-HUB development
127.0.0.1       hub.teehr.local.app.garden
127.0.0.1       minio.teehr.local.app.garden
127.0.0.1       api.teehr.local.app.garden
127.0.0.1       prefect.teehr.local.app.garden
```

### Create Keycloak User
The deployment process create 2 users as described in `docs/access-control-matrix.md`

| User type | Default username | Default password | Group membership |
|---|---|---|---|
| Admin test user | admin | admin |basic-user, iceberg-user, jupyter-admin, key-management-admin, prefect-admin, webapi-admin |
| Regular test user | user | user |basic-user, jupyter-user |


### Load Test Data to Warehouse
Loading data is a little fractured depending on what data you are loading.  For the purpose of developing there are 2 different types of data that can be loaded. Regardless, you first need to create an Iceberg warehouse in the KinD cluster, then load some data.

1) To create the Iceberg warehouse and load some historic simulation data, start by going to the JupyterHub environment `hub.teehr.local.app.garden` and logging in with username: `user` and password: `user`.

2) Copy the contents of `examples/developer` to JupyterHub.  Note that the local_data folder and its contents will have to be uploaded as separate operations (i.e. create a folder manually, upload the file(s), and move into the folder).

3) Run the following notebooks in order.  This will create an Iceberg data warehouse in the KinD cluster and populate it with historic observations and simulations for 10 sites.
- `01_setup_minio_warehouse.ipynb`
- `02_create_joined_timeseries.ipynb`
- `03_generate_basic_metrics.ipynb`

4) To load some recent (but not too recent forecasts), go to the Prefect UI at `https://prefect.teehr.local.app.garden` and log in with a Keycloak user in the `admin` group. Navigate to `Deployments`.

5) Click on `ingest-usgs-streamflow-obs`.  In the upper right corner select Run > Custom Run.  Change the num_lookback_days to 10 and Submit.  Monitor the run through the browser UI.  When done, proceed to the next one.

6) Click on `ingest-nwm-medium-range-streamflow-forecasts`. In the upper right corner select Run > Custom Run.  Change the `end_dt` to a date approximately 9 days prior to today, remove the `Z` from the end of the `end_dt` string (TEEHR expects a tz-naive datetime), and Submit.  Monitor the run through the browser UI.  When done, proceed to the next one.

7) Click on `update-joined-forecast-table`. In the upper right corner select Run > Custom Run. Submit.

8) Click on `update-forecast-metrics-table`. In the upper right corner select Run > Custom Run. Submit.

Now the fun of adding new features and bug fixes starts.

### Code Syncing
When working on the API or the frontend it is convenient to have code syncing.  Code syncing can be done in `garden` by running:
```bash
garden deploy --sync
```