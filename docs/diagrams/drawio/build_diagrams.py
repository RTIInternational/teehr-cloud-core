"""Build the TEEHR framework schematics as pages of one editable .drawio file.

Usage:
    python build_diagrams.py

Each page has its own function; add new diagrams by writing another function
that returns a Page and adding it to PAGES. Re-running overwrites the output
file, so make manual edits in draw.io only once the generated layout is final
(or update the script instead).
"""
from pathlib import Path

from drawio_helpers import (
    CLIENT, CORE, DASHED, DIRECT, EXT, INFRA, NOTE, PY, STORE, TEXT, Page, title_body as tb, write_drawio,
)

OUTPUT = Path(__file__).parent / "teehr-framework.drawio"


def overall_architecture():
    p = Page("Overall architecture", 1580, 960)
    p.box("users", tb("Users", "Web browser: dashboards, notebooks, workflow UI"), EXT, 560, 20, 440, 50)

    p.group("grp-cloud", "TEEHR-Cloud (Kubernetes)", 260, 110, 1040, 690)
    p.box("ingress", tb("Ingress", "TLS termination and routing (Contour, cert-manager)"), CORE, 290, 145, 780, 40)
    p.box("keycloak", tb("Identity and access", "Keycloak: single sign-on (OIDC), group- and role-based access, API keys"), CORE, 290, 195, 780, 40)
    p.box("monitor", tb("Monitoring", "Prometheus, Grafana"), CORE, 1090, 145, 180, 90)

    p.group("grp-apps", "Applications", 280, 255, 1000, 130, sub=True)
    # Workflows and JupyterHub share one Python environment and the same two data paths
    p.box("pyenv", tb("TEEHR-Python environment", "Spark for Iceberg tables; xarray / Icechunk for gridded data"), PY + "verticalAlign=bottom;spacingBottom=2;", 285, 265, 390, 113)
    p.box("workflows", tb("Prefect workflows", "Data ingests, metric calculations, maintenance (Prefect server)"), CLIENT, 295, 272, 180, 72)
    p.box("jupyter", tb("JupyterHub", "Notebooks for interactive evaluation"), CORE, 485, 272, 180, 72)
    p.box("api", tb("REST / Download API", "FastAPI: locations, timeseries, metrics, downloads"), CORE, 690, 270, 180, 108)
    p.box("dash", tb("Dashboards", "Purpose-built web frontend (React)"), CLIENT, 890, 270, 180, 108)
    p.box("xpub", tb("Gridded data and vector tile API", "xpublish: Zarr datasets, PMTiles vector tiles"), CORE, 1090, 270, 180, 108)

    p.group("grp-compute", "Compute", 280, 410, 1000, 90, sub=True)
    p.box("spark", tb("Apache Spark", "Distributed processing; executor pods scale on Kubernetes"), CORE, 290, 425, 380, 60)
    p.box("trino", tb("Trino", "Distributed SQL queries"), CORE, 690, 425, 180, 60)

    p.group("grp-wh", "Data warehouse", 280, 520, 1000, 265, sub=True)
    p.box("polaris", tb("Apache Polaris", "Iceberg REST catalog: table metadata, access control"), CORE, 290, 540, 580, 55)
    p.box("s3", tb("Object storage (S3)"), CORE + "verticalAlign=top;", 290, 615, 980, 155)
    p.box("iceberg", tb("Evaluation-ready tables (Apache Iceberg / Parquet)", "Domains, locations, primary and secondary timeseries, metrics"), CORE + "fillColor=#ffffff;", 310, 650, 540, 100)
    p.box("gridded", tb("Gridded datasets (Icechunk repositories)", "Zarr / Icechunk stores, GeoZarr pyramids; not registered in Polaris"), CORE + "fillColor=#ffffff;", 890, 650, 170, 100)
    p.box("tiles", tb("Vector tiles (PMTiles)", "Map layers built by workflows"), CORE + "fillColor=#ffffff;", 1110, 650, 140, 100)

    p.box("sources", tb("External data sources", "USGS observations<br>National Water Model (retrospective and forecasts, point and gridded)<br>NWPS / RFC forecasts<br>NRCS water supply forecasts<br>Other gridded datasets"), EXT + "align=left;spacingLeft=6;", 20, 255, 210, 175)
    p.box("ext-clients", tb("External clients", "Local TEEHR-Python / Spark sessions"), EXT, 20, 530, 210, 75)
    p.box("py-pkg", tb("TEEHR-Python package", "Fetching, loading and validation, querying, metrics, calculated fields; runs locally or in the cloud"), PY, 20, 650, 210, 110)

    p.box("infra-cloud", tb("Cloud infrastructure (AWS, infrastructure as code)", "EKS, VPC, IAM/IRSA, S3, ECR, EFS, cluster autoscaler<br>Terraform (TEEHR Hub) / OpenTofu (FVED)"), INFRA, 260, 830, 640, 65)
    p.box("infra-local", tb("Local development", "kind cluster with local S3"), INFRA, 920, 830, 380, 65)
    p.box("garden", "Services deployed with Garden; the shared core (teehr-cloud-core) is included in each client deployment as a git submodule", NOTE, 260, 900, 1040, 30)
    p.legend(1340, 255, width=220)

    p.edge("e-users", "users", "ingress")
    p.edge("e-src", "sources", "workflows", "fetch", "exitX=1;exitY=0.25;entryX=0;entryY=0.3;")
    p.edge("e-py-spark", "pyenv", "spark", "Iceberg tables", "exitX=0.5;exitY=1;entryX=0.5;entryY=0;")
    p.edge("e-dash-api", "dash", "api", "", "exitX=0;exitY=0.5;entryX=1;entryY=0.5;")
    p.edge("e-dash-xpub", "dash", "xpub", "", "exitX=1;exitY=0.5;entryX=0;entryY=0.5;")
    p.edge("e-api-trino", "api", "trino")
    p.edge("e-spark-pol", "spark", "polaris", "", "exitX=0.5;exitY=1;entryX=0.33;entryY=0;")
    p.edge("e-trino-pol", "trino", "polaris", "", "exitX=0.5;exitY=1;entryX=0.845;entryY=0;")
    p.edge("e-pol-ice", "polaris", "iceberg", "", "exitX=0.5;exitY=1;entryX=0.5;entryY=0;")
    p.edge("e-xpub-grid", "xpub", "gridded", "", "exitX=0.03;exitY=1;entryX=1;entryY=0.5;", points=[(1095, 700)])
    p.edge("e-xpub-tiles", "xpub", "tiles", "", "exitX=0.6;exitY=1;entryX=0.629;entryY=0;")
    # Drawn after e-api-trino so the crossing gets a jump arc
    p.edge("e-py-grid", "pyenv", "gridded", "", "exitX=0.95;exitY=1;entryX=0.529;entryY=0;" + DIRECT, points=[(656, 397), (980, 397)])
    # Branches off the same route just above the storage boxes
    p.edge("e-py-tiles", "pyenv", "tiles", "", "exitX=0.95;exitY=1;entryX=0.286;entryY=0;" + DIRECT, points=[(656, 397), (980, 397), (980, 632), (1150, 632)])
    p.box("py-grid-label", "<b>Direct object storage access</b><br>Icechunk reads and writes;<br>PMTiles built by workflows<br>(bypasses Polaris)", TEXT, 990, 420, 170, 75)
    p.edge("e-ext-pol", "ext-clients", "polaris", "Iceberg REST", "exitX=1;exitY=0.5;entryX=0;entryY=0.5;" + DASHED)
    p.edge("e-pkg-ext", "py-pkg", "ext-clients", "", DASHED)
    return p


def deployment_model():
    p = Page("Core and client deployments", 1100, 760)
    p.group("grp-repos", "Shared components", 20, 40, 300, 330)
    p.box("teehr", tb("teehr", "TEEHR-Python package<br>(PyPI / GitHub)"), PY, 50, 80, 240, 80)
    p.box("core", tb("teehr-cloud-core", "Shared platform services: Keycloak, JupyterHub, Prefect server, Spark, Trino, Polaris, APIs, local S3, monitoring"), CORE, 50, 200, 240, 140)

    p.group("grp-clients", "Client deployments", 420, 40, 640, 330)
    client_body = "React dashboards<br>Prefect workflows<br>Warehouse migrations<br>JupyterHub profiles<br>Garden project (local and remote environments)"
    p.box("hub", tb("teehr-hub (TEEHR Hub)", "CIROH National Water Model evaluation<br><br>" + client_body), CLIENT, 450, 80, 280, 260)
    p.box("fved", tb("teehr-fved (FVED)", "U.S. Bureau of Reclamation forecast evaluation; adds gridded ingests and RiverWare workflows<br><br>" + client_body), CLIENT, 750, 80, 280, 260)

    p.box("local", tb("Local environment", "kind cluster, local S3, self-signed certificates"), INFRA, 450, 450, 280, 80)
    p.box("remote", tb("Remote environment", "AWS EKS cluster"), INFRA, 750, 450, 280, 80)
    p.box("iac", tb("Infrastructure as code", "teehr-cloud-platform (Terraform): TEEHR Hub<br>aws-project-amzifved-infrastructure (OpenTofu): FVED<br>Plan/apply via GitHub Actions"), INFRA, 750, 610, 280, 110)
    p.box("note", "Client deployments pick up teehr changes by bumping the pinned git SHA, and core changes by updating the submodule pointer", NOTE, 20, 400, 300, 60)

    p.edge("e-teehr", "teehr", "grp-clients", "pip install<br>(pinned git SHA)", "exitX=1;exitY=0.5;entryX=0;entryY=0.17;")
    p.edge("e-core", "core", "grp-clients", "git submodule", "exitX=1;exitY=0.5;entryX=0;entryY=0.82;")
    p.edge("e-local", "grp-clients", "local", "garden deploy", "exitX=0.25;exitY=1;entryX=0.5;entryY=0;")
    p.edge("e-remote", "grp-clients", "remote", "garden deploy", "exitX=0.72;exitY=1;entryX=0.5;entryY=0;")
    p.edge("e-iac", "iac", "remote", "provisions")
    p.legend(20, 560, [(CORE, "Shared core (teehr-cloud-core)"), (CLIENT, "Client deployment"), (PY, "TEEHR-Python"), (INFRA, "Infrastructure")], width=260)
    return p


def data_flow():
    p = Page("End-to-end data flow", 1440, 620)
    p.box("prefect", "Scheduled and orchestrated by Prefect; processing runs on Spark executors in Kubernetes", NOTE + "dashed=1;strokeColor=#888888;dashPattern=4 4;", 195, 60, 1025, 40)
    steps = [
        ("sources", tb("External data sources", "USGS, NWM, NWPS/RFC, NRCS, gridded datasets"), EXT),
        ("ingest", tb("Ingest", "Fetch, validate against the data model, load (append / upsert)"), CLIENT),
        ("core-tables", tb("Core tables", "Locations, attributes, crosswalks; primary and secondary timeseries"), STORE),
        ("join", tb("Join and enrich", "Pair primary and secondary timeseries by location and time; add calculated fields"), CLIENT),
        ("joined", tb("Joined timeseries"), STORE),
        ("metrics", tb("Compute metrics", "Deterministic, probabilistic and signature metrics; grouping, filtering, bootstrapping"), CLIENT),
        ("outputs", tb("Metrics and summary tables", "Metrics; location, configuration and completeness summaries; vector tiles"), STORE),
        ("serve", tb("Access", "REST / Download API (Trino), dashboards, gridded data and vector tile API"), CORE),
    ]
    x = 20
    for i, (id, value, style) in enumerate(steps):
        p.box(id, value, style, x, 140, 150, 150)
        if i:
            p.edge(f"e-{i}", steps[i - 1][0], id)
        x += 175
    p.box("jupyter", tb("Interactive analysis (JupyterHub + TEEHR-Python)", "Query any table, compute ad hoc metrics, visualize"), CORE, 370, 400, 680, 70)
    for id, target, ex in [("e-jh-core", "core-tables", 0.03), ("e-jh-joined", "joined", 0.5), ("e-jh-out", "outputs", 0.97)]:
        p.edge(id, "jupyter", target, "", f"exitX={ex};exitY=0;entryX=0.5;entryY=1;startArrow=block;startFill=1;" + DASHED)
    p.box("note", "The same steps run on a laptop with TEEHR-Python and a local Iceberg warehouse", NOTE, 370, 490, 680, 30)
    p.legend(1190, 400, [(CLIENT, "Workflow step (client deployment)"), (STORE, "Warehouse table (Iceberg)"), (CORE, "Shared core service"), (EXT, "External")], width=230)
    return p


def teehr_python_overview():
    p = Page("TEEHR-Python overview", 1300, 700)
    p.box("sources", tb("Data sources", "USGS observations; NWM point and gridded, retrospective and forecasts"), EXT, 60, 20, 300, 60)
    p.box("files", tb("User data", "CSV, Parquet, NetCDF, GeoJSON, ..."), EXT, 400, 20, 300, 60)

    p.group("grp-ev", "Evaluation (local or remote; read-only or read-write)", 20, 120, 1000, 560)
    p.box("fetch", tb("ev.fetch", "USGS and NWM (point and gridded, retrospective and forecasts)"), PY, 60, 160, 300, 90)
    p.box("load", tb("ev.load", "Locations, timeseries, crosswalks, attributes; validated against the data model"), PY, 380, 160, 300, 90)
    p.box("generate", tb("ev.generate", "Benchmark and signature timeseries: persistence, reference forecasts, normals"), PY, 700, 160, 300, 90)

    p.group("grp-tables", "Tables and views", 40, 280, 960, 170, sub=True)
    p.box("domains", tb("Domain tables", "Units, variables, attributes, configurations, configuration pairs"), PY, 60, 300, 220, 130)
    p.box("locs", tb("Location tables", "Locations, location attributes, crosswalks, ID aliases"), PY, 300, 300, 220, 130)
    p.box("ts", tb("Timeseries tables", "Primary (observed) and secondary (simulated / forecast)"), PY, 540, 300, 200, 130)
    p.box("views", tb("Views", "Joined timeseries, primary / secondary timeseries, location attributes"), PY, 760, 300, 220, 130)

    p.box("query", tb("Query", "Filtering, method chaining, SQL"), PY, 60, 480, 220, 90)
    p.box("calc", tb("Calculated fields", "Row-level and timeseries-aware"), PY, 300, 480, 220, 90)
    p.box("metrics", tb("ev.metrics", "Deterministic, probabilistic (ensemble) and signature metrics; group by, filter, bootstrap"), PY, 540, 480, 200, 90)
    p.box("write", tb("I/O", "Write (append, upsert, overwrite, ...), download, extract"), PY, 760, 480, 220, 90)

    p.box("spark", tb("Apache Spark", "Local session or Kubernetes cluster"), CORE, 60, 600, 300, 60)
    p.box("catalog", tb("Apache Iceberg catalog", "Local (JDBC / SQLite) or remote (REST)"), CORE, 380, 600, 300, 60)
    p.box("storage", tb("Storage", "Local disk or S3 object storage"), CORE, 700, 600, 300, 60)

    p.box("outputs", tb("Outputs", "Spark, pandas and GeoPandas DataFrames; visualization (e.g., hvPlot)"), EXT, 1080, 400, 190, 110)

    p.edge("e-src", "sources", "fetch", "", "exitX=0.5;exitY=1;entryX=0.5;entryY=0;")
    p.edge("e-files", "files", "load", "", "exitX=0.5;exitY=1;entryX=0.5;entryY=0;")
    p.edge("e-out", "grp-ev", "outputs", "", "exitX=1;exitY=0.5;entryX=0;entryY=0.5;")
    return p


def local_warehouse():
    p = Page("Local warehouse directory", 760, 440)
    tree = "\n".join([
        "my_evaluation/",
        "├── teehr.log                       # log file (when logging is enabled)",
        "└── local/                          # Iceberg warehouse (catalog \"local\")",
        "    ├── local_catalog.db            # JDBC catalog (SQLite)",
        "    ├── version                     # evaluation version",
        "    ├── cache/                      # temporary fetching and loading files",
        "    ├── schema_evolution/",
        "    │   └── schema_version_history/ # applied schema migrations",
        "    └── teehr/                      # namespace: one directory per table",
        "        ├── units/",
        "        ├── variables/",
        "        ├── attributes/",
        "        ├── configurations/",
        "        ├── configuration_pairs/",
        "        ├── locations/",
        "        ├── location_attributes/",
        "        ├── location_crosswalks/",
        "        ├── location_id_aliases/",
        "        ├── primary_timeseries/",
        "        │   ├── data/               # Parquet data files",
        "        │   └── metadata/           # Iceberg metadata (JSON, Avro)",
        "        └── secondary_timeseries/",
    ])
    # Non-breaking spaces keep the tree aligned in draw.io's HTML labels
    tree = "<br>".join(line.replace(" ", "&nbsp;") for line in tree.split("\n"))
    p.box("tree", tree, "text;html=1;whiteSpace=nowrap;align=left;verticalAlign=top;fontFamily=Courier New;fontSize=13;spacing=12;rounded=1;fillColor=#ffe6cc;strokeColor=#d79b00;", 20, 20, 720, 400)
    return p


PAGES = [overall_architecture, deployment_model, data_flow, teehr_python_overview, local_warehouse]

if __name__ == "__main__":
    write_drawio(OUTPUT, [build() for build in PAGES])
    print(f"Wrote {len(PAGES)} pages to {OUTPUT}")
