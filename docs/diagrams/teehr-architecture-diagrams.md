# TEEHR Hub Architecture Diagrams

This file contains editable Mermaid diagrams tailored to the current TEEHR Hub platform.

## 1) High-Level Platform Architecture

```mermaid
flowchart LR
    User[Developer or Analyst] --> UI[Dashboards Frontend]
    User --> Hub[JupyterHub]

    UI --> API[TEEHR API FastAPI]
    Hub --> API

    API --> KC[Keycloak]
    Hub --> KC

    API --> Trino
    Hub --> Trino
    Prefect[Prefect Workflows] --> API
    Prefect --> Spark

    Spark --> IcebergREST[Iceberg REST Catalog]
    Trino --> IcebergREST

    IcebergREST --> S3[(S3 Iceberg Warehouse)]
    Spark --> S3
```

Notes:
- Captures user-facing entry points plus core compute and data services.
- Keeps storage shown at a high level for readability.

## 2) Authentication Flow (Keycloak + App Services)

```mermaid
sequenceDiagram
    autonumber
    participant User
    participant Frontend as Dashboards or JupyterHub
    participant Keycloak
    participant API as TEEHR API

    User->>Frontend: Open app and sign in
    Frontend->>Keycloak: Redirect for authentication
    Keycloak-->>Frontend: Return token (OIDC)
    Frontend->>API: Request with bearer token
    API->>Keycloak: Validate token or session context
    Keycloak-->>API: Token valid + claims
    API-->>Frontend: Authorized response
    Frontend-->>User: Show protected data/features
```

Notes:
- Represents shared login pattern for UI and notebook-driven API access.
- Use this as the base for role/group-level authorization detail later.

## 3) Data Pipeline Flow (Ingest to Query)

```mermaid
flowchart TD
    External[External Hydrologic Sources] --> Prefect[Prefect Ingestion Flow]
    Prefect --> Spark[Spark Jobs]
    Spark --> Iceberg[Iceberg Tables via REST Catalog]
    Iceberg --> Warehouse[(S3 Warehouse)]

    Warehouse --> Trino[Trino Query Engine]
    Trino --> API[TEEHR API]
    API --> Dashboards[Dashboards Frontend]
    API --> Notebooks[Jupyter Notebooks]
```

Notes:
- Highlights operational path from ingestion through analytics consumption.
- Useful for discussing ownership, retries, and data freshness SLAs.

## 4) Local Development Deployment View (Kind + Garden)

```mermaid
flowchart TB
    Dev[Developer Machine] --> Kind[Kind Kubernetes Cluster]
    Dev --> Garden[Garden Deploy or Deploy --sync]
    Garden --> Kind

    subgraph Kind Services
        FE[frontend]
        API[api]
        HUB[jupyterhub]
        KC[keycloak]
        PR[prefect-server]
        SP[spark]
        TR[trino]
        IR[iceberg-rest]
        MINIO[minio]
    end

    FE --> API
    API --> TR
    HUB --> TR
    TR --> IR
    SP --> IR
    IR --> MINIO
    PR --> API
```

Notes:
- Focuses on local cluster topology and service interactions.
- Keep this synchronized with Garden module names as they evolve.

## Editing Tips

1. For simpler layouts, change flow direction (`LR`, `TB`, `TD`).
2. Use `subgraph` blocks to reduce visual clutter.
3. Keep labels short; move details to nearby notes.
4. Split large diagrams into purpose-specific diagrams, as done here.
