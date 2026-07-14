# Mermaid Basics for TEEHR Hub

This page is a minimal sandbox to learn Mermaid syntax before we draft project diagrams.

## How to Use This File

1. Open this file in VS Code.
2. Open Markdown preview.
3. Edit labels, arrows, and groups directly in the Mermaid blocks.
4. Save and confirm the diagram updates.

## Example 1: Basic Flowchart

```mermaid
flowchart TD
    A[User] --> B[Web UI]
    B --> C[FastAPI]
    C --> E[(Text)]
    E --> D[(Database)]
```

Try editing:
- Change `LR` to `TD` to switch layout direction.
- Rename nodes, for example `API` to `FastAPI`.
- Add a step between `C` and `D`.

## Example 2: Grouped Components

```mermaid
flowchart TB
    subgraph Client
        U[Browser]
    end

    subgraph Platform
        FE[Frontend]
        API[Backend API]
    end

    subgraph Data
        DB[(Postgres)]
        OBJ[(Object Store)]
    end

    U --> FE --> API
    API --> DB
    API --> OBJ
```

Try editing:
- Rename subgraphs.
- Add another component in `Platform`.
- Add arrows to show more dependencies.

## Example 3: Simple Sequence Diagram

```mermaid
sequenceDiagram
    participant User
    participant Frontend
    participant API

    User->>Frontend: Click Run Query
    Frontend->>API: POST /query
    API-->>Frontend: 200 OK + data
    Frontend-->>User: Render chart
```

Try editing:
- Add a new participant like `Trino`.
- Insert a new request/response step.
- Change labels to match your own use case.

## Next Step

Once this feels comfortable, we can add repository-specific diagrams for:
- high-level architecture
- authentication flow
- data pipeline flow
- deployment view
