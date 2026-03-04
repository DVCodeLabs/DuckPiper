# Marketplace Listing Copy

Use this document as the source of truth for VS Code Marketplace listing text.

## Extension Name

`Duck Piper`

## Short Description (`package.json -> description`)

`Local-first SQL, DuckDB notebooks, and pipelines in VS Code.`

## Marketplace Tagline (first paragraph of README)

Duck Piper is a VS Code extension for local-first analytics workflows: run SQL, manage connections, cache data into a workspace DuckDB, and build DuckDB-backed notebook pipelines with lineage and ERD artifacts you can commit and review.

## Value Proposition Bullets

- Offline-first by default for local iteration loops
- Built-in Data Work DuckDB cache per workspace
- Custom `*.dpnb` notebooks for analyze/transform pipelines
- Automatic lineage JSON + lineage view
- ERD generation from schema introspection snapshots
- Optional AI docs/comments with provider flexibility

## Key Features Section Copy

### SQL Execution + Results
Run active SQL from VS Code and inspect results in a dedicated panel. Export CSV and trigger chart hooks from result sets.

### Local Data Work DuckDB
Use a workspace-local DuckDB file for scratch, caching, and medallion-style layering (`bronze -> silver -> gold`).

### Notebook Pipelines
Create `*.dpnb` notebooks with `analyze` and `transform` cells. Transform cells materialize named outputs into target layers.

### Lineage + ERD
Generate lineage and ERD artifacts as JSON files under `DP/system/` for traceability, code review, and onboarding.

### Connections + Introspection
Manage DuckDB/Postgres/MySQL connections, introspect schemas, and persist snapshots for autocomplete and reviewable project context.

### Optional AI
Generate query/notebook docs and inline SQL comments with configurable providers (or skip AI entirely).

## Positioning Snippet (vs dbt-core)

Duck Piper is the local development loop for SQL and DuckDB pipelines in VS Code.  
dbt-core remains the warehouse-first governance and deployment layer.  
Use Duck Piper to iterate quickly; promote stable logic into downstream production frameworks as needed.

## Suggested Keywords

`duckdb, sql, vscode, data pipeline, analytics engineering, lineage, erd, notebook, postgres, mysql`

## Release Notes Template (Marketplace)

Use this for each release entry.

```md
## Duck Piper vX.Y.Z

### Added
- ...

### Improved
- ...

### Fixed
- ...

### Docs
- ...

### Notes
- Offline-first behavior remains default.
- AI features remain optional and provider-configurable.
```

## v0.0.1 Release Notes Draft

```md
## Duck Piper v0.0.1

### Added
- SQL execution with dedicated results panel.
- Connection management and schema introspection (DuckDB, PostgreSQL, MySQL).
- Workspace-local Data Work DuckDB cache.
- Custom transformation notebooks (`*.dpnb`) with analyze/transform cells.
- Lineage generation and lineage visualization.
- ERD generation and ERD artifact persistence.
- Query history and saved query indexing.
- Optional AI helpers for Markdown docs and inline SQL comments.

### Docs
- New getting-started, feature, configuration, adapters, AI, pipelines, ERD, troubleshooting, and security docs.
```

## Screenshot Captions (Marketplace)

- `quickstart-run-sql.png`: Run SQL and inspect results in VS Code
- `quickstart-cache-local-duckdb.png`: Cache results into local Data Work DuckDB
- `quickstart-notebook-lineage.png`: Build notebook pipelines and view lineage

