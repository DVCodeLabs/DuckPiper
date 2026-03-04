# Feature Highlights

## SQL Execution + Results

- Run active SQL quickly from the editor
- Inspect tabular output in a dedicated panel
- Export CSV from results
- Use chart generation hooks from query output

## Connections + Introspection

- Add, edit, test, remove, and select connections
- Introspect schema metadata into JSON artifacts in `DP/schemas/`
- Use introspection for completion and diagram generation

## Local Data Work DuckDB

- Workspace-local DuckDB for scratch and caching
- Designed for quick iteration and reproducibility
- Supports medallion-style organization (`bronze`, `silver`, `gold`)

## Transformation Notebooks (`*.dpnb`)

- Custom notebook type focused on SQL transforms
- Cell mode can be `analyze` or `transform`
- Transform cells materialize named outputs into selected layers

## Lineage

- Transform runs produce lineage JSON under `DP/system/pipelines/`
- Lineage view visualizes table/column flow
- Artifacts are commit-friendly for review and onboarding

## ERD

- Generate ERD from active connection/schema introspection
- Save ERD JSON under `DP/system/erd/`

## Query Memory + Similarity

- Query history view for quick reopen
- Similar-query detection helps avoid duplicate work
- SQL files can include companion Markdown docs

## Optional AI Helpers

- Generate query and notebook docs
- Add inline SQL comments
- Choose provider based on your environment and policy

