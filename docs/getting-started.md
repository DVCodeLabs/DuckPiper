# Getting Started

This guide walks through first use of Duck Piper in a normal folder-based project.

## 1) Install

Install from VS Code Marketplace or via CLI:

```bash
code --install-extension duckpiper.duckpiper
```

## 2) Open a Workspace Folder

Duck Piper initializes a `DP/` folder structure for:

- `DP/queries/`
- `DP/notebooks/`
- `DP/schemas/`
- `DP/system/`

These artifacts are designed to be committed and reviewed with your project.

## 3) Run Your First Query

1. Create or open a `.sql` file
2. Select a connection
3. Run `DP: Run Query` (default keybinding: `Shift+Cmd+R` in SQL editors)
4. Review results in `DP: Results`

![Run SQL in editor and inspect results](../media/marketplace/screenshots/quickstart-run-sql.png)

## 4) Cache Data Locally

Use Duck Piper commands to save results/table data into the local Data Work DuckDB for offline iteration.

![Cache query output into local DuckDB](../media/marketplace/screenshots/quickstart-cache-local-duckdb.png)

## 5) Create a Notebook Pipeline

1. Run `DP: Create Transformation Notebook`
2. Add SQL cells
3. Set cell mode:
   - `analyze` for read-only queries
   - `transform` to materialize outputs into medallion layers
4. Run notebook cells
5. Open lineage view

![Notebook pipeline and lineage flow](../media/marketplace/screenshots/quickstart-notebook-lineage.png)

## 6) Generate ERD

Run `DP: View ERD (Active Connection)` or `DP: View ERD (Selected Schema)` to render schema structure and save ERD artifacts.

## Offline Notes

Duck Piper is offline-first by default. You can do local analysis with workspace DuckDB without external services. External connections and hosted AI providers require network when used.
