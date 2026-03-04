# Duck Piper

[![Tests](https://github.com/duckpiper/duckpiper/actions/workflows/test.yml/badge.svg)](https://github.com/duckpiper/duckpiper/actions/workflows/test.yml)
![Version](https://img.shields.io/badge/version-0.0.1-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](./LICENSE)
[![VS Code](https://img.shields.io/badge/vscode-%5E1.96.0-007ACC)](https://code.visualstudio.com/)

Local-first SQL, DuckDB notebooks, and pipelines in VS Code.

## Project Overview

Your SQL workbench, notebook, and pipeline builder, inside VS Code.

- Run SQL with a dedicated results panel
- Manage and introspect connections (DuckDB, Postgres, MySQL)
- Cache data into a workspace-local DuckDB (`dp/system/piper.duckdb` by default)
- Build transformation pipelines with custom notebooks (`*.dpnb`)
- Generate lineage and ERD artifacts as JSON you can commit
- Optionally generate docs/comments with AI providers you control

Duck Piper is offline-first by default. External DB connections and hosted AI providers require network access when used.

## Key Features

### SQL Execution + Results UI
- Run active SQL with a keybinding (`Shift+Cmd+R` on macOS in SQL editors)
- View tabular results in the `DP: Results` panel
- Export CSV and trigger chart generation hooks

### Local “Data Work” DuckDB
- Workspace-local DuckDB for scratch, cache, and medallion-style layers
- Default layer model: `bronze -> silver -> gold`
- Keep fast iteration loops even when disconnected

### Pipelines + Lineage
- Custom notebook type (`*.dpnb`) with `analyze` and `transform` cell modes
- Transform runs materialize outputs and update lineage JSON
- Visual lineage view for onboarding and review

### Connections + Introspection
- Add/test/select connections and introspect schemas
- Persist schema snapshots as JSON in `DP/schemas/`
- Use introspection for autocomplete and ERD generation

### Optional AI Integration
- Generate companion Markdown docs and inline SQL comments
- Use VS Code LM API, OpenAI, Anthropic, Azure OpenAI, Ollama, or OpenAI-compatible endpoints
- Keep AI optional; extension works without it

## Installation

### VS Code Marketplace
- Search for `Duck Piper` in the Extensions view and install.

### CLI (after Marketplace publish)
```bash
code --install-extension runql.duckpiper
```

### Manual VSIX
- Build a platform-specific package and install via:
```bash
npm ci
npx vsce package --target darwin-arm64  # use your platform
code --install-extension duckpiper-darwin-arm64-0.0.1.vsix
```

## Quick Start

### 1. Open a workspace folder
Duck Piper initializes a `DP/` structure for queries, notebooks, schemas, and system artifacts.

### 2. Run your first SQL query
- Create/open a `.sql` file
- Select a connection
- Run query with `Shift+Cmd+R`

![Run SQL and inspect results](media/marketplace/screenshots/quickstart-run-sql.png)

### 3. Cache results to local DuckDB
- Save query/table data into the local Data Work DB for offline iteration.

![Cache to workspace DuckDB](media/marketplace/screenshots/quickstart-cache-local-duckdb.png)

### 4. Build a notebook pipeline
- Create a `*.dpnb` notebook
- Set transform cell layer/output
- Run cells and open lineage view

![Notebook pipeline and lineage](media/marketplace/screenshots/quickstart-notebook-lineage.png)

## Feature Highlights

### ERD
- Generate ERDs for active connections or specific schemas
- Save ERD artifacts under `DP/system/erd/*.erd.json`

### Pipelines
- Use notebooks as in-repo pipeline assets
- Track outputs and lineage under `DP/system/pipelines/`

### AI
- Keep prompt templates in `DP/system/prompts/`
- Generate query/notebook docs and inline comments when desired

### Multi-Database Support
- DuckDB, Postgres, and MySQL adapters are wired in core
- Additional providers can be added through external connector extensions (for example, `duckpiper-snowflake`)

## Configuration Guide

Common settings (VS Code settings key prefix: `dp.`):

- `dp.query.maxRowsLimit`: hard result limit for SELECT queries (`0` disables limit)
- `dp.duckdb.path`: local DuckDB file path relative to workspace root
- `dp.transform.previewRows`: row count preview in transform runs
- `dp.ai.provider`: `none|vscode|openai|anthropic|azureOpenAI|ollama|openaiCompatible`
- `dp.ai.model`: provider-specific model selection
- `dp.ai.endpoint`: custom endpoint for local/self-hosted providers
- `dp.ai.sendSchemaContext`: include schema context in AI prompts
- `dp.format.enabled`: enable SQL formatting

Full reference: [`docs/configuration.md`](docs/configuration.md)

## Documentation

- [`docs/getting-started.md`](docs/getting-started.md)
- [`docs/features.md`](docs/features.md)
- [`docs/pipelines-notebooks.md`](docs/pipelines-notebooks.md)
- [`docs/erd-guide.md`](docs/erd-guide.md)
- [`docs/database-adapters.md`](docs/database-adapters.md)
- [`docs/ai-providers.md`](docs/ai-providers.md)
- [`docs/troubleshooting.md`](docs/troubleshooting.md)
- [`docs/security.md`](docs/security.md)

## Contributing

Contributions are welcome. Please read:

- [`CONTRIBUTING.md`](CONTRIBUTING.md)
- [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)
- [`SECURITY.md`](SECURITY.md)

## License

This project is licensed under the MIT License. See [`LICENSE`](LICENSE).
