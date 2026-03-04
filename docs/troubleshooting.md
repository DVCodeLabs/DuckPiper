# Troubleshooting

## Installation Issues

### Extension not loading
- Confirm VS Code version meets engine requirement (`^1.96.0`).
- Reload VS Code window after install/update.

### Commands not visible
- Ensure a workspace folder is open.
- Open command palette and search `DP:` commands.

## Connection Issues

### Test connection fails
- Verify host/port/database/user credentials.
- Check network access for remote DBs.
- Retry with minimal connection options.

### Introspection fails
- Validate DB permissions for metadata queries.
- Retry connection test, then introspection.
- Check Output/Developer console logs.

## Query Execution Issues

### Query returns too few rows
- Check `dp.query.maxRowsLimit`.
- Use run-without-limit command path when appropriate.

### Results panel is empty
- Ensure query completed successfully.
- Re-run query in the active SQL editor.

## Local DuckDB / Pipeline Issues

### Local DB path problems
- Check `dp.duckdb.path` setting.
- Ensure workspace folder is writable.

### Notebook transform fails
- Verify transform cell has valid layer/output metadata.
- Confirm SQL is compatible with DuckDB.
- Re-run after clearing invalid output names.

### Lineage not updating
- Run transform cells (analyze-only cells do not materialize lineage outputs).
- Refresh pipeline views.

## AI Issues

### No model/provider available
- Set `dp.ai.provider` and `dp.ai.model`.
- For local providers, verify endpoint and local model availability.
- For hosted providers, verify credentials and network access.

### Unexpected AI output
- Inspect and tune templates under `DP/system/prompts/`.
- Reduce or adjust schema context settings if prompts are too large.

## Where to Get Help

- Open an issue for reproducible bugs.
- Use discussions for questions/workflow guidance.
- Report vulnerabilities privately via `SECURITY.md`.

