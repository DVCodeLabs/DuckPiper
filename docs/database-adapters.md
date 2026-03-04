# Database Adapters

Duck Piper supports multiple database dialects via adapters.

## Adapter Status

| Adapter | Status | Notes |
| --- | --- | --- |
| DuckDB | Wired | Full local-first path including Data Work workflows |
| PostgreSQL | Wired | Connection + query + introspection flow |
| MySQL | Wired | Connection + query + introspection flow |
| Snowflake | External | Supported via optional connector extension (`duckpiper-snowflake`) |

## DuckDB

- Best path for local/offline workflows
- Supports local file path (`dp.duckdb.path`)
- Used by Data Work cache and notebook transform outputs

## PostgreSQL

- Use for remote operational/warehouse query workflows
- Supports test connection and schema introspection

## MySQL

- Same operational flow as Postgres within the extension
- Supports introspection and table-level workflows

## Snowflake

- Snowflake support is provided by the optional `duckpiper-snowflake` connector extension.
- Core DuckPiper now exposes a provider/adapter API so external connectors can register schemas and runtime adapters consistently.

## Typical Workflow

1. Add connection
2. Test connection
3. Introspect schema
4. Run SQL and inspect results
5. Save/copy data into local DuckDB for offline transforms
