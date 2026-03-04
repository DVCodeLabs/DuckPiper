# Configuration Guide

Duck Piper settings use the `dp.` prefix in VS Code.

## Query and Results

### `dp.query.maxRowsLimit`
- Type: number
- Default: `1000`
- Description: hard LIMIT for rows returned by SELECT queries. Set `0` to disable.

## Local DuckDB

### `dp.duckdb.path`
- Type: string
- Default: `dp/system/piper.duckdb`
- Description: path to local Data Work DuckDB, relative to workspace root.

### `dp.transform.previewRows`
- Type: number
- Default: `200`
- Description: row preview count after transformation runs.

## AI

### `dp.ai.provider`
- Type: string enum
- Values: `none`, `vscode`, `openai`, `anthropic`, `azureOpenAI`, `ollama`, `openaiCompatible`
- Default: `vscode`

### `dp.ai.model`
- Type: string
- Default: `gpt-4o`
- Description: provider-specific model identifier.

### `dp.ai.endpoint`
- Type: string
- Default: empty
- Description: custom endpoint for compatible or self-hosted providers.

### `dp.ai.sendSchemaContext`
- Type: boolean
- Default: `true`
- Description: include schema/metadata context in prompts.

### `dp.ai.maxSchemaChars`
- Type: number
- Default: `150000`
- Description: approximate schema context character cap.

## Formatting

### `dp.format.enabled`
- Type: boolean
- Default: `true`

### `dp.format.indentSize`
- Type: number
- Default: `2`

### `dp.format.keywordCase`
- Type: string enum
- Values: `upper`, `lower`, `preserve`
- Default: `upper`

### `dp.format.dialectFallback`
- Type: string enum
- Values: `postgresql`, `mysql`, `sql`
- Default: `sql`

## System Behaviors

### `dp.system.overwriteByDefault`
- Type: boolean
- Default: `true`

### `dp.system.maxRows`
- Type: number
- Default: `100000`

## SQL CodeLens

### `dp.sqlCodelens.enabled`
- Type: boolean
- Default: `true`
- Description: display Duck Piper actions and connection selector in SQL files.

