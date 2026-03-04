# Pipeline and Notebook Guide

Duck Piper uses a custom notebook format (`*.dpnb`) to model SQL pipelines.

## Notebook Model

Each SQL cell is either:

- `analyze`: read-only query
- `transform`: materialized output into a target layer/table

For transform cells, set:

- Target layer (`bronze`, `silver`, `gold`)
- Output name (snake_case recommended)

## Typical Medallion Flow

1. Ingest raw data into `bronze`
2. Clean/standardize in `silver`
3. Build consumption-ready marts in `gold`

## Execution Behavior

- Notebook runs execute SQL through the local Data Work DuckDB path.
- Transform outputs are materialized into the selected layer.
- Preview output is shown after execution.

## Lineage Generation

After transform runs:

- Lineage JSON is generated under `DP/system/pipelines/`
- Lineage graph can be opened from notebook/pipeline commands
- Artifacts can be committed for review and reproducibility

## Documentation Workflow

For each notebook, you can generate a companion Markdown doc:

- `my_pipeline.dpnb`
- `my_pipeline.md`

This keeps intent and implementation coupled in-repo.

