
import * as vscode from 'vscode';
import { Logger } from './logger';

export async function ensureDPDirs(): Promise<vscode.Uri> {
  const folders = vscode.workspace.workspaceFolders;
  if (!folders) {
    throw new Error('No workspace folder open.');
  }
  const root = folders[0].uri;
  const dpDir = vscode.Uri.joinPath(root, 'DP');

  // Create DP directory (ignore if already exists)
  try {
    await vscode.workspace.fs.createDirectory(dpDir);
  } catch {
    // Directory already exists - this is expected and safe to ignore
  }

  // Create subdirs (ignore if already exist)
  const subs = ['schemas', 'queries', 'notebooks', 'system', 'system/pipelines', 'system/erd', 'system/prompts'];
  for (const s of subs) {
    try {
      await vscode.workspace.fs.createDirectory(vscode.Uri.joinPath(dpDir, s));
    } catch {
      // Directory already exists - this is expected and safe to ignore
    }
  }

  return dpDir;
}

export async function fileExists(uri: vscode.Uri): Promise<boolean> {
  try {
    await vscode.workspace.fs.stat(uri);
    return true;
  } catch {
    return false;
  }
}

export async function readJson<T>(uri: vscode.Uri): Promise<T> {
  const bytes = await vscode.workspace.fs.readFile(uri);
  const text = new TextDecoder().decode(bytes);
  return JSON.parse(text);
}

export async function writeJson(uri: vscode.Uri, data: unknown): Promise<void> {
  const text = JSON.stringify(data, null, 2);
  const bytes = new TextEncoder().encode(text);
  await vscode.workspace.fs.writeFile(uri, bytes);
}

export async function listFiles(dir: vscode.Uri): Promise<string[]> {
  try {
    const entries = await vscode.workspace.fs.readDirectory(dir);
    // entries is [name, type][]
    return entries.map(([name, _type]) => name);
  } catch (e) {
    Logger.warn(`listFiles failed for ${dir.toString()}`, e);
    return [];
  }
}

/**
 * Ensure AGENTS.md exists in the project root.
 * If AGENTS.md already exists, create AGENTS_DP.md instead.
 */
export async function ensureAgentsMd(): Promise<void> {
  const folders = vscode.workspace.workspaceFolders;
  if (!folders) return;

  const root = folders[0].uri;
  const agentsUri = vscode.Uri.joinPath(root, 'AGENTS.md');
  const fallbackUri = vscode.Uri.joinPath(root, 'AGENTS_DP.md');

  // If both files exist, nothing to do
  if (await fileExists(agentsUri)) {
    if (await fileExists(fallbackUri)) return;
    // AGENTS.md exists but not ours — use fallback name
    const bytes = new TextEncoder().encode(agentsContent());
    await vscode.workspace.fs.writeFile(fallbackUri, bytes);
    return;
  }

  const bytes = new TextEncoder().encode(agentsContent());
  await vscode.workspace.fs.writeFile(agentsUri, bytes);
}

function agentsContent(): string {
  return `# Agent Guidance

This repo stores SQL, schemas, notebooks, ERDs, and lineage metadata in known locations. When a user asks for SQL or a pipeline, always look for existing artifacts first, then use schema and documentation to build something new only if needed.

## Source Locations

- Existing queries: \`DP/queries/\` (may include subdirectories).
- Query index: \`DP/system/queryIndex.json\` (this is auto updated when a query is saved)
- Schemas and descriptions: \`DP/schemas/\` (these are auto create when a connection is added)
- Notebooks (existing pipelines): \`DP/notebooks/\` (may include subdirectories).
- Pipeline Lineage JSON files: \`DP/system/pipelines/\` (this gets auto updated when a notebook is saved)
- ERD files: \`DP/system/erd/\` (these get auto created when a user clicks view ERD)

## Required Workflow (SQL Queries)

1. Search for existing queries first.
   - Check \`DP/queries/\` (including subdirectories) and \`DP/system/queryIndex.json\`.
2. If nothing relevant exists, read the schema and docs.
   - Use \`DP/schemas/\` for table/column definitions, relationships (if available) and descriptions.
   - Use \`DP/system/erd/\` (if populated) and \`DP/system/pipelines/\` to understand joins, relationships, and lineage.
3. Only then should you create a new SQL query file (.sql)
   - Prefer to reuse or extend existing patterns when possible.
   - Do NOT create any other DP files when creating sql - only create .sql files

## Required Workflow (Pipelines)

1. Search for existing pipelines first.
   - Check \`DP/notebooks/\` and \`DP/system/pipelines/\`.
2. If nothing relevant exists, read the schema and docs.
   - Use \`DP/schemas/\` for table/column definitions and descriptions.
   - Use \`DP/system/erd/\` and \`DP/system/pipelines/\` to understand joins, relationships, and lineage.
   - Use \`DP/queries/\` (including subdirectories) and \`DP/system/queryIndex.json\` for more context.
3. Only then should you create a new pipeline notebook file (.dpnb)
   - Prefer to reuse or extend existing patterns when possible.
   - Do NOT create any other DuckPiper files (lineage, indexes, documentation) - only create .dpnb files.
4. Pipeline rules (DuckDB bronze / silver / gold).
   - Pipelines use built-in DuckDB databases: bronze, silver, and gold.
   - Build stepwise: read from existing bronze table/view → transform to silver → transform to gold.
   - Do not try to perform all transformations in a single step.
   - Do not use CREATE, CREATE OR REPLACE, or any other write statements.
   - Each pipeline cell must contain only a SELECT (or WITH … SELECT) statement.
5. Pipeline Destination Layer and Table.
   - For pipelines, add a comment at the end of the SQL in each cell in the following format:
   " -- Destination Info: " with the destination layer and destination table name.
6. Pipeline DuckDB
   - Do NOT run any DuckDB command.  DuckDB is built-in.

## Required Workflow (Documentation Requests)

1. SQL Query Documentation:
   - If a user asks you to document an SQL query, follow the prompt in \`DP/system/prompts/markdownDoc.txt\`.
   - Output the file in the exact same directory as the query (\`DP/queries/\`) with the same name but a different extension.
   - Example: \`olympic_gold.sql\` -> \`olympic_gold.md\`.
2. Notebook Documentation:
   - If a user asks you to document a notebook (pipeline), follow the prompt in \`DP/system/prompts/notebookMarkdownDoc.txt\`.
   - Output the file in the exact same directory as the notebook (\`DP/notebooks/\`) with the same name but a different extension.
   - Example: \`olympic_medals_pipeline.dpnb\` -> \`olympic_medals_pipeline.md\`.
3. Schema Description:
   - If a user asks you to describe a schema, follow the prompt in \`DP/system/prompts/describeSchema.txt\`.
   - Output the results to \`DP/schemas/\` with the same name as the connection but a different extension.
   - Example: \`olympics_db.json\` -> \`olympics_db.description.json\`.
4. Inline Comments:
   - If a user asks you to create inline comments on an SQL file, follow the prompt in \`DP/system/prompts/inlineComments.txt\`.

## Notes

- If an existing query/pipeline partially answers the request, adapt it rather than starting from scratch.
- Keep outputs consistent with the repository's established conventions and naming.
`;
}

/**
 * Ensure README_DP.md exists in the project root with project setup instructions.
 */
export async function ensureReadmeMd(): Promise<void> {
  const folders = vscode.workspace.workspaceFolders;
  if (!folders) return;

  const root = folders[0].uri;
  const readmeUri = vscode.Uri.joinPath(root, 'README_DP.md');

  if (await fileExists(readmeUri)) return;

  const content = `# DuckPiper Project

This project uses DuckPiper for data pipelines and queries.

## Setup

1. **Git Configuration**:
   The \`DP/system/\` directory contains generated system files and indices that usually do not need to be committed to version control, unless you want to share lineage and index metadata.

   Recommended \`.gitignore\` entry:
   \`\`\`gitignore
   DP/system/
   \`\`\`

   *Note: \`DP/queries/\`, \`DP/schemas/\`, and \`DP/notebooks/\` SHOULD be committed as they contain your source artifacts.*

## Folder Structure

- **DP/queries/**: Saved SQL queries.
- **DP/notebooks/**: Data pipelines (DuckPiper Notebooks).
- **DP/schemas/**: Schema definitions and descriptions.
- **DP/system/**: generated indexes, ERD data, pipeline lineage (optional to commit).
`;

  const bytes = new TextEncoder().encode(content);
  await vscode.workspace.fs.writeFile(readmeUri, bytes);
}
