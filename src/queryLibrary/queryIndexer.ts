import * as vscode from "vscode";
import { canonicalizeSql } from "../core/hashing";
import { ensureDPDirs, writeJson } from "../core/fsWorkspace";

export interface QueryIndexFile {
  version: "0.1";
  generatedAt: string;
  queries: QueryIndexEntry[];
}

export interface QueryIndexEntry {
  path: string;       // workspace-relative
  docPath?: string;   // companion markdown relative path
  title?: string;     // first comment line if present
  sqlHash: string;
  tables?: string[];  // optional best-effort later
  createdAt: string;  // ISO - when the query was first indexed
  updatedAt: string;  // ISO - when the query was last modified
  connectionId?: string | null;
  connectionName?: string | null;
  dialect?: string | null;
  lastRunAt?: string | null;

  // Search metadata (derived from companion markdown)
  mdTitle?: string;
  mdTags?: string[];
  mdSummary?: string;          // first meaningful paragraph/line from notes
  mdBodyText?: string;         // plain text, normalized
  searchText?: string;         // concatenated normalized field for fast contains match
  searchUpdatedAt?: string;    // last time search fields were derived
}

const EXCLUDE_GLOB =
  "**/{node_modules,dist,out,.git,DP/system}/**";

export async function rebuildQueryIndex(): Promise<void> {
  const dpDir = await ensureDPDirs();
  const indexUri = vscode.Uri.joinPath(dpDir, "system", "queries", "queryIndex.json");

  // VS Code findFiles exclude pattern doesn't need wrapping braces if it's a single string with braces inside
  const sqlFiles = await vscode.workspace.findFiles("**/*.sql", EXCLUDE_GLOB);

  const entries: QueryIndexEntry[] = [];

  for (const file of sqlFiles) {
    const doc = await vscode.workspace.openTextDocument(file);
    const text = doc.getText();
    const { sqlHash } = canonicalizeSql(text);
    const title = extractTitle(text);

    const wsRelative = vscode.workspace.asRelativePath(file, false);
    const stat = await vscode.workspace.fs.stat(file);

    // Check for companion markdown
    let docPath: string | undefined;
    let connectionId: string | undefined;
    const mdPath = file.path.replace(/\.sql$/i, '.md');
    try {
      const mdUri = file.with({ path: mdPath });
      const mdStat = await vscode.workspace.fs.stat(mdUri);
      if (mdStat) {
        docPath = wsRelative.replace(/\.sql$/i, '.md');
        // Read connection ID from frontmatter
        const mdBytes = await vscode.workspace.fs.readFile(mdUri);
        const mdContent = Buffer.from(mdBytes).toString('utf8');
        const match = mdContent.match(/^connection_id:\s*"?(.*?)"?$/m);
        if (match) {
          connectionId = match[1];
        } else {
          // Fallback: try to resolve connection name? (Too risky without index)
          // Just leave undefined, user will select manually.
        }
      }
    } catch {
      // No companion doc
    }

    entries.push({
      path: wsRelative,
      docPath,
      title,
      sqlHash,
      createdAt: new Date(stat.ctime).toISOString(),
      updatedAt: new Date(stat.mtime).toISOString(),
      connectionId
    });
  }

  const index: QueryIndexFile = {
    version: "0.1",
    generatedAt: new Date().toISOString(),
    queries: entries
  };

  await writeJson(indexUri, index);
}

function extractTitle(sql: string): string | undefined {
  // Title heuristic: first non-empty line that is a comment
  const lines = sql.split(/\r?\n/);
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;
    if (line.startsWith("--")) return line.replace(/^--\s?/, "").trim() || undefined;
    if (line.startsWith("/*")) return undefined; // keep v0 simple
    return undefined;
  }
  return undefined;
}
