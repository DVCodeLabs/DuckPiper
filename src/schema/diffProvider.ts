import * as vscode from 'vscode';
import { loadSchemas } from './schemaStore';
import { normalizeSchemaForDiff, normalizeTableForDiff } from './diffNormalize';

/**
 * TextDocumentContentProvider for the `dp-diff:` URI scheme.
 *
 * URI format:
 *   dp-diff:///ConnectionName/schemaName?connectionId=xxx          (schema diff)
 *   dp-diff:///ConnectionName/schemaName/tableName?connectionId=xxx (table diff)
 */
export class SchemaDiffContentProvider implements vscode.TextDocumentContentProvider {

  private _onDidChange = new vscode.EventEmitter<vscode.Uri>();
  readonly onDidChange = this._onDidChange.event;

  /** Force refresh of a virtual document (e.g. after re-introspection). */
  refresh(uri: vscode.Uri): void {
    this._onDidChange.fire(uri);
  }

  async provideTextDocumentContent(uri: vscode.Uri): Promise<string> {
    const params = new URLSearchParams(uri.query);
    const connectionId = params.get('connectionId');
    if (!connectionId) {
      return '// Error: missing connectionId in URI';
    }

    // Path segments: /ConnectionName/schemaName  or  /ConnectionName/schemaName/tableName
    const segments = uri.path.split('/').filter(Boolean);
    if (segments.length < 2) {
      return '// Error: URI must include at least connectionName and schemaName';
    }

    const schemaName = decodeURIComponent(segments[1]);
    const tableName = segments.length >= 3 ? decodeURIComponent(segments[2]) : undefined;

    // Load schemas from cache
    const allSchemas = await loadSchemas();
    const introspection = allSchemas.find(s => s.connectionId === connectionId);
    if (!introspection) {
      return `// Error: no introspection found for connectionId "${connectionId}"`;
    }

    const schemaModel = introspection.schemas.find(s => s.name === schemaName);
    if (!schemaModel) {
      return `// Error: schema "${schemaName}" not found in connection`;
    }

    if (tableName) {
      // Table-level or view-level diff
      const table =
        (schemaModel.tables || []).find(t => t.name === tableName) ||
        (schemaModel.views || []).find(v => v.name === tableName);

      if (!table) {
        return `// Error: table/view "${tableName}" not found in schema "${schemaName}"`;
      }

      return JSON.stringify(normalizeTableForDiff(table), null, 2);
    }

    // Schema-level diff
    return JSON.stringify(normalizeSchemaForDiff(schemaModel), null, 2);
  }
}

// --- URI builders ---

export function buildDiffUri(
  connectionId: string,
  connectionName: string,
  schemaName: string,
  tableName?: string
): vscode.Uri {
  const safeName = encodeURIComponent(connectionName);
  const safeSchema = encodeURIComponent(schemaName);
  const path = tableName
    ? `/${safeName}/${safeSchema}/${encodeURIComponent(tableName)}`
    : `/${safeName}/${safeSchema}`;

  return vscode.Uri.parse(`dp-diff://${path}?connectionId=${encodeURIComponent(connectionId)}`);
}
