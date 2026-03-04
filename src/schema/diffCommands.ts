import * as vscode from 'vscode';
import { SchemaIntrospection } from '../core/types';
import { loadSchemas } from './schemaStore';
import { loadConnectionProfiles } from '../connections/connectionStore';
import { buildDiffUri } from './diffProvider';
import { setCompareSourceSet, setCompareSourceKind } from '../core/context';

// --- Schema tree node shape (used by compare commands) ---

interface SchemaTreeNode {
  contextValue?: string;
  connectionId?: string;
  introspection?: SchemaIntrospection;
  schemaName?: string;
  schemaModel?: { name: string };
  table?: { name: string };
  label?: string;
}

// --- Compare selection state ---

interface CompareSource {
  kind: 'schema' | 'table' | 'view';
  connectionId: string;
  connectionName: string;
  schemaName: string;
  tableName?: string;
}

let compareSource: CompareSource | undefined;
let statusBarItem: vscode.StatusBarItem | undefined;

// --- Registration ---

export function registerSchemaDiffCommands(
  context: vscode.ExtensionContext,
  _explorerProvider?: unknown
): void {
  // Status bar
  statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 900);
  statusBarItem.command = 'dp.schema.clearCompareSelection';
  statusBarItem.tooltip = 'Click to clear compare selection';
  context.subscriptions.push(statusBarItem);

  // Commands
  context.subscriptions.push(
    vscode.commands.registerCommand('dp.schema.selectForCompare', selectForCompare),
    vscode.commands.registerCommand('dp.schema.compareWithSelected', compareWithSelected),
    vscode.commands.registerCommand('dp.schema.clearCompareSelection', clearCompareSelection),
    vscode.commands.registerCommand('dp.schema.compareSchemas', compareSchemasQuickPick),
    vscode.commands.registerCommand('dp.schema.compareTables', compareTablesQuickPick),
  );
}

// --- Select for Compare ---

async function selectForCompare(item?: SchemaTreeNode): Promise<void> {
  if (!item) return;

  const kind = resolveItemKind(item);
  if (!kind) {
    void vscode.window.showWarningMessage('Cannot compare this item.');
    return;
  }

  const connectionId = item.connectionId || item.introspection?.connectionId;
  const connectionName = item.introspection?.connectionName || connectionId || 'Unknown';
  const schemaName = item.schemaName || item.schemaModel?.name || '';
  const tableName = (kind === 'table' || kind === 'view') ? (item.table?.name || item.label) : undefined;

  if (!connectionId || !schemaName) {
    void vscode.window.showWarningMessage('Missing connection or schema information.');
    return;
  }

  compareSource = { kind, connectionId, connectionName, schemaName, tableName };
  await setCompareSourceSet(true);
  await setCompareSourceKind(kind);
  updateStatusBar();
}

// --- Compare with Selected ---

async function compareWithSelected(item?: SchemaTreeNode): Promise<void> {
  if (!item || !compareSource) return;

  const targetKind = resolveItemKind(item);
  if (!targetKind) {
    void vscode.window.showWarningMessage('Cannot compare this item.');
    return;
  }

  // Validate kind match: schema↔schema, table↔table, view↔view (table and view can compare with each other)
  const sourceIsTableLike = compareSource.kind === 'table' || compareSource.kind === 'view';
  const targetIsTableLike = targetKind === 'table' || targetKind === 'view';

  if (compareSource.kind === 'schema' && targetKind !== 'schema') {
    void vscode.window.showWarningMessage('Cannot compare a schema with a table/view. Select a schema to compare.');
    return;
  }
  if (sourceIsTableLike && !targetIsTableLike) {
    void vscode.window.showWarningMessage('Cannot compare a table/view with a schema. Select a table or view to compare.');
    return;
  }

  const targetConnectionId = item.connectionId || item.introspection?.connectionId;
  const targetConnectionName = item.introspection?.connectionName || targetConnectionId || 'Unknown';
  const targetSchemaName = item.schemaName || item.schemaModel?.name || '';
  const targetTableName = targetIsTableLike ? (item.table?.name || item.label) : undefined;

  if (!targetConnectionId || !targetSchemaName) {
    void vscode.window.showWarningMessage('Missing connection or schema information on target.');
    return;
  }

  // Build URIs and open diff
  const leftUri = buildDiffUri(
    compareSource.connectionId,
    compareSource.connectionName,
    compareSource.schemaName,
    compareSource.tableName
  );
  const rightUri = buildDiffUri(
    targetConnectionId,
    targetConnectionName,
    targetSchemaName,
    targetTableName
  );

  const leftLabel = compareSource.tableName
    ? `${compareSource.connectionName} / ${compareSource.schemaName}.${compareSource.tableName}`
    : `${compareSource.connectionName} / ${compareSource.schemaName}`;
  const rightLabel = targetTableName
    ? `${targetConnectionName} / ${targetSchemaName}.${targetTableName}`
    : `${targetConnectionName} / ${targetSchemaName}`;
  const title = `${leftLabel}  ↔  ${rightLabel}`;

  await vscode.commands.executeCommand('vscode.diff', leftUri, rightUri, title);
  await clearCompareSelection();
}

// --- Clear Compare Selection ---

async function clearCompareSelection(): Promise<void> {
  compareSource = undefined;
  await setCompareSourceSet(false);
  await setCompareSourceKind(undefined);
  if (statusBarItem) {
    statusBarItem.hide();
  }
}

// --- QuickPick: Compare Schemas ---

async function compareSchemasQuickPick(): Promise<void> {
  const allSchemas = await loadSchemas();
  const connections = await loadConnectionProfiles();
  const userConnections = connections.filter(c => !c.isLocalDuckPiperCacheDb);

  // Build flat list of connection > schema entries
  interface SchemaPickItem extends vscode.QuickPickItem {
    connectionId: string;
    connectionName: string;
    schemaName: string;
  }

  const items: SchemaPickItem[] = [];
  for (const conn of userConnections) {
    const intro = allSchemas.find(s => s.connectionId === conn.id);
    if (!intro || !intro.schemas.length) continue;
    for (const schema of intro.schemas) {
      items.push({
        label: `${conn.name} > ${schema.name}`,
        description: intro.dialect,
        connectionId: conn.id,
        connectionName: conn.name,
        schemaName: schema.name,
      });
    }
  }

  if (items.length < 2) {
    void vscode.window.showInformationMessage('Need at least two introspected schemas to compare.');
    return;
  }

  const left = await vscode.window.showQuickPick(items, { placeHolder: 'Select the first schema' });
  if (!left) return;

  const rightItems = items.filter(i => !(i.connectionId === left.connectionId && i.schemaName === left.schemaName));
  const right = await vscode.window.showQuickPick(rightItems, { placeHolder: 'Select the second schema' });
  if (!right) return;

  const leftUri = buildDiffUri(left.connectionId, left.connectionName, left.schemaName);
  const rightUri = buildDiffUri(right.connectionId, right.connectionName, right.schemaName);
  const title = `${left.connectionName} / ${left.schemaName}  ↔  ${right.connectionName} / ${right.schemaName}`;
  await vscode.commands.executeCommand('vscode.diff', leftUri, rightUri, title);
}

// --- QuickPick: Compare Tables ---

async function compareTablesQuickPick(): Promise<void> {
  const allSchemas = await loadSchemas();
  const connections = await loadConnectionProfiles();
  const userConnections = connections.filter(c => !c.isLocalDuckPiperCacheDb);

  interface TablePickItem extends vscode.QuickPickItem {
    connectionId: string;
    connectionName: string;
    schemaName: string;
    tableName: string;
  }

  const items: TablePickItem[] = [];
  for (const conn of userConnections) {
    const intro = allSchemas.find(s => s.connectionId === conn.id);
    if (!intro) continue;
    for (const schema of intro.schemas) {
      for (const table of schema.tables || []) {
        items.push({
          label: `${conn.name} > ${schema.name} > ${table.name}`,
          description: intro.dialect,
          connectionId: conn.id,
          connectionName: conn.name,
          schemaName: schema.name,
          tableName: table.name,
        });
      }
      for (const view of schema.views || []) {
        items.push({
          label: `${conn.name} > ${schema.name} > ${view.name}`,
          description: `${intro.dialect} (view)`,
          connectionId: conn.id,
          connectionName: conn.name,
          schemaName: schema.name,
          tableName: view.name,
        });
      }
    }
  }

  if (items.length < 2) {
    void vscode.window.showInformationMessage('Need at least two introspected tables to compare.');
    return;
  }

  const left = await vscode.window.showQuickPick(items, { placeHolder: 'Select the first table' });
  if (!left) return;

  const rightItems = items.filter(i =>
    !(i.connectionId === left.connectionId && i.schemaName === left.schemaName && i.tableName === left.tableName)
  );
  const right = await vscode.window.showQuickPick(rightItems, { placeHolder: 'Select the second table' });
  if (!right) return;

  const leftUri = buildDiffUri(left.connectionId, left.connectionName, left.schemaName, left.tableName);
  const rightUri = buildDiffUri(right.connectionId, right.connectionName, right.schemaName, right.tableName);
  const title = `${left.connectionName} / ${left.schemaName}.${left.tableName}  ↔  ${right.connectionName} / ${right.schemaName}.${right.tableName}`;
  await vscode.commands.executeCommand('vscode.diff', leftUri, rightUri, title);
}

// --- Helpers ---

function resolveItemKind(item: SchemaTreeNode): 'schema' | 'table' | 'view' | undefined {
  const ctx: string | undefined = item.contextValue;
  if (!ctx) return undefined;

  if (ctx === 'dp.schema.schema') return 'schema';
  if (ctx === 'dp.schema.view') return 'view';
  if (ctx.startsWith('dp.schema.table')) return 'table'; // covers table, table.local, table.reserved
  return undefined;
}

function updateStatusBar(): void {
  if (!statusBarItem || !compareSource) return;

  const label = compareSource.tableName
    ? `${compareSource.connectionName} / ${compareSource.schemaName}.${compareSource.tableName}`
    : `${compareSource.connectionName} / ${compareSource.schemaName}`;

  statusBarItem.text = `$(git-compare) Compare: ${label}`;
  statusBarItem.show();
}
