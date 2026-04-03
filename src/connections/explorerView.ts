import * as vscode from "vscode";
import { ConnectionProfile, SchemaIntrospection, SchemaModel, TableModel, ColumnModel, RoutineModel, ForeignKeyModel, IndexModel } from "../core/types";
import { loadConnectionProfiles, getConnection } from "./connectionStore";
import { loadSchemas } from "../schema/schemaStore";
import { loadDescriptions } from "../schema/descriptionStore";
import { isProjectInitialized } from "../core/isProjectInitialized";
import { quoteIdentifier } from "../core/sqlUtils";

type ExplorerFolderKind = "tables" | "views" | "procedures" | "functions";
type TableDetailFolderKind = "columns" | "keys" | "foreignKeys" | "indexes";

export class ExplorerViewProvider implements vscode.TreeDataProvider<ExplorerItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<ExplorerItem | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(private context: vscode.ExtensionContext) { }

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: ExplorerItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: ExplorerItem): Promise<ExplorerItem[]> {
    // Top Level: Connections or Welcome
    if (!element) {
      // Check if project is initialized first
      const initialized = await isProjectInitialized();
      if (!initialized) {
        return [ExplorerItem.welcomeItem()];
      }

      const connections = await loadConnectionProfiles();
      const userConnections = connections.filter(c => !c.isLocalDuckPiperCacheDb);

      if (userConnections.length === 0) {
        return [
          new ExplorerItem(
            "No connections yet",
            undefined,
            undefined,
            "Add a connection to get started.",
            true,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            'placeholder-no-conns'
          )
        ];
      }

      // Return Connection Items
      return userConnections.map((c) => ExplorerItem.fromProfile(c, this._activeId));
    }

    // Level 2: Schemas (Children of Connection)
    if (element.profile) {
      // Load schemas for this connection
      const allSchemas = await loadSchemas();
      const intro = allSchemas.find(s => s.connectionId === element.profile!.id);

      if (!intro) {
        // Not introspected yet -> Show placeholder action
        return [ExplorerItem.fromEmptyConnection(element.profile)];
      }

      // Show Schemas
      const schemas = intro.schemas;
      if (schemas.length === 0) return [];

      const showInternal = this.context.workspaceState.get<boolean>('dp.ui.showSystemSchemas', false);
      const INTERNAL_SCHEMAS = ['dp_app', 'information_schema', 'pg_catalog'];

      return schemas
        .filter(s => showInternal || !INTERNAL_SCHEMAS.includes(s.name))
        .map(s => ExplorerItem.fromSchemaModel(s, intro, element.isLocalDuckDB || false, this.context.extensionUri, element.profile!.allowCsvExport ?? true));
    }

    // Level 3: Folders (Children of Schema)
    if (element.schemaModel && element.introspection && !element.folderKind) {
      const showRoutines = vscode.workspace.getConfiguration('dp').get<boolean>('ui.showRoutines', true);
      const procedures = element.schemaModel.procedures || [];
      const functions = element.schemaModel.functions || [];
      const items: ExplorerItem[] = [];

      if ((element.schemaModel.tables || []).length > 0) {
        items.push(ExplorerItem.fromSchemaFolder("tables", element.schemaModel, element.introspection, element.isLocalDuckDB || false));
      }
      if ((element.schemaModel.views || []).length > 0) {
        items.push(ExplorerItem.fromSchemaFolder("views", element.schemaModel, element.introspection, element.isLocalDuckDB || false));
      }
      if (showRoutines && procedures.length > 0) {
        items.push(ExplorerItem.fromSchemaFolder("procedures", element.schemaModel, element.introspection, element.isLocalDuckDB || false));
      }
      if (showRoutines && functions.length > 0) {
        items.push(ExplorerItem.fromSchemaFolder("functions", element.schemaModel, element.introspection, element.isLocalDuckDB || false));
      }

      return items;
    }

    // Level 4: Tables or Routines (Children of schema folder)
    if (element.folderKind && element.schemaModel && element.introspection) {
      const introspection = element.introspection;
      const schemaName = element.schemaModel.name;

      if (element.folderKind === "tables") {
        const safeName = getSafeName(introspection);
        const descriptions = await loadDescriptions(safeName);
        const profile = await getConnection(introspection.connectionId);
        const allowCsvExport = profile?.allowCsvExport ?? true;
        return element.schemaModel.tables.map(t => {
          const tableKey = `${schemaName}.${t.name}`;
          const desc = descriptions?.tables?.[tableKey]?.description;
          return ExplorerItem.fromTable(t, schemaName, introspection, element.isLocalDuckDB || false, desc, allowCsvExport);
        });
      }

      if (element.folderKind === "views") {
        const safeName = getSafeName(introspection);
        const descriptions = await loadDescriptions(safeName);
        return (element.schemaModel.views || []).map(v => {
          const viewKey = `${schemaName}.${v.name}`;
          const desc = descriptions?.tables?.[viewKey]?.description;
          return ExplorerItem.fromView(v, schemaName, introspection, element.isLocalDuckDB || false, desc);
        });
      }

      const showRoutineParameters = vscode.workspace.getConfiguration('dp').get<boolean>('ui.showRoutineParameters', true);
      const routines = element.folderKind === "procedures"
        ? (element.schemaModel.procedures || [])
        : (element.schemaModel.functions || []);
      return routines.map((routine) =>
        ExplorerItem.fromRoutine(routine, schemaName, introspection, element.isLocalDuckDB || false, showRoutineParameters)
      );
    }

    // Level 5: Table Detail Folders (Children of Table)
    if (element.table && element.introspection && !element.tableDetailKind) {
      const table = element.table;
      const schemaName = element.schemaName || element.introspection.schemas[0].name;
      const items: ExplorerItem[] = [];

      // columns — always present
      if (table.columns.length > 0) {
        items.push(ExplorerItem.fromTableDetailFolder("columns", table, schemaName, element.introspection, element.isLocalDuckDB || false));
      }
      // keys — only if primary key exists
      if (table.primaryKey && table.primaryKey.length > 0) {
        items.push(ExplorerItem.fromTableDetailFolder("keys", table, schemaName, element.introspection, element.isLocalDuckDB || false));
      }
      // foreign keys
      if (table.foreignKeys && table.foreignKeys.length > 0) {
        items.push(ExplorerItem.fromTableDetailFolder("foreignKeys", table, schemaName, element.introspection, element.isLocalDuckDB || false));
      }
      // indexes
      if (table.indexes && table.indexes.length > 0) {
        items.push(ExplorerItem.fromTableDetailFolder("indexes", table, schemaName, element.introspection, element.isLocalDuckDB || false));
      }

      return items;
    }

    // Level 6: Table Detail Folder Children
    if (element.tableDetailKind && element.table && element.introspection) {
      const introspection = element.introspection;
      const table = element.table;
      const schemaName = element.schemaName || introspection.schemas[0].name;

      if (element.tableDetailKind === "columns") {
        const safeName = getSafeName(introspection);
        const descriptions = await loadDescriptions(safeName);
        const tableKey = `${schemaName}.${table.name}`;
        return table.columns.map(c => {
          const colKey = `${tableKey}.${c.name}`;
          const desc = descriptions?.columns?.[colKey]?.description;
          return ExplorerItem.fromColumn(c, introspection, table, schemaName, desc);
        });
      }

      if (element.tableDetailKind === "keys") {
        return table.primaryKey && table.primaryKey.length > 0
          ? [ExplorerItem.fromPrimaryKey(table, introspection, schemaName)]
          : [];
      }

      if (element.tableDetailKind === "foreignKeys") {
        return (table.foreignKeys || []).map(fk =>
          ExplorerItem.fromForeignKeyItem(fk, table, introspection, schemaName)
        );
      }

      if (element.tableDetailKind === "indexes") {
        return (table.indexes || []).map(idx =>
          ExplorerItem.fromIndexItem(idx, table, introspection, schemaName)
        );
      }
    }

    return [];
  }

  private _activeId?: string;
  setActiveId(id: string | undefined) {
    this._activeId = id;
    this.refresh();
  }
}

export class ExplorerItem extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly profile?: ConnectionProfile,
    public readonly introspection?: SchemaIntrospection,
    tooltip?: string,
    isPlaceholder?: boolean,
    collapsibleState?: vscode.TreeItemCollapsibleState,
    // Schema Logic properties
    public readonly schemaModel?: SchemaModel,
    public readonly table?: TableModel,
    public readonly column?: ColumnModel,
    public readonly routine?: RoutineModel,
    public readonly folderKind?: ExplorerFolderKind,
    public readonly schemaName?: string,
    public readonly connectionId?: string,
    public readonly isLocalDuckDB?: boolean,
    public readonly forcedId?: string,
    public readonly tableDetailKind?: TableDetailFolderKind
  ) {
    super(label, collapsibleState ?? vscode.TreeItemCollapsibleState.None);
    this.tooltip = tooltip;
    if (forcedId) {
      this.id = forcedId;
    }

    // Default contexts
    if (isPlaceholder) {
      this.contextValue = "dp.connection.placeholder";
    }
  }

  // --- FACTORY METHODS ---

  // 0. Welcome Item (shown when project is not initialized)
  static welcomeItem(): ExplorerItem {
    const item = new ExplorerItem(
      "Get Started with DuckPiper",
      undefined,
      undefined,
      "Open Welcome to initialize this workspace and configure DuckPiper.",
      false,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      'dp-welcome-item'
    );
    item.iconPath = new vscode.ThemeIcon("rocket");
    item.contextValue = "dp.welcome.item";
    item.command = {
      command: "dp.welcome.open",
      title: "Open Welcome"
    };
    return item;
  }

  // 1. Connection Node
  static fromProfile(p: ConnectionProfile, activeId?: string): ExplorerItem {
    const isActive = p.id === activeId;
    const label = `${p.name}${isActive ? ' (Active)' : ''}`;
    const dialect = p.dialect || String(((p as unknown) as Record<string, unknown>).type ?? '?');
    const description = `${dialect}${p.database ? ` • ${p.database}` : ""}${p.host ? ` • ${p.host}` : ""}`;

    // Collapsible to show schemas
    const isLocalDuckDB = p.dialect === 'duckdb' && !p.host; // Treat as local if no host (file based) or specific check
    const item = new ExplorerItem(
      label,
      p,
      undefined,
      description,
      false,
      vscode.TreeItemCollapsibleState.Collapsed,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      p.id,
      isLocalDuckDB,
      p.id
    );
    item.description = description;

    // Command to select as active on click is mostly redundant if we just expand, 
    // but useful to set active context.
    // However, TreeView "click" usually toggles expansion. 
    // We can add a command to the item, but that might override expansion behavior depending on VSCode version.
    // Let's keep the "Select Connection" command but maybe make it auxiliary?
    // Actually, widespread usage puts selection on click.
    item.command = {
      command: "dp.connection.select",
      title: "Select Connection",
      arguments: [p]
    };

    item.iconPath = new vscode.ThemeIcon(isActive ? "pass-filled" : "plug");

    // Legacy locked check
    if (p.id === 'duck-piper-local-data-work') {
      item.contextValue = 'dp.connection.item.locked';
    } else {
      item.contextValue = 'dp.connection.item';
    }

    return item;
  }

  // 2. Empty Connection Placeholder (Introspection Nudge)
  static fromEmptyConnection(c: ConnectionProfile): ExplorerItem {
    const label = "Introspect to see schemas...";
    // Placeholder with command
    const item = new ExplorerItem(
      label,
      undefined,
      undefined,
      "Click to introspect",
      true,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      c.id,
      undefined,
      c.id + '-placeholder'
    );
    item.contextValue = "dp.connection.placeholder"; // Or reuse connection item context to allow introspect?
    // Actually we want a specific button.
    item.command = {
      command: "dp.connection.introspect",
      title: "Introspect Schema",
      arguments: [{ profile: c }]
    };
    item.iconPath = new vscode.ThemeIcon("database", new vscode.ThemeColor("disabledForeground"));
    return item;
  }

  // 3. Schema Node (Merged from SchemaItem)
  static fromSchemaModel(s: SchemaModel, introspection: SchemaIntrospection, isLocalConnection = false, extensionUri?: vscode.Uri, allowCsvExport: boolean = true): ExplorerItem {
    const shouldExpand = s.name === 'imports' || s.name === 'data_cache';
    const state = shouldExpand ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed;
    const nodeId = `${introspection.connectionId}/${s.name}`;

    // Note: we pass introspection and schemaModel
    // profile check: introspection object has connectionId, do we need full profile object? 
    // getChildren uses element.profile to detect root connection. 
    // element.schemaModel for schema.
    const item = new ExplorerItem(
      s.name,
      undefined,
      introspection,
      undefined,
      false,
      state,
      s,
      undefined,
      undefined,
      undefined,
      undefined,
      s.name,
      introspection.connectionId,
      isLocalConnection,
      nodeId
    );

    if (extensionUri) {
      const iconDark = vscode.Uri.joinPath(extensionUri, 'media', 'icons', 'lucide', 'dark', 'database.svg');
      const iconLight = vscode.Uri.joinPath(extensionUri, 'media', 'icons', 'lucide', 'light', 'database.svg');
      item.iconPath = { light: iconLight, dark: iconDark };
    } else {
      item.iconPath = new vscode.ThemeIcon("symbol-folder");
    }

    // Context value matches package.json expectations (renamed to explorerView)
    item.contextValue = allowCsvExport === false ? "dp.schema.schema.nobackup" : "dp.schema.schema";

    return item;
  }

  // 3b. Schema Folder Nodes
  static fromSchemaFolder(
    kind: ExplorerFolderKind,
    schemaModel: SchemaModel,
    introspection: SchemaIntrospection,
    isLocalConnection = false
  ): ExplorerItem {
    const label = kind === "tables" ? "Tables" : kind === "views" ? "Views" : kind === "procedures" ? "Procedures" : "Functions";
    const nodeId = `${introspection.connectionId}/${schemaModel.name}/${kind}`;
    const item = new ExplorerItem(
      label,
      undefined,
      introspection,
      undefined,
      false,
      vscode.TreeItemCollapsibleState.Collapsed,
      schemaModel,
      undefined,
      undefined,
      undefined,
      kind,
      schemaModel.name,
      introspection.connectionId,
      isLocalConnection,
      nodeId
    );

    if (kind === "tables") {
      item.iconPath = new vscode.ThemeIcon("symbol-class");
      item.contextValue = "dp.schema.folder.tables";
    } else if (kind === "views") {
      item.iconPath = new vscode.ThemeIcon("eye");
      item.contextValue = "dp.schema.folder.views";
    } else if (kind === "procedures") {
      item.iconPath = new vscode.ThemeIcon("symbol-method");
      item.contextValue = "dp.schema.folder.procedures";
    } else {
      item.iconPath = new vscode.ThemeIcon("symbol-function");
      item.contextValue = "dp.schema.folder.functions";
    }

    return item;
  }

  // 4. Table Node
  static fromTable(t: TableModel, schemaName: string, introspection: SchemaIntrospection, isLocalConnection = false, description?: string, allowCsvExport: boolean = true): ExplorerItem {
    const __schemaName = schemaName;
    const nodeId = `${introspection.connectionId}/${schemaName}/${t.name}`;
    const item = new ExplorerItem(
      t.name,
      undefined,
      introspection,
      description,
      false,
      vscode.TreeItemCollapsibleState.Collapsed,
      undefined,
      t,
      undefined,
      undefined,
      undefined,
      __schemaName,
      introspection.connectionId,
      isLocalConnection,
      nodeId
    );

    item.iconPath = new vscode.ThemeIcon("table");
    if (description) {
      item.tooltip = new vscode.MarkdownString(description);
    }

    const RESERVED = ['imports', 'data_cache', 'bronze', 'silver', 'gold', 'dp_app'];

    // Context values must match package.json checks for explorerView
    if (schemaName && RESERVED.includes(schemaName)) {
      item.contextValue = "dp.schema.table.reserved";
    } else if (isLocalConnection) {
      item.contextValue = "dp.schema.table.local";
    } else if (allowCsvExport === false) {
      item.contextValue = "dp.schema.table.noexport";
    } else {
      item.contextValue = "dp.schema.table";
    }

    // Click to insert name
    item.command = {
      command: "dp.editor.insertText",
      title: "Insert Table",
      arguments: [t.name]
    };
    return item;
  }

  // 4b. View Node
  static fromView(v: TableModel, schemaName: string, introspection: SchemaIntrospection, isLocalConnection = false, description?: string): ExplorerItem {
    const nodeId = `${introspection.connectionId}/${schemaName}/view/${v.name}`;
    const item = new ExplorerItem(
      v.name,
      undefined,
      introspection,
      description,
      false,
      vscode.TreeItemCollapsibleState.Collapsed,
      undefined,
      v,
      undefined,
      undefined,
      undefined,
      schemaName,
      introspection.connectionId,
      isLocalConnection,
      nodeId
    );

    item.iconPath = new vscode.ThemeIcon("eye");
    if (description) {
      item.tooltip = new vscode.MarkdownString(description);
    }

    item.contextValue = "dp.schema.view";

    item.command = {
      command: "dp.editor.insertText",
      title: "Insert View",
      arguments: [v.name]
    };
    return item;
  }

  // 5. Column Node
  static fromColumn(c: ColumnModel, introspection: SchemaIntrospection, table: TableModel, schemaName: string, description?: string): ExplorerItem {
    const label = `${c.name} : ${c.type}`;
    const nodeId = `${introspection.connectionId}/${schemaName}/${table.name}/${c.name}`;
    const item = new ExplorerItem(
      label,
      undefined,
      introspection,
      description,
      false,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      c,
      undefined,
      undefined,
      schemaName,
      introspection.connectionId,
      undefined,
      nodeId
    );
    item.iconPath = new vscode.ThemeIcon("symbol-field");
    item.contextValue = "dp.schema.column";

    if (description) {
      item.tooltip = new vscode.MarkdownString(description);
    }

    item.command = {
      command: "dp.editor.insertText",
      title: "Insert Column",
      arguments: [c.name]
    };
    return item;
  }

  // 6. Routine Node
  static fromRoutine(
    routine: RoutineModel,
    schemaName: string,
    introspection: SchemaIntrospection,
    isLocalConnection = false,
    showRoutineParameters = true
  ): ExplorerItem {
    const displayLabel = showRoutineParameters && routine.signature
      ? routine.signature
      : routine.name;
    const nodeId = `${introspection.connectionId}/${schemaName}/${routine.kind}/${routine.name}`;
    const item = new ExplorerItem(
      displayLabel,
      undefined,
      introspection,
      routine.comment,
      false,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      undefined,
      routine,
      undefined,
      schemaName,
      introspection.connectionId,
      isLocalConnection,
      nodeId
    );

    const identifier = formatQualifiedIdentifier(introspection, schemaName, routine.name);
    item.description = routine.returnType ? `→ ${routine.returnType}` : undefined;
    item.iconPath = new vscode.ThemeIcon(routine.kind === "procedure" ? "symbol-method" : "symbol-function");
    item.contextValue = routine.kind === "procedure" ? "dp.schema.procedure" : "dp.schema.function";
    item.command = {
      command: "dp.editor.insertText",
      title: "Insert Routine",
      arguments: [identifier]
    };

    if (routine.comment || routine.signature || routine.returnType) {
      const tooltip = new vscode.MarkdownString();
      tooltip.appendMarkdown(`**${identifier}**`);
      if (routine.signature) {
        tooltip.appendMarkdown(`\n\n\`${routine.signature}\``);
      }
      if (routine.returnType) {
        tooltip.appendMarkdown(`\n\nReturns: \`${routine.returnType}\``);
      }
      if (routine.comment) {
        tooltip.appendMarkdown(`\n\n${routine.comment}`);
      }
      item.tooltip = tooltip;
    }

    return item;
  }

  // 7. Table Detail Folder Node
  static fromTableDetailFolder(
    kind: TableDetailFolderKind,
    table: TableModel,
    schemaName: string,
    introspection: SchemaIntrospection,
    isLocalConnection = false
  ): ExplorerItem {
    const labelMap: Record<TableDetailFolderKind, string> = {
      columns: "columns",
      keys: "keys",
      foreignKeys: "foreign keys",
      indexes: "indexes",
    };
    const iconMap: Record<TableDetailFolderKind, string> = {
      columns: "symbol-field",
      keys: "key",
      foreignKeys: "references",
      indexes: "list-tree",
    };
    const countMap: Record<TableDetailFolderKind, number> = {
      columns: table.columns.length,
      keys: table.primaryKey?.length ? 1 : 0,
      foreignKeys: table.foreignKeys?.length ?? 0,
      indexes: table.indexes?.length ?? 0,
    };

    const nodeId = `${introspection.connectionId}/${schemaName}/${table.name}/${kind}`;
    const item = new ExplorerItem(
      labelMap[kind],
      undefined,
      introspection,
      undefined,
      false,
      vscode.TreeItemCollapsibleState.Collapsed,
      undefined,
      table,
      undefined,
      undefined,
      undefined,
      schemaName,
      introspection.connectionId,
      isLocalConnection,
      nodeId,
      kind
    );

    item.description = `${countMap[kind]}`;
    item.iconPath = new vscode.ThemeIcon(iconMap[kind]);
    item.contextValue = `dp.schema.table.${kind}`;

    return item;
  }

  // 8. Primary Key Item
  static fromPrimaryKey(
    table: TableModel,
    introspection: SchemaIntrospection,
    schemaName: string
  ): ExplorerItem {
    const cols = table.primaryKey || [];
    const nodeId = `${introspection.connectionId}/${schemaName}/${table.name}/pk/PRIMARY`;
    const item = new ExplorerItem(
      "PRIMARY",
      undefined,
      introspection,
      undefined,
      false,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      schemaName,
      introspection.connectionId,
      undefined,
      nodeId
    );

    item.description = `(${cols.join(', ')})`;
    item.iconPath = new vscode.ThemeIcon("key");
    item.contextValue = "dp.schema.primaryKey";

    return item;
  }

  // 9. Foreign Key Item
  static fromForeignKeyItem(
    fk: ForeignKeyModel,
    table: TableModel,
    introspection: SchemaIntrospection,
    schemaName: string
  ): ExplorerItem {
    const label = fk.name || `FK_${fk.column}`;
    const nodeId = `${introspection.connectionId}/${schemaName}/${table.name}/fk/${label}`;
    const item = new ExplorerItem(
      label,
      undefined,
      introspection,
      undefined,
      false,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      schemaName,
      introspection.connectionId,
      undefined,
      nodeId
    );

    item.description = `(${fk.column}) → ${fk.foreignTable} (${fk.foreignColumn})`;
    item.iconPath = new vscode.ThemeIcon("references");
    item.contextValue = "dp.schema.foreignKey";

    return item;
  }

  // 10. Index Item
  static fromIndexItem(
    idx: IndexModel,
    table: TableModel,
    introspection: SchemaIntrospection,
    schemaName: string
  ): ExplorerItem {
    const nodeId = `${introspection.connectionId}/${schemaName}/${table.name}/idx/${idx.name}`;
    const item = new ExplorerItem(
      idx.name,
      undefined,
      introspection,
      undefined,
      false,
      vscode.TreeItemCollapsibleState.None,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      schemaName,
      introspection.connectionId,
      undefined,
      nodeId
    );

    const uniqueSuffix = idx.unique ? ' UNIQUE' : '';
    item.description = `(${idx.columns.join(', ')})${uniqueSuffix}`;
    item.iconPath = new vscode.ThemeIcon("list-tree");
    item.contextValue = "dp.schema.index";

    return item;
  }
}

function getSafeName(introspection: SchemaIntrospection): string {
  return introspection.connectionName
    ? introspection.connectionName.replace(/[^a-z0-9_\-\.]/gi, '_')
    : introspection.connectionId;
}

function formatQualifiedIdentifier(introspection: SchemaIntrospection, schemaName: string | undefined, objectName: string): string {
  const dialect = introspection.dialect;
  const quotedObject = quoteIdentifier(dialect, objectName);
  if (!schemaName) {
    return quotedObject;
  }
  const schemaParts = schemaName.split('.').filter((part) => part.length > 0);
  const quotedSchema = schemaParts.map((part) => quoteIdentifier(dialect, part)).join('.');
  return `${quotedSchema}.${quotedObject}`;
}
