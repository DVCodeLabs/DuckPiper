import * as vscode from 'vscode';
import { LocalDuckDB } from './localDuckDB';
import { getConnection } from '../connections/connectionStore';
import { ConnectionProfile } from '../core/types';
import { isProjectInitialized } from '../core/isProjectInitialized';

export class LocalDuckDBViewProvider implements vscode.TreeDataProvider<DuckDBItem> {
    private _onDidChangeTreeData: vscode.EventEmitter<DuckDBItem | undefined | null | void> = new vscode.EventEmitter<DuckDBItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<DuckDBItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor(private context: vscode.ExtensionContext) { }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: DuckDBItem): vscode.TreeItem {
        return element;
    }

    async getChildren(element?: DuckDBItem): Promise<DuckDBItem[]> {
        if (!element) {
            // Check initialization first
            if (!(await isProjectInitialized())) {
                return [new DuckDBItem("Not initialized", vscode.TreeItemCollapsibleState.None)];
            }

            // Root: Duck Piper Connection Node
            const profile = await getConnection('duck-piper-local-data-work');
            if (profile) {
                return [new LocalConnectionItem(profile)];
            } else {
                // Fallback if profile missing (shouldn't happen usually)
                return [new DuckDBItem("Duck_Piper (Not Found)", vscode.TreeItemCollapsibleState.None)];
            }
        }

        if (element instanceof LocalConnectionItem) {
            const showSystem = this.context.workspaceState.get<boolean>('dp.ui.showSystemSchemas', false);

            // Children of Connection: Groups
            const groups = [
                new GroupItem('Inputs', ['imports', 'data_cache']),
                new GroupItem('Pipeline', ['bronze', 'silver', 'gold'])
            ];

            if (showSystem) {
                groups.push(new GroupItem('System', ['dp_app'], vscode.TreeItemCollapsibleState.Collapsed));
            }

            return groups;
        } else if (element instanceof GroupItem) {
            // Children of Group: Schemas
            return element.schemas.map(s => new SchemaItem(s, vscode.TreeItemCollapsibleState.Collapsed, this.context.extensionUri));
        } else if (element instanceof SchemaItem) {
            // Children of Schema: Tables
            try {
                const tables = await LocalDuckDB.getInstance().listTables(element.schemaName);
                if (tables.length === 0) {
                    return [new DuckDBItem("No tables", vscode.TreeItemCollapsibleState.None)];
                }
                return tables.map(t => new TableItem(element.schemaName, t.name, t.type));
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                return [new DuckDBItem(`Error: ${msg}`, vscode.TreeItemCollapsibleState.None)];
            }
        } else if (element instanceof TableItem) {
            // Children of Table: Columns
            try {
                const columns = await LocalDuckDB.getInstance().listColumns(element.schemaName, element.tableName);
                if (columns.length === 0) {
                    return [new DuckDBItem("No columns", vscode.TreeItemCollapsibleState.None)];
                }
                return columns.map((c: { name: string; type: string }) => new ColumnItem(element.schemaName, element.tableName, c.name, c.type));
            } catch (e: unknown) {
                const msg = e instanceof Error ? e.message : String(e);
                return [new DuckDBItem(`Error: ${msg}`, vscode.TreeItemCollapsibleState.None)];
            }
        }
        return [];
    }
}

export class DuckDBItem extends vscode.TreeItem { }

export class LocalConnectionItem extends DuckDBItem {
    constructor(public readonly profile: ConnectionProfile) {
        super(profile.name, vscode.TreeItemCollapsibleState.Expanded);
        this.contextValue = 'dp.connection.item.locked'; // Use locked context to allow "Refresh" etc if generic commands support it
        this.iconPath = new vscode.ThemeIcon('plug');
        this.description = "Local DuckDB instance";
    }
}

export class GroupItem extends DuckDBItem {
    constructor(
        public readonly label: string,
        public readonly schemas: string[],
        public readonly collapsibleState: vscode.TreeItemCollapsibleState = vscode.TreeItemCollapsibleState.Expanded
    ) {
        super(label, collapsibleState);
        this.contextValue = 'group';
        this.iconPath = new vscode.ThemeIcon('folder');
    }
}

export class SchemaItem extends DuckDBItem {
    constructor(
        public readonly schemaName: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        extensionUri: vscode.Uri
    ) {
        super(schemaName, collapsibleState);

        // Set context value for menu actions
        if (schemaName === 'imports') {
            this.contextValue = 'dp.duckdb.schema.imports';
        } else if (['data_cache', 'bronze', 'silver', 'gold'].includes(schemaName)) {
            this.contextValue = 'dp.duckdb.schema.layer';
        } else {
            this.contextValue = 'dp.duckdb.schema';
        }

        if (schemaName === 'data_cache') {
            const iconDark = vscode.Uri.joinPath(extensionUri, 'media', 'icons', 'lucide', 'dark', 'database-zap.svg');
            const iconLight = vscode.Uri.joinPath(extensionUri, 'media', 'icons', 'lucide', 'light', 'database-zap.svg');
            this.iconPath = { light: iconLight, dark: iconDark };
        } else {
            const iconDark = vscode.Uri.joinPath(extensionUri, 'media', 'icons', 'lucide', 'dark', 'database.svg');
            const iconLight = vscode.Uri.joinPath(extensionUri, 'media', 'icons', 'lucide', 'light', 'database.svg');
            this.iconPath = { light: iconLight, dark: iconDark };
        }

        if (schemaName === 'dp_app') {
            this.description = '(system)';
        } else if (['bronze', 'silver', 'gold'].includes(schemaName)) {
            this.description = '(layer)';
        } else if (schemaName === 'data_cache') {
            this.description = '(warehouse or db extract cache)';
        } else if (schemaName === 'imports') {
            this.description = '(raw file imports)';
        } else {
            this.description = '';
        }

        this.command = {
            command: 'dp.schema.select',
            title: 'Select Schema',
            arguments: [schemaName]
        };
    }
}

export class TableItem extends DuckDBItem {
    constructor(
        public readonly schemaName: string,
        public readonly tableName: string,
        public readonly type: string
    ) {
        super(tableName, vscode.TreeItemCollapsibleState.Collapsed);

        // Set context value for menu actions
        if (['imports', 'data_cache'].includes(schemaName)) {
            this.contextValue = 'dp.duckdb.table.import';
        } else if (['bronze', 'silver', 'gold'].includes(schemaName)) {
            this.contextValue = 'dp.duckdb.table.layer';
        } else {
            this.contextValue = 'dp.duckdb.table';
        }

        this.iconPath = new vscode.ThemeIcon(type === 'VIEW' ? 'eye' : 'table');
        this.tooltip = `${schemaName}.${tableName}`;

        this.command = {
            command: 'dp.editor.insertText',
            title: 'Insert Table',
            arguments: [tableName]
        };
    }
}

export class ColumnItem extends DuckDBItem {
    constructor(
        public readonly schemaName: string,
        public readonly tableName: string,
        public readonly columnName: string,
        public readonly columnType: string
    ) {
        super(columnName, vscode.TreeItemCollapsibleState.None);
        this.contextValue = 'dp.duckdb.column';
        this.description = columnType;
        this.iconPath = new vscode.ThemeIcon('symbol-field');
        this.tooltip = `${schemaName}.${tableName}.${columnName}`;

        this.command = {
            command: 'dp.editor.insertText',
            title: 'Insert Column',
            arguments: [columnName]
        };
    }
}
