import * as vscode from 'vscode';
import { loadSchemas } from '../schema/schemaStore';
import { SchemaIntrospection, TableModel } from '../core/types';

export class DPCompletionProvider implements vscode.CompletionItemProvider {

    constructor(private getConnectionId: (doc: vscode.TextDocument) => string | undefined) { }


    async provideCompletionItems(
        document: vscode.TextDocument,
        position: vscode.Position,
        _token: vscode.CancellationToken,
        _context: vscode.CompletionContext
    ): Promise<vscode.CompletionItem[]> {

        // 1. Load schemas
        let schemas = await loadSchemas();

        // Filter by active connection
        const connectionId = this.getConnectionId(document);
        if (connectionId) {
            // Keep schemas for the active connection OR the managed local data schemas
            schemas = schemas.filter(s =>
                s.connectionId === connectionId ||
                s.connectionId === 'duck-piper-local-data-work'
            );
        }

        // 2. Analyze context - get ALL text before cursor, not just current line
        const textBeforeCursor = document.getText(new vscode.Range(new vscode.Position(0, 0), position));
        const linePrefix = document.lineAt(position).text.substr(0, position.character);

        // CASE A: Triggered by dot '.' -> Column completion (for alias or table)
        if (linePrefix.trim().endsWith('.')) {
            return this.provideColumnCompletions(linePrefix, schemas);
        }

        // CASE B: Keyword Context Heuristic - look at ALL text before cursor
        const keyword = this.getLastKeyword(textBeforeCursor);
        if (keyword) {
            if (['FROM', 'JOIN', 'UPDATE', 'INTO'].includes(keyword)) {
                return this.provideTableCompletions(schemas);
            }
            if (['SELECT', 'WHERE', 'HAVING', 'SET', 'BY', 'AND', 'OR'].includes(keyword)) {
                return this.provideAllColumnCompletions(schemas);
            }
        }

        return [];
    }

    private getLastKeyword(textBeforeCursor: string): string | undefined {
        // Look at all text before cursor to find the last SQL keyword
        // This handles multi-line SQL properly

        // Simple tokenizer: replace parens/commas/newlines with space, split by whitespace
        const tokens = textBeforeCursor.replace(/[\(\),\n\r]/g, ' ').trim().split(/\s+/);

        // Iterate backwards to find the last SQL keyword
        for (let i = tokens.length - 1; i >= 0; i--) {
            const word = tokens[i].toUpperCase();
            if (['SELECT', 'FROM', 'JOIN', 'WHERE', 'UPDATE', 'INTO', 'SET', 'HAVING', 'GROUP', 'ORDER', 'BY', 'AND', 'OR'].includes(word)) {
                return word;
            }
        }
        return undefined;
    }

    private provideTableCompletions(introspections: SchemaIntrospection[]): vscode.CompletionItem[] {
        const items: vscode.CompletionItem[] = [];
        const addedSchemas = new Set<string>();

        for (const intro of introspections) {
            for (const schema of intro.schemas) {
                // Add schema name as a completion item (so user can type "schema." to get tables)
                if (!addedSchemas.has(schema.name) && schema.name !== 'default' && schema.name !== 'public' && schema.name !== 'main') {
                    const schemaItem = new vscode.CompletionItem(schema.name, vscode.CompletionItemKind.Module);
                    const totalObjects = schema.tables.length + (schema.views?.length || 0);
                    schemaItem.detail = `Schema (${totalObjects} tables/views)`;
                    schemaItem.insertText = schema.name; // Just the schema name, user adds "."
                    items.push(schemaItem);
                    addedSchemas.add(schema.name);
                }

                for (const table of schema.tables) {
                    const item = new vscode.CompletionItem(table.name, vscode.CompletionItemKind.Class);
                    item.detail = `${schema.name}.${table.name} (${intro.connectionName || intro.connectionId})`;
                    items.push(item);

                    // Also add schema-qualified suggestion
                    if (schema.name !== 'default' && schema.name !== 'public') {
                        const qualItem = new vscode.CompletionItem(`${schema.name}.${table.name}`, vscode.CompletionItemKind.Class);
                        qualItem.detail = `Full path`;
                        items.push(qualItem);
                    }
                }

                for (const view of (schema.views || [])) {
                    const item = new vscode.CompletionItem(view.name, vscode.CompletionItemKind.Interface);
                    item.detail = `View: ${schema.name}.${view.name} (${intro.connectionName || intro.connectionId})`;
                    items.push(item);

                    if (schema.name !== 'default' && schema.name !== 'public') {
                        const qualItem = new vscode.CompletionItem(`${schema.name}.${view.name}`, vscode.CompletionItemKind.Interface);
                        qualItem.detail = `Full path (view)`;
                        items.push(qualItem);
                    }
                }
            }
        }
        return items;
    }

    private provideColumnCompletions(linePrefix: string, introspections: SchemaIntrospection[]): vscode.CompletionItem[] {
        const items: vscode.CompletionItem[] = [];

        // 1. Try specific match first (if we have a prefix match)
        const match = linePrefix.match(/([a-zA-Z0-9_]+)\.$/);

        if (match) {
            const aliasOrTable = match[1];

            for (const intro of introspections) {
                for (const schema of intro.schemas) {
                    // Check if alias matches table or view name directly
                    const table = schema.tables.find(t => t.name.toLowerCase() === aliasOrTable.toLowerCase())
                        || (schema.views || []).find(v => v.name.toLowerCase() === aliasOrTable.toLowerCase());
                    if (table) {
                        items.push(...this.columnsToItems(table));
                    }
                }
            }

            // 1b. Check if prefix matches a SCHEMA name (e.g. "data_cache.")
            for (const intro of introspections) {
                for (const schema of intro.schemas) {
                    if (schema.name.toLowerCase() === aliasOrTable.toLowerCase()) {
                        // Found schema match -> return Tables and Views
                        for (const table of schema.tables) {
                            const item = new vscode.CompletionItem(table.name, vscode.CompletionItemKind.Class);
                            item.detail = `Table in ${schema.name}`;
                            items.push(item);
                        }
                        for (const view of (schema.views || [])) {
                            const item = new vscode.CompletionItem(view.name, vscode.CompletionItemKind.Interface);
                            item.detail = `View in ${schema.name}`;
                            items.push(item);
                        }
                    }
                }
            }
        }

        // 2. ALWAYS include all tables (e.g. for "public." -> "users")
        items.push(...this.provideTableCompletions(introspections));

        // 3. ALWAYS include all columns (e.g. for "t." -> "col" even if alias resolution fails)
        items.push(...this.provideAllColumnCompletions(introspections));

        return items;
    }

    private provideAllColumnCompletions(introspections: SchemaIntrospection[]): vscode.CompletionItem[] {
        const items: vscode.CompletionItem[] = [];
        for (const intro of introspections) {
            for (const schema of intro.schemas) {
                for (const table of schema.tables) {
                    for (const c of table.columns) {
                        const item = new vscode.CompletionItem(c.name, vscode.CompletionItemKind.Field);
                        item.detail = `${c.type} (${table.name})`;
                        items.push(item);
                    }
                }
                for (const view of (schema.views || [])) {
                    for (const c of view.columns) {
                        const item = new vscode.CompletionItem(c.name, vscode.CompletionItemKind.Field);
                        item.detail = `${c.type} (${view.name})`;
                        items.push(item);
                    }
                }
            }
        }
        return items;
    }

    private columnsToItems(table: TableModel): vscode.CompletionItem[] {
        return table.columns.map(c => {
            const item = new vscode.CompletionItem(c.name, vscode.CompletionItemKind.Field);
            item.detail = c.type;
            return item;
        });
    }
}
