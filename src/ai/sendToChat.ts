import * as vscode from 'vscode';
import { loadConnectionProfiles } from '../connections/connectionStore';
import { buildSchemaContext } from './schemaContext';
import { loadPromptTemplate, renderPrompt } from './prompts';

// ---------------------------------------------------------------------------
// Core helper – copies prompt to clipboard and notifies the user
// ---------------------------------------------------------------------------

async function sendPromptToChat(prompt: string): Promise<void> {
    await vscode.env.clipboard.writeText(prompt);
    vscode.window.showInformationMessage('Prompt copied to clipboard. Paste into your AI chat tool.');
}

// ---------------------------------------------------------------------------
// Connection helper (shared across SQL-oriented tasks)
// ---------------------------------------------------------------------------

async function resolveConnectionInfo(
    context: vscode.ExtensionContext,
    doc: vscode.TextDocument
): Promise<{ connectionId?: string; connectionName: string; dialect: string }> {
    const docKey = doc.uri.toString();
    const docConnections = context.workspaceState.get<Record<string, string>>('dp.docConnections.v1', {});
    const docConnId = docConnections[docKey];
    const activeId = context.workspaceState.get<string>('dp.activeConnectionId');
    const connectionId = docConnId || activeId;

    let connectionName = 'none';
    let dialect = 'unknown';

    if (connectionId) {
        const profiles = await loadConnectionProfiles();
        const profile = profiles.find(p => p.id === connectionId);
        if (profile) {
            connectionName = profile.name;
            dialect = profile.dialect;
        }
    }

    return { connectionId, connectionName, dialect };
}

// ---------------------------------------------------------------------------
// Send to Chat: Inline Comments
// ---------------------------------------------------------------------------

export async function sendCommentToChat(context: vscode.ExtensionContext): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showWarningMessage('No active editor.');
        return;
    }

    const sqlText = editor.document.getText();
    if (!sqlText.trim()) {
        vscode.window.showWarningMessage('File is empty.');
        return;
    }

    const { connectionName, dialect, connectionId } = await resolveConnectionInfo(context, editor.document);
    const schemaContext = await buildSchemaContext(sqlText, connectionId);

    const promptTemplate = await loadPromptTemplate('inlineComments');
    const prompt = renderPrompt(promptTemplate, {
        sql: sqlText,
        dialect,
        connection: connectionName,
        schemaContext: schemaContext ? `Schema context:\n${schemaContext}` : ''
    });

    await sendPromptToChat(prompt);
}

// ---------------------------------------------------------------------------
// Send to Chat: SQL Markdown Documentation
// ---------------------------------------------------------------------------

export async function sendDocumentToChat(context: vscode.ExtensionContext): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showWarningMessage('No active editor.');
        return;
    }

    const sqlText = editor.document.getText();
    if (!sqlText.trim()) {
        vscode.window.showWarningMessage('File is empty.');
        return;
    }

    const { connectionName, dialect, connectionId } = await resolveConnectionInfo(context, editor.document);
    const schemaContext = await buildSchemaContext(sqlText, connectionId);

    const promptTemplate = await loadPromptTemplate('markdownDoc');
    const prompt = renderPrompt(promptTemplate, {
        sql: sqlText,
        dialect,
        connection: connectionName,
        schemaContext: schemaContext ? `Schema context:\n${schemaContext}` : ''
    });

    await sendPromptToChat(prompt);
}

// ---------------------------------------------------------------------------
// Send to Chat: Notebook Documentation (DuckPiper only)
// ---------------------------------------------------------------------------

export async function sendNotebookDocumentToChat(_context: vscode.ExtensionContext): Promise<void> {
    const editor = vscode.window.activeNotebookEditor;
    const textEditor = vscode.window.activeTextEditor;
    const textDocNotebook = textEditor && (textEditor.document as unknown as { notebook?: vscode.NotebookDocument }).notebook;

    if (!editor && !textDocNotebook) {
        vscode.window.showWarningMessage('Open a notebook to send documentation prompt to chat.');
        return;
    }

    const notebook = editor?.notebook || textDocNotebook;
    if (!notebook) return;

    if (notebook.cellCount === 0) {
        vscode.window.showWarningMessage('Notebook is empty.');
        return;
    }

    const cells = notebook.getCells();
    const cellContents: string[] = [];

    for (const cell of cells) {
        if (cell.kind === vscode.NotebookCellKind.Code) {
            const sql = cell.document.getText();
            const outputName = cell.metadata?.dp?.outputName || 'unknown_table';
            const layer = cell.metadata?.dp?.layer || 'raw';
            cellContents.push(`### Step ${cell.index + 1}: ${layer}.${outputName}\n\`\`\`sql\n${sql}\n\`\`\``);
        } else {
            cellContents.push(`> Context: ${cell.document.getText()}`);
        }
    }

    const joinedContent = cellContents.join('\n\n');

    const promptTemplate = await loadPromptTemplate('notebookMarkdownDoc');
    const prompt = renderPrompt(promptTemplate, {
        cellContent: joinedContent
    });

    await sendPromptToChat(prompt);
}

// ---------------------------------------------------------------------------
// Send to Chat: Schema Descriptions
// ---------------------------------------------------------------------------

export async function sendSchemaDescriptionsToChat(
    _context: vscode.ExtensionContext,
    item?: { introspection?: { schemas: Array<{ name: string; tables: Array<{ name: string; columns: Array<{ name: string; type: string; comment?: string }> }>; views?: Array<{ name: string; columns: Array<{ name: string; type: string; comment?: string }> }> }>; connectionName?: string; connectionId: string }; schemaModel?: { name: string }; contextValue?: string; schemaName?: string }
): Promise<void> {
    let introspection = item?.introspection;

    // Case B: Data Work Item (Local DuckDB)
    if (!introspection && item && (item.contextValue?.startsWith('dp.duckdb') || item.schemaName)) {
        const connectionId = 'duck-piper-local-data-work';
        const { loadSchemas } = require('../schema/schemaStore');
        const allSchemas = await loadSchemas();
        introspection = allSchemas.find((s: { connectionId: string }) => s.connectionId === connectionId);
    }

    if (!introspection) {
        vscode.window.showWarningMessage('No schema selected. Invoke from the Schemas view context menu.');
        return;
    }

    let schemas = introspection.schemas;
    if (item?.schemaModel?.name) {
        schemas = schemas.filter(s => s.name === item.schemaModel!.name);
    }

    if (!schemas || schemas.length === 0) {
        vscode.window.showWarningMessage('No schemas found in introspection.');
        return;
    }

    const template = await loadPromptTemplate('describeSchema');
    const prompts: string[] = [];

    for (const schema of schemas) {
        const allTables = [...schema.tables, ...(schema.views || [])];
        for (const table of allTables) {
            const columns = table.columns.map(c => `- ${c.name} (${c.type}): ${c.comment || ''}`).join('\n');
            prompts.push(renderPrompt(template, {
                schemaName: schema.name,
                tableName: table.name,
                columns,
                tableConstraint: '',
                columnsConstraint: ''
            }));
        }
    }

    if (prompts.length === 0) {
        vscode.window.showWarningMessage('No tables found to generate descriptions for.');
        return;
    }

    // Copy the first table prompt to clipboard; user can re-invoke for subsequent tables
    await vscode.env.clipboard.writeText(prompts[0]);

    const tableCount = prompts.length;
    if (tableCount === 1) {
        vscode.window.showInformationMessage(
            'Prompt copied to clipboard. Paste into your AI chat tool.'
        );
    } else {
        vscode.window.showInformationMessage(
            `Prompt copied to clipboard (table 1 of ${tableCount}). Paste into your AI chat tool.`
        );
    }
}
