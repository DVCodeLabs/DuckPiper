import * as vscode from 'vscode';
import { SchemaIntrospection, TableModel } from '../core/types';
import { getConfiguredAIProvider, openAiProviderSettings } from '../ai/aiService';
import { loadPromptTemplate, renderPrompt } from '../ai/prompts';
import { SchemaDescriptionsFile, loadDescriptions, saveDescriptions } from './descriptionStore';
import { Logger } from '../core/logger';
import { ErrorHandler, ErrorSeverity, formatSchemaError, formatAIError } from '../core/errorHandler';

interface SchemaGeneratorItem {
    introspection?: SchemaIntrospection;
    schemaModel?: { name: string };
    contextValue?: string;
    schemaName?: string;
}

interface AIDescriptionResponse {
    schemaDescription?: string;
    table?: { description?: string };
    columns?: Array<{ key: string; description: string; sampleValue?: string }>;
}

export async function generateDescriptionsWithAI(context: vscode.ExtensionContext, item?: SchemaGeneratorItem) {
    // 1. Resolve Schema Introspection
    let introspection: SchemaIntrospection | undefined;
    let schemaNameFilter: string | undefined;

    // Case A: Standard Schema Item (has introspection)
    if (item && item.introspection) {
        introspection = item.introspection;
        // If specific schema selected (not root connection)
        if (item.schemaModel?.name) {
            schemaNameFilter = item.schemaModel.name;
        } else if (item.introspection.schemas && item.introspection.schemas.length > 0) {
            // Default to first schema or handle "all"?
            // Existing logic seemed to target schemas[0].name implicitly in step 3 logic.
            // We will let the user flow continue.
        }
    }
    // Case B: Data Work Item (Local DuckDB)
    else if (item && (item.contextValue?.startsWith('dp.duckdb') || item.schemaName)) {
        const connectionId = 'duck-piper-local-data-work';
        const { loadSchemas } = require('./schemaStore');
        const allSchemas: SchemaIntrospection[] = await loadSchemas();
        introspection = allSchemas.find(s => s.connectionId === connectionId);

        if (!introspection) {
            await ErrorHandler.handle(
                new Error(formatSchemaError(
                    'Schema access',
                    'Schema info not found for Data Work',
                    'Refresh the Schema panel and try again'
                )),
                { severity: ErrorSeverity.Warning, context: 'Generate Descriptions' }
            );
            return;
        }

        // Check if specific schema (e.g. bronze, imports)
        if (item.schemaName && item.schemaName !== 'duck-piper-local-data-work') {
            schemaNameFilter = item.schemaName;
        }
    }

    if (!introspection) {
        await ErrorHandler.handle(
            new Error(formatSchemaError(
                'Description generation',
                'No schema selected',
                'Invoke this command from the Schemas view context menu'
            )),
            { severity: ErrorSeverity.Warning, context: 'Generate Descriptions' }
        );
        return;
    }

    const _connectionId = introspection.connectionId;

    // Let's grab the schemas from introspection.
    let schemas = introspection.schemas;
    if (schemaNameFilter) {
        schemas = schemas.filter(s => s.name === schemaNameFilter);
    }

    if (!schemas || schemas.length === 0) {
        vscode.window.showWarningMessage("No schemas found in introspection.");
        return;
    }

    // Always generate both tables and columns (scope picker removed per spec)
    const includeTables = true;
    const includeColumns = true;

    // 3. Plan Work
    // We will generate descriptions for ALL tables in the introspection file.
    // Wait, "For a selected schema... generate...". 
    // Use schemaFileBaseName.

    const safeName = introspection.connectionName
        ? introspection.connectionName.replace(/[^a-z0-9_\-\.]/gi, '_')
        : introspection.connectionId;

    // Load existing
    let existing = await loadDescriptions(safeName);

    if (!existing) {
        existing = {
            __dpHeader: "#DuckPiper created",
            version: "0.1",
            generatedAt: new Date().toISOString(),
            connectionId: introspection.connectionId,
            connectionName: introspection.connectionName,
            dialect: introspection.dialect,
            schemaName: schemas[0].name,
            schemaDescription: undefined, // Placeholder to maintain property order
            tables: {},
            columns: {}
        };
    }

    const ai = await getConfiguredAIProvider(context, { requireConfigured: true });
    if (!ai) {
        const picked = await vscode.window.showWarningMessage(
            "No AI provider configured. Click Copy Prompt to paste it into your AI tool of choice.",
            "Open AI Settings",
            "Copy Prompt"
        );
        if (picked === 'Open AI Settings') await openAiProviderSettings();
        if (picked === 'Copy Prompt') {
            const { sendSchemaDescriptionsToChat } = require('../ai/sendToChat');
            await sendSchemaDescriptionsToChat(context, item);
        }
        return;
    }

    vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: "Generating AI Descriptions",
        cancellable: true
    }, async (progress, token) => {

        // Flatten all tables and views from all schemas in the file
        const workItems: { schema: string; table: TableModel }[] = [];
        for (const s of schemas) {
            for (const t of s.tables) {
                workItems.push({ schema: s.name, table: t });
            }
            for (const v of (s.views || [])) {
                workItems.push({ schema: s.name, table: v });
            }
        }

        const total = workItems.length;
        let processed = 0;

        for (const { schema, table } of workItems) {
            if (token.isCancellationRequested) break;

            progress.report({ message: `${schema}.${table.name} (${processed}/${total})`, increment: (1 / total) * 100 });

            // Prepare existing data to check for manual overrides
            const tableKey = `${schema}.${table.name}`;
            const existingTableDesc = existing?.tables?.[tableKey];

            // If scope includes tables AND (missing OR ai-source), we generate
            const shouldGenTable = includeTables && (!existingTableDesc || existingTableDesc.source === 'ai');

            // Check columns
            // We'll generate the prompt anyway if we need EITHER table or columns
            // But we filter what we ask for?
            // Spec says "Chunk by table... Ask AI to return 1 table description, per-column descriptions".

            // Optimization: If we only need columns, we can still ask for table context but ignore table desc output check?
            // Simpler: Just ask for what is needed.

            const prompt = await buildPrompt(schema, table, includeTables, includeColumns);

            try {
                const responseText = await ai.generateCompletion(prompt);
                const json = parseAIResponse(responseText);

                if (json) {
                    const _now = new Date().toISOString();

                    // Update Schema Description (only on first response or if not set)
                    if (json.schemaDescription && !existing!.schemaDescription) {
                        existing!.schemaDescription = json.schemaDescription;
                    }

                    // Update Table
                    if (json.table) {
                        const tDesc = json.table.description;
                        if (tDesc && shouldGenTable) {
                            existing!.tables[tableKey] = {
                                description: tDesc,
                                source: 'ai',
                                confidence: 0.9,
                                stale: false
                            };
                        }
                    }

                    // Update Columns
                    if (json.columns && Array.isArray(json.columns) && includeColumns) {
                        for (const c of json.columns) {
                            const colKey = c.key; // "schema.table.column"
                            const existingCol = existing!.columns[colKey];

                            if (!existingCol || existingCol.source === 'ai') {
                                existing!.columns[colKey] = {
                                    description: c.description,
                                    sampleValue: c.sampleValue,
                                    source: 'ai',
                                    confidence: 0.9,
                                    stale: false
                                };
                            }
                        }
                    }
                }
            } catch (err) {
                Logger.error(`Failed to generate for ${tableKey}`, err);
            }

            processed++;
        }

        // Mark stale
        // (Optional for this pass, but good practice per spec 8.2)
        markStaleEntries(existing!, workItems);

        // Update timestamp
        existing!.generatedAt = new Date().toISOString();

        await saveDescriptions(safeName, existing!);

        vscode.commands.executeCommand("dp.view.refreshSchemas"); // Refresh UI

        // Open the file for the user
        const dpDir = await import('../core/fsWorkspace').then(m => m.ensureDPDirs());
        const uri = vscode.Uri.joinPath(dpDir, 'schemas', `${safeName}.description.json`);
        const doc = await vscode.workspace.openTextDocument(uri);
        await vscode.window.showTextDocument(doc);
    });
}

async function buildPrompt(schemaName: string, table: TableModel, includeTable: boolean, includeColumns: boolean): Promise<string> {
    const template = await loadPromptTemplate('describeSchema');
    const columns = table.columns.map(c => `- ${c.name} (${c.type}): ${c.comment || ''}`).join('\n');

    return renderPrompt(template, {
        schemaName,
        tableName: table.name,
        columns,
        tableConstraint: !includeTable ? '- "table" field is optional or ignore descriptions.' : '',
        columnsConstraint: !includeColumns ? '- "columns" array is optional.' : ''
    });
}


function parseAIResponse(text: string): AIDescriptionResponse | null {
    try {
        // Strip markdown code blocks if any
        const clean = text.replace(/```json/g, '').replace(/```/g, '').trim();
        return JSON.parse(clean);
    } catch (_e) {
        Logger.warn(formatAIError(
            'Response parsing',
            'AI',
            'Invalid JSON response',
            'Try regenerating descriptions'
        ), text);
        return null;
    }
}

function markStaleEntries(data: SchemaDescriptionsFile, currentTables: { schema: string; table: TableModel }[]) {
    const currentTableKeys = new Set(currentTables.map(t => `${t.schema}.${t.table.name}`));
    const currentColumnKeys = new Set();
    currentTables.forEach(t => {
        t.table.columns.forEach(c => {
            currentColumnKeys.add(`${t.schema}.${t.table.name}.${c.name}`);
        });
    });

    // Mark tables stale
    for (const key in data.tables) {
        if (!currentTableKeys.has(key)) {
            data.tables[key].stale = true;
        } else {
            data.tables[key].stale = false;
        }
    }

    // Mark columns stale
    for (const key in data.columns) {
        if (!currentColumnKeys.has(key)) {
            data.columns[key].stale = true;
        } else {
            data.columns[key].stale = false;
        }
    }
}
