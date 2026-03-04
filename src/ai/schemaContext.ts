import * as vscode from "vscode";
import { loadSchemas } from "../schema/schemaStore";
import { extractTables } from "../util/sqlParseHeuristics";
import { ensureDPDirs, fileExists, readJson } from "../core/fsWorkspace";
import { loadDescriptions } from "../schema/descriptionStore";
import { Logger } from "../core/logger";

/** A single table entry in the schema context sent to AI */
interface SchemaContextTable {
    schema: string;
    table: string;
    description?: string;
    columns: { name: string; type: string; description?: string }[];
}

/** Top-level structure for schema context sent to AI */
interface SchemaContextData {
    tables: SchemaContextTable[];
    meta: unknown[];
    relationships: unknown[];
}

export async function buildSchemaContext(sqlText: string, connectionId?: string): Promise<string> {
    const config = vscode.workspace.getConfiguration("dp");
    if (!config.get<boolean>("ai.sendSchemaContext", true)) return "";

    const maxChars = config.get<number>("ai.maxSchemaChars", 150000);
    const referenced = extractTables(sqlText);
    if (referenced.length === 0) return "";

    const schemas = await loadSchemas();
    const introspection = connectionId
        ? schemas.find(s => s.connectionId === connectionId)
        : schemas[0];

    if (!introspection) return "";

    const context: SchemaContextData = { tables: [], meta: [], relationships: [] };

    const safeName = introspection.connectionName
        ? introspection.connectionName.replace(/[^a-z0-9_\-\.]/gi, '_')
        : introspection.connectionId;
    const descriptions = await loadDescriptions(safeName);

    for (const ref of referenced) {
        const [schemaName, tableName] = ref.split('.');
        const schema = introspection.schemas.find(s => s.name === schemaName);
        const table = schema?.tables.find(t => t.name === tableName)
            || (schema?.views || []).find(v => v.name === tableName);
        if (table) {
            const tableKey = `${schemaName}.${tableName}`;
            const tDesc = descriptions?.tables?.[tableKey]?.description;

            context.tables.push({
                schema: schemaName,
                table: tableName,
                description: tDesc,
                columns: table.columns.map(c => {
                    const colKey = `${tableKey}.${c.name}`;
                    return {
                        name: c.name,
                        type: c.type,
                        description: descriptions?.columns?.[colKey]?.description
                    };
                })
            });
        }
    }

    const dpDir = await ensureDPDirs();
    for (const ref of referenced) {
        const schemaName = ref.split('.')[0];
        const metaUri = vscode.Uri.joinPath(dpDir, 'schemas', `${schemaName}Meta.json`);
        const relUri = vscode.Uri.joinPath(dpDir, 'schemas', `${schemaName}Relationships.json`);
        if (await fileExists(metaUri)) {
            try {
                context.meta.push(await readJson(metaUri));
            } catch (e) {
                // Failed to parse meta JSON - file may be corrupted, continue without it
                Logger.warn(`Failed to load metadata from ${metaUri.fsPath}`, e);
            }
        }
        if (await fileExists(relUri)) {
            try {
                context.relationships.push(await readJson(relUri));
            } catch (e) {
                // Failed to parse relationships JSON - file may be corrupted, continue without it
                Logger.warn(`Failed to load relationships from ${relUri.fsPath}`, e);
            }
        }
    }

    let text = JSON.stringify(context, null, 2);
    if (text.length > maxChars) {
        text = text.slice(0, maxChars) + "\n...truncated...";
    }
    return text;
}
