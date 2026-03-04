
import * as vscode from 'vscode';
import { ensureDPDirs, readJson, writeJson } from '../core/fsWorkspace';
import { DbDialect } from '../core/types';

export interface DescriptionEntry {
    description: string;
    sampleValue?: string;
    confidence?: number;
    source: 'ai' | 'manual';
    stale?: boolean;
}

export interface SchemaDescriptionsFile {
    __dpHeader: string;
    version: string;
    generatedAt: string;
    connectionId: string;
    connectionName?: string;
    dialect: DbDialect;
    schemaName: string;
    schemaDescription?: string; // Top-level description of the schema
    tables: Record<string, DescriptionEntry>; // key: "schema.table"
    columns: Record<string, DescriptionEntry>; // key: "schema.table.column"
}

export async function loadDescriptions(schemaFileBaseName: string): Promise<SchemaDescriptionsFile | null> {
    try {
        const dpDir = await ensureDPDirs();
        const uri = vscode.Uri.joinPath(dpDir, 'schemas', `${schemaFileBaseName}.description.json`);
        return await readJson<SchemaDescriptionsFile>(uri);
    } catch (_e) {
        return null;
    }
}

export async function saveDescriptions(schemaFileBaseName: string, data: SchemaDescriptionsFile): Promise<void> {
    const dpDir = await ensureDPDirs();
    const uri = vscode.Uri.joinPath(dpDir, 'schemas', `${schemaFileBaseName}.description.json`);
    await writeJson(uri, data);
}
