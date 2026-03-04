import * as vscode from 'vscode';
import { SchemaIntrospection } from '../core/types';
import { ERDViewProvider } from './erdViewProvider';

interface SchemaErdItem {
    introspection?: SchemaIntrospection;
    schemaModel?: { name: string };
    contextValue?: string;
    schemaName?: string;
}

export async function openSchemaErdCommand(context: vscode.ExtensionContext, item?: SchemaErdItem) {
    if (!item) {
        vscode.window.showErrorMessage("Select a schema to view ERD.");
        return;
    }

    const provider = ERDViewProvider.current;
    if (!provider) {
        vscode.window.showErrorMessage("ERD View not initialized.");
        return;
    }

    const { getConnection } = require('../connections/connectionStore');
    const { ensureConnectionSecrets } = require('../connections/connectionCommands');

    // Case 1: Standard Schema Item (with introspection)
    if (item.introspection) {
        const connectionId = item.introspection.connectionId;
        const profile = await getConnection(connectionId);

        if (!profile) {
            vscode.window.showErrorMessage("Connection profile not found.");
            return;
        }

        const secrets = await ensureConnectionSecrets(profile);
        if (!secrets) return;

        if (item.schemaModel?.name) {
            const filtered: SchemaIntrospection = {
                ...item.introspection,
                schemas: item.introspection.schemas.filter((s: { name: string }) => s.name === item.schemaModel?.name)
            };
            await provider.showERD(profile, secrets, filtered);
            return;
        }

        // Open the ERD panel using the existing implementation (full connection)
        await provider.showERD(profile, secrets, item.introspection);
        return;
    }

    // Case 2: Data Work Item (Duck_Piper DuckDB) - detected via contextValue or schemaName check
    if (item.contextValue && item.contextValue.startsWith('dp.duckdb')) {
        const connectionId = 'duck-piper-local-data-work';
        const profile = await getConnection(connectionId);

        if (!profile) {
            vscode.window.showErrorMessage("Duck_Piper connection not found.");
            return;
        }

        const secrets = await ensureConnectionSecrets(profile);
        if (!secrets) return;

        // Load schemas from disk since item doesn't have it attached
        const { loadSchemas } = require('../schema/schemaStore');
        const allSchemas: SchemaIntrospection[] = await loadSchemas();
        const introspection = allSchemas.find(s => s.connectionId === connectionId);

        if (!introspection) {
            vscode.window.showErrorMessage("Schema info not found. Please refresh Data Work panel.");
            return;
        }

        // Filter if specific schema (item.schemaName)
        if (item.schemaName) {
            const filtered: SchemaIntrospection = {
                ...introspection,
                schemas: introspection.schemas.filter(s => s.name === item.schemaName)
            };
            await provider.showERD(profile, secrets, filtered);
        } else {
            await provider.showERD(profile, secrets, introspection);
        }
        return;
    }

    vscode.window.showErrorMessage("Select a schema to view ERD.");
}
