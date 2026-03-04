
import * as vscode from 'vscode';
import { HistoryEntry } from '../services/historyService';

export async function openMemoryRecallQuery(context: vscode.ExtensionContext, entry: HistoryEntry) {
    try {
        // Open new untitled SQL file
        const doc = await vscode.workspace.openTextDocument({
            language: 'sql',
            content: entry.query
        });

        await vscode.window.showTextDocument(doc);

        // Attempt to set connection context
        // Try original connection ID first
        let connectionId = entry.connectionId;

        // Verification: Check if this connection actually exists in current profile list?
        // But for now we just try to set it.
        const { getConnection } = require('../connections/connectionStore');

        let profile = null;
        if (connectionId) {
            profile = await getConnection(connectionId);
        }

        // Fallback to active global connection if original specific one is missing or undefined
        if (!profile) {
            const activeId = context.workspaceState.get<string>("dp.activeConnectionId");
            if (activeId) {
                connectionId = activeId;
                profile = await getConnection(activeId);
            }
        }

        if (connectionId && profile) {
            await vscode.commands.executeCommand('dp.sql.setConnectionForDoc', doc.uri, connectionId);
            // Also force set context for UI updates if needed
            // await vscode.commands.executeCommand('dp.connection.select', profile); // Do we want to switch GLOBAL context? Maybe not.
            // dp.sql.setConnectionForDoc should handle the document specific context.
        } else {
            // Just notify
            // vscode.window.showInformationMessage('Original connection not found. Using default context.');
        }

    } catch (e: unknown) {
        vscode.window.showErrorMessage(`Failed to open query: ${e instanceof Error ? e.message : String(e)}`);
    }
}
