import * as vscode from "vscode";
import { DPDocConnectionStore } from "./sqlCodelens";
import { loadConnectionProfiles } from "../connections/connectionStore";
import { queryIndex } from "../queryLibrary/queryIndex";

async function updateQueryMdConnection(docUri: vscode.Uri, connectionName: string, dialect?: string) {
    if (!docUri.fsPath.endsWith(".sql")) return;
    const mdPath = docUri.fsPath.replace(/\.sql$/i, ".md");
    const mdUri = vscode.Uri.file(mdPath);

    let raw: Uint8Array;
    try {
        raw = await vscode.workspace.fs.readFile(mdUri);
    } catch {
        return;
    }

    const text = new TextDecoder("utf-8").decode(raw);
    if (!text.startsWith("---")) return;

    const lines = text.split(/\r?\n/);
    const endIndex = lines.findIndex((line, idx) => idx > 0 && line.trim() === "---");
    if (endIndex === -1) return;

    let connectionReplaced = false;
    let dialectReplaced = false;
    for (let i = 1; i < endIndex; i += 1) {
        if (lines[i].startsWith("connection:")) {
            lines[i] = `connection: "${connectionName}"`;
            connectionReplaced = true;
        }
        if (dialect && lines[i].startsWith("dialect:")) {
            lines[i] = `dialect: "${dialect}"`;
            dialectReplaced = true;
        }
    }

    if (!connectionReplaced) {
        lines.splice(endIndex, 0, `connection: "${connectionName}"`);
    }
    if (dialect && !dialectReplaced) {
        lines.splice(endIndex, 0, `dialect: "${dialect}"`);
    }

    const updated = lines.join("\n");
    await vscode.workspace.fs.writeFile(mdUri, new TextEncoder().encode(updated));
}

export function registerSqlCodelensCommands(
    context: vscode.ExtensionContext,
    store: DPDocConnectionStore,
    providerRefresh: () => void,
    onConnectionChanged?: () => void
) {
    context.subscriptions.push(
        vscode.commands.registerCommand("dp.sql.setConnectionForDoc", async (uri: vscode.Uri, connectionId?: string) => {
            // Ensure we have a valid URI
            if (!uri) {
                const editor = vscode.window.activeTextEditor;
                if (!editor) return;
                uri = editor.document.uri;
            }

            const doc = await vscode.workspace.openTextDocument(uri);

            // Bypass UI if connectionId is provided
            if (connectionId) {
                const profiles = await loadConnectionProfiles();
                const profile = profiles.find(p => p.id === connectionId);
                if (profile) {
                    await store.set(doc, connectionId);
                    // Update Index
                    await queryIndex.updateConnectionContext(doc.uri, profile.id, profile.name, profile.dialect);

                    providerRefresh();
                    onConnectionChanged?.();
                    return;
                }
            }

            // Load connections dynamically from the store
            const profiles = await loadConnectionProfiles();
            const connections = profiles.map(p => ({
                label: p.name,
                description: `${p.dialect}${p.database ? ` • ${p.database}` : ''}`,
                id: p.id,
                profile: p
            }));

            if (!connections.length) {
                vscode.window.showWarningMessage("DuckPiper: No connections available. Add one first.");
                return;
            }

            const picked = await vscode.window.showQuickPick(
                connections,
                { title: "Select connection for this SQL tab" }
            );

            if (!picked) return;

            await store.set(doc, picked.id);
            await updateQueryMdConnection(doc.uri, picked.label, picked.profile?.dialect);

            // Update Index (Source of Truth)
            await queryIndex.updateConnectionContext(doc.uri, picked.id, picked.label, picked.profile?.dialect || 'duckdb');

            // Also update running context if this is the active document
            if (vscode.window.activeTextEditor?.document.uri.toString() === doc.uri.toString()) {
                await context.workspaceState.update("dp.activeConnectionId", picked.id);
                // We might want to trigger context updates here too if needed
            }

            providerRefresh();
            onConnectionChanged?.();
        })
    );
}
