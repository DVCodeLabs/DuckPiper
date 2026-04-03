import * as vscode from "vscode";
import { getConfiguredAIProvider, openAiProviderSettings } from "./aiService";
import { loadConnectionProfiles } from "../connections/connectionStore";
import { buildSchemaContext } from "./schemaContext";
import { loadPromptTemplate, renderPrompt } from "./prompts";
import { streamEdit } from "../editor/editorStreaming";

export async function generateAndStreamInlineComments(
    context: vscode.ExtensionContext,
    editor: vscode.TextEditor
): Promise<void> {
    const sqlDoc = editor.document;
    const sqlText = sqlDoc.getText();

    if (!sqlText.trim()) {
        vscode.window.showWarningMessage("File is empty.");
        return;
    }

    const { connectionName, dialect, connectionId } = await resolveConnectionInfo(context, sqlDoc);

    const provider = await getConfiguredAIProvider(context, { requireConfigured: true });
    if (!provider) {
        const picked = await vscode.window.showWarningMessage(
            "No AI provider configured. Click Copy Prompt to paste it into your AI tool of choice.",
            "Open AI Settings",
            "Copy Prompt"
        );
        if (picked === "Open AI Settings") await openAiProviderSettings();
        if (picked === "Copy Prompt") {
            const { sendCommentToChat } = require('./sendToChat');
            await sendCommentToChat(context);
        }
        return;
    }

    const schemaContext = await buildSchemaContext(sqlText, connectionId);

    // We use a specific prompt that asks for a full rewrite with comments
    const promptTemplate = await loadPromptTemplate("inlineComments");
    const prompt = renderPrompt(promptTemplate, {
        sql: sqlText,
        dialect,
        connection: connectionName,
        schemaContext: schemaContext ? `Schema context:\n${schemaContext}` : ""
    });

    await vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: "Generating inline comments...",
        cancellable: true
    }, async (_progress, token) => {
        if (token.isCancellationRequested) return;
        try {
            const output = await provider.generateCompletion(prompt);
            if (token.isCancellationRequested) return;

            const cleanSql = stripWrappingCodeFence(output);

            // "Stream" the edit into the editor
            await streamEdit(editor, cleanSql);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Comment generation failed: ${msg}`);
        }
    });
}

function stripWrappingCodeFence(text: string): string {
    const trimmed = text.trim();
    // Check for ```sql or just ```
    const match = trimmed.match(/^```(\w+)?\n([\s\S]+?)\n```$/);
    if (match) {
        return match[2];
    }
    if (trimmed.startsWith("```") && trimmed.endsWith("```")) {
        const lines = trimmed.split(/\r?\n/);
        if (lines.length >= 3) {
            return lines.slice(1, -1).join("\n").trim();
        }
    }
    return trimmed;
}

async function resolveConnectionInfo(
    context: vscode.ExtensionContext,
    doc: vscode.TextDocument
): Promise<{ connectionId?: string; connectionName: string; dialect: string }> {
    const docKey = doc.uri.toString();
    const docConnections = context.workspaceState.get<Record<string, string>>("dp.docConnections.v1", {});
    const docConnId = docConnections[docKey];
    const activeId = context.workspaceState.get<string>("dp.activeConnectionId");
    const connectionId = docConnId || activeId;

    let connectionName = "none";
    let dialect = "unknown";

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
