import * as vscode from 'vscode';
import { Logger } from '../core/logger';

type BrokerId = 'claudeExtension' | 'codexExtension';
type BrokerMode = 'handoff';
type BrokerStatus = 'launched' | 'failed' | 'userActionRequired';

interface BrokerSelection {
    file: string;
    startLine: number;
    startColumn: number;
    endLine: number;
    endColumn: number;
}

export interface BrokerTask {
    title: string;
    prompt: string;
    workspaceRoot: string;
    targetFiles: string[];
    expectedWriteTargets: string[];
    contextFiles?: string[];
    primaryTarget?: string;
    selection?: BrokerSelection;
    allowCommands?: boolean;
}

export interface BrokerResult {
    handled: boolean;
    providerId?: BrokerId;
    mode?: BrokerMode;
    status?: BrokerStatus;
    message?: string;
    changedFiles?: string[];
}

interface BrokerConfig {
    broker: string;
    backend: string;
    installedExtensionChoice: string;
}

interface InstalledExtensionOption {
    id: BrokerId;
    label: string;
    detail: string;
}

const CONFIG_SECTION = 'dp';
const CLAUDE_EXTENSION_ID = 'anthropic.claude-code';

async function updateConfig(key: string, value: string): Promise<void> {
    const config = vscode.workspace.getConfiguration(CONFIG_SECTION);
    await config.update(key, value, vscode.ConfigurationTarget.Global);
    if (vscode.workspace.workspaceFolders?.length) {
        await config.update(key, value, vscode.ConfigurationTarget.Workspace);
    }
}
const CODEX_EXTENSION_ID = 'openai.chatgpt';
const CODEX_COMMAND_OPEN = ['chatgpt.openSidebar', 'chatgpt.newCodexPanel'];

function getConfig(): BrokerConfig {
    const config = vscode.workspace.getConfiguration(CONFIG_SECTION);
    return {
        broker: config.get<string>('ai.broker', 'auto'),
        backend: config.get<string>('ai.backend', ''),
        installedExtensionChoice: config.get<string>('ai.installedExtensionChoice', '')
    };
}

export function createFileEditingBrokerPrompt(basePrompt: string, task: Pick<BrokerTask, 'workspaceRoot' | 'targetFiles' | 'primaryTarget' | 'allowCommands'>): string {
    const lines: string[] = [
        `Workspace root: ${task.workspaceRoot}`,
        '',
        'Only modify these files:'
    ];

    const targets = task.targetFiles.length > 0 ? task.targetFiles : [task.primaryTarget].filter(Boolean) as string[];
    for (const target of targets) {
        lines.push(`- ${target}`);
    }

    if (task.primaryTarget) {
        lines.push('');
        lines.push('Primary target:');
        lines.push(`- ${task.primaryTarget}`);
    }

    lines.push('');
    lines.push('Requirements:');
    lines.push('- Keep changes minimal and focused.');
    lines.push('- Do not modify files outside the allowlist.');
    lines.push('- Edit the target file directly when your environment supports file edits.');
    if (!task.allowCommands) {
        lines.push('- Do not run shell commands unless they are strictly required.');
    }
    lines.push('');
    lines.push('Task instructions:');
    lines.push(basePrompt.trim());

    return lines.join('\n');
}

export async function selectInstalledExtensionChoice(): Promise<void> {
    const options = await getInstalledExtensionOptions();
    if (options.length === 0) {
        vscode.window.showWarningMessage('No supported AI extensions are installed.');
        return;
    }

    const picked = await vscode.window.showQuickPick(options.map((option) => ({
        label: option.label,
        detail: option.detail,
        option
    })), {
        title: 'Select Installed AI Extension',
        placeHolder: 'Choose which installed AI extension DuckPiper should use by default'
    });

    if (!picked) {
        return;
    }

    await updateConfig('ai.installedExtensionChoice', picked.option.id);
    vscode.window.showInformationMessage(`Installed AI extension set to ${picked.option.label}.`);
}

export async function maybeHandleBrokerTask(task: BrokerTask): Promise<BrokerResult | null> {
    const config = getConfig();

    if (config.broker === 'auto' && config.backend && !config.installedExtensionChoice) {
        return null;
    }

    if (config.broker === 'none') {
        return null;
    }

    if (config.broker === 'claudeExtension' || config.broker === 'codexExtension') {
        const explicitBroker = config.broker;
        const available = await isBrokerAvailable(explicitBroker);
        if (!available) {
            const message = `Configured AI broker "${explicitBroker}" is not currently available in this editor instance.`;
            vscode.window.showWarningMessage(message);
            return {
                handled: true,
                providerId: explicitBroker,
                mode: 'handoff',
                status: 'failed',
                message
            };
        }
    }

    const resolved = await resolveBroker(config);
    if (!resolved) {
        return null;
    }

    switch (resolved.id) {
        case 'claudeExtension':
            return runClaudeExtensionHandoff(task);
        case 'codexExtension':
            return runCodexExtensionHandoff(task);
        default:
            return null;
    }
}

async function resolveBroker(config: BrokerConfig): Promise<{ id: BrokerId } | null> {
    const explicit = config.broker;
    if (explicit === 'claudeExtension' || explicit === 'codexExtension') {
        return { id: explicit };
    }

    if (explicit !== 'auto') {
        return null;
    }

    const availableExtensions = await getInstalledExtensionOptions();
    if (availableExtensions.length > 1) {
        let chosen = config.installedExtensionChoice;
        if (!chosen) {
            const picked = await vscode.window.showQuickPick(availableExtensions.map((option) => ({
                label: option.label,
                detail: option.detail,
                option
            })), {
                title: 'Choose Installed AI Extension',
                placeHolder: 'Select which installed AI extension DuckPiper should use'
            });
            if (picked) {
                chosen = picked.option.id;
                await updateConfig('ai.installedExtensionChoice', chosen);
            }
        }

        if (chosen === 'claudeExtension' || chosen === 'codexExtension') {
            return { id: chosen };
        }
    }

    if (availableExtensions.length === 1) {
        const onlyOption = availableExtensions[0];
        if (config.installedExtensionChoice && config.installedExtensionChoice !== onlyOption.id) {
            return null;
        }
        return { id: onlyOption.id };
    }

    return null;
}

async function getInstalledExtensionOptions(): Promise<InstalledExtensionOption[]> {
    const options: InstalledExtensionOption[] = [];

    if (await isBrokerAvailable('codexExtension')) {
        options.push({
            id: 'codexExtension',
            label: 'Codex',
            detail: 'Open the Codex extension and attach context there.'
        });
    }

    if (await isBrokerAvailable('claudeExtension')) {
        options.push({
            id: 'claudeExtension',
            label: 'Claude Code',
            detail: 'Open Claude Code with a pre-filled prompt and continue there.'
        });
    }

    return options;
}

async function isBrokerAvailable(id: BrokerId): Promise<boolean> {
    switch (id) {
        case 'claudeExtension':
            return Boolean(vscode.extensions.getExtension(CLAUDE_EXTENSION_ID));
        case 'codexExtension': {
            if (vscode.extensions.getExtension(CODEX_EXTENSION_ID)) {
                return true;
            }
            const commands = await vscode.commands.getCommands(true);
            return commands.includes('chatgpt.openSidebar') || commands.includes('chatgpt.newCodexPanel');
        }
        default:
            return false;
    }
}

async function runClaudeExtensionHandoff(task: BrokerTask): Promise<BrokerResult> {
    await vscode.env.clipboard.writeText(task.prompt);
    const commands = await vscode.commands.getCommands(true);
    let opened = false;

    if (commands.includes('claude-vscode.editor.open')) {
        try {
            await vscode.commands.executeCommand('claude-vscode.editor.open', undefined, task.prompt, vscode.ViewColumn.Beside);
            opened = true;
        } catch (error) {
            Logger.warn('Failed to open Claude Code in a right split. Falling back to URI handoff.', error);
        }
    }

    if (!opened) {
        const uri = vscode.Uri.parse(`vscode://anthropic.claude-code/open?prompt=${encodeURIComponent(task.prompt)}`);
        opened = await vscode.env.openExternal(uri);
    }

    const message = opened
        ? 'Prompt loaded in Claude Code. Submit there to continue.'
        : 'Claude Code could not be opened automatically. The prompt was copied to your clipboard.';

    vscode.window.showInformationMessage(message);

    return {
        handled: true,
        providerId: 'claudeExtension',
        mode: 'handoff',
        status: 'userActionRequired',
        message
    };
}

async function runCodexExtensionHandoff(task: BrokerTask): Promise<BrokerResult> {
    const commands = await vscode.commands.getCommands(true);
    const openCommand = CODEX_COMMAND_OPEN.find((candidate) => commands.includes(candidate));
    if (!openCommand) {
        vscode.window.showWarningMessage('Codex commands are not available in this editor instance.');
        return {
            handled: true,
            providerId: 'codexExtension',
            mode: 'handoff',
            status: 'failed',
            message: 'Codex commands are not available.'
        };
    }

    await vscode.commands.executeCommand(openCommand);
    if (commands.includes('chatgpt.newChat')) {
        await vscode.commands.executeCommand('chatgpt.newChat');
    }

    const paths = Array.from(new Set([
        ...(task.contextFiles || []),
        ...task.targetFiles
    ]));

    for (const filePath of paths) {
        try {
            if (commands.includes('chatgpt.addFileToThread')) {
                await vscode.commands.executeCommand('chatgpt.addFileToThread', vscode.Uri.file(filePath));
            }
        } catch (error) {
            Logger.warn(`Failed to attach ${filePath} to Codex thread`, error);
        }
    }

    if (task.selection && commands.includes('chatgpt.addToThread')) {
        const originalEditor = vscode.window.activeTextEditor;
        try {
            const document = await vscode.workspace.openTextDocument(vscode.Uri.file(task.selection.file));
            const editor = await vscode.window.showTextDocument(document, { preview: false, preserveFocus: true });
            editor.selection = new vscode.Selection(
                task.selection.startLine - 1,
                task.selection.startColumn - 1,
                task.selection.endLine - 1,
                task.selection.endColumn - 1
            );
            await vscode.commands.executeCommand('chatgpt.addToThread');
        } catch (error) {
            Logger.warn('Failed to attach selected range to Codex thread', error);
        } finally {
            if (originalEditor) {
                await vscode.window.showTextDocument(originalEditor.document, originalEditor.viewColumn, true);
            }
        }
    }

    await vscode.env.clipboard.writeText(task.prompt);
    const message = 'Context attached to Codex. The prompt was copied to your clipboard; paste it into the Codex composer to continue.';
    vscode.window.showInformationMessage(message);

    return {
        handled: true,
        providerId: 'codexExtension',
        mode: 'handoff',
        status: 'userActionRequired',
        message
    };
}
