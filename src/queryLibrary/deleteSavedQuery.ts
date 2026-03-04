import * as vscode from 'vscode';
import * as path from 'path';
import { SavedQueryItem } from './savedQueriesView';
import { ErrorHandler, ErrorSeverity, formatQueryError } from '../core/errorHandler';

export async function deleteSavedQuery(item: SavedQueryItem | vscode.Uri) {
    if (!item) return;

    if (!vscode.workspace.workspaceFolders) return;
    const root = vscode.workspace.workspaceFolders[0].uri;

    let fileUri: vscode.Uri;
    let displayLabel: string;

    // Handle both SavedQueryItem (from sidebar) and Uri (from codelens)
    if (item instanceof vscode.Uri) {
        fileUri = item;
        displayLabel = path.basename(item.fsPath, '.sql');
    } else if (item.entry) {
        fileUri = vscode.Uri.joinPath(root, item.entry.path);
        displayLabel = item.label as string || path.basename(item.entry.path, '.sql');
    } else {
        return;
    }

    // 1. Confirm
    const choice = await vscode.window.showWarningMessage(
        `Are you sure you want to delete '${displayLabel}' and its source file? This cannot be undone.`,
        { modal: true },
        'Delete'
    );

    if (choice !== 'Delete') return;

    // 2. Delete File
    try {
        await vscode.workspace.fs.delete(fileUri, { recursive: false, useTrash: true });

        // Note: The file watcher in queryIndex.ts and deleteBundleWatcher.ts will handle:
        // - Updating the index (removing the entry)
        // - Deleting sibling files (.md, .json, etc)

        // We can optionally trigger a refresh of the view, but the watcher should trigger it via onDidChangeTreeData if wired up.
        // SavedQueriesViewProvider listens? 
        // In extension.ts: context.subscriptions.push(watchers) -> () => savedQueriesProvider.refresh()
        // So it should auto-refresh.

        vscode.window.showInformationMessage(`Deleted query '${displayLabel}'`);

    } catch (e: unknown) {
        // If file not found, it might already be deleted, so we should just ensure index is clean?
        // But let's show error if it's a real FS error.
        const isFileNotFound = e instanceof vscode.FileSystemError && e.code === 'FileNotFound';
        if (!isFileNotFound) {
            await ErrorHandler.handle(e, {
                severity: ErrorSeverity.Error,
                userMessage: formatQueryError(
                    'Delete query',
                    ErrorHandler.extractErrorMessage(e),
                    'Check file permissions and try again'
                ),
                context: 'Delete Saved Query'
            });
        } else {
            // Force refresh if file was already gone
            vscode.commands.executeCommand('dp.view.refreshSavedQueries');
        }
    }
}
