import * as vscode from 'vscode';
import * as path from 'path';
import { fileExists } from '../core/fsWorkspace';
import { queryIndex } from './queryIndex';
import { Logger } from '../core/logger';
import { ErrorHandler, ErrorSeverity, formatQueryError } from '../core/errorHandler';
import {
    ignoreNewUri,
    isCanonicalSql,
    patchJsonSourcePath,
    patchMdFrontmatter,
    siblingUri,
    stripSuffix,
    withPath
} from './bundleUtils';

export async function renameQueryBundle(context: vscode.ExtensionContext, arg?: vscode.Uri | { entry?: { path: string } }) {
    // 1. Determine Target SQL File
    let targetUri: vscode.Uri | undefined;

    if (arg instanceof vscode.Uri) {
        targetUri = arg;
    } else if (arg?.entry?.path) {
        // Handle SavedQueryItem
        const root = vscode.workspace.workspaceFolders?.[0]?.uri;
        if (root) {
            targetUri = vscode.Uri.joinPath(root, arg.entry.path);
        }
    }

    // Fallback if no URI provided
    if (!targetUri) {
        const editor = vscode.window.activeTextEditor;
        if (editor && isCanonicalSql(editor.document.uri)) {
            targetUri = editor.document.uri;
        }
    }

    if (!targetUri || !isCanonicalSql(targetUri)) {
        await ErrorHandler.handle(
            new Error(formatQueryError(
                'Rename query',
                'No SQL query file selected',
                'Select a .sql file to rename'
            )),
            { severity: ErrorSeverity.Warning, context: 'Rename Query Bundle' }
        );
        return;
    }

    // 2. Ask for New Name
    const currentName = path.basename(targetUri.fsPath, '.sql');
    const newNameInput = await vscode.window.showInputBox({
        prompt: "New query name",
        value: currentName,
        validateInput: (val) => {
            if (!val || !val.trim()) return "Name cannot be empty";
            if (val.includes('/') || val.includes('\\') || val.includes('..')) return "Name cannot contain path separators";
            if (!/^[a-zA-Z0-9_\-]+$/.test(val)) return "Name must be alphanumeric, underscores, or hyphens.";
            return null;
        }
    });

    if (!newNameInput) return; // Cancelled

    // Normalize (in case they typed .sql)
    const newBaseName = newNameInput.endsWith('.sql') ? newNameInput.slice(0, -4) : newNameInput;

    // 3. Ask for Destination Folder
    const wsFolder = vscode.workspace.workspaceFolders?.[0];
    if (!wsFolder) return;

    const currentDir = path.dirname(targetUri.fsPath);
    const currentRel = vscode.workspace.asRelativePath(currentDir, false);

    const folderInput = await vscode.window.showInputBox({
        prompt: "Destination Folder (relative)",
        value: currentRel,
        placeHolder: "DP/queries"
    });

    if (folderInput === undefined) return; // Cancelled

    const destFolder = vscode.Uri.joinPath(wsFolder.uri, folderInput);

    // Ensure directory exists
    if (!(await fileExists(destFolder))) {
        await vscode.workspace.fs.createDirectory(destFolder);
    }

    // 4. Calculate Paths
    const newSqlUri = vscode.Uri.joinPath(destFolder, `${newBaseName}.sql`);

    // Check conflict for main SQL
    if (await fileExists(newSqlUri)) {
        const errorMsg = formatQueryError(
            'Rename query',
            `File already exists: ${newBaseName}.sql`,
            'Choose a different name'
        );
        const choice = await vscode.window.showErrorMessage(errorMsg, "Choose different name");
        if (choice === "Choose different name") {
            // Restart
            return renameQueryBundle(context, targetUri);
        }
        return;
    }

    // 5. Execute Renames
    const oldSql = targetUri;
    const conflicts: string[] = [];

    // Helpers
    const doRename = async (oldU: vscode.Uri, newU: vscode.Uri) => {
        if (await fileExists(oldU)) {
            if (await fileExists(newU)) {
                conflicts.push(path.basename(newU.fsPath));
                return;
            }
            ignoreNewUri(newU);
            await vscode.workspace.fs.rename(oldU, newU);
        }
    };

    // Main SQL
    // NOTE: We call ignoreNewUri inside doRename to prevent watcher loop
    await doRename(oldSql, newSqlUri);

    // Companions
    const oldMd = siblingUri(oldSql, ".md");
    const newMd = siblingUri(newSqlUri, ".md");

    const oldComments = siblingUri(oldSql, ".comments.json");
    const newComments = siblingUri(newSqlUri, ".comments.json");

    const oldChart = siblingUri(oldSql, ".chart.json");
    const newChart = siblingUri(newSqlUri, ".chart.json");

    const oldChartConfig = siblingUri(oldSql, ".chartconfig.json");
    const newChartConfig = siblingUri(newSqlUri, ".chartconfig.json");

    const oldAnnotated = withPath(oldSql, stripSuffix(oldSql.path, ".sql") + ".annotated.sql");
    const newAnnotated = withPath(newSqlUri, stripSuffix(newSqlUri.path, ".sql") + ".annotated.sql");

    await doRename(oldMd, newMd);
    await doRename(oldComments, newComments);
    await doRename(oldChart, newChart);
    await doRename(oldChartConfig, newChartConfig);
    await doRename(oldAnnotated, newAnnotated);

    // 6. Patch References
    const newSqlRel = vscode.workspace.asRelativePath(newSqlUri, false);
    if (await fileExists(newComments)) await patchJsonSourcePath(newComments, newSqlRel);
    if (await fileExists(newChart)) await patchJsonSourcePath(newChart, newSqlRel);
    if (await fileExists(newChartConfig)) await patchJsonSourcePath(newChartConfig, newSqlRel);
    if (await fileExists(newMd)) await patchMdFrontmatter(newMd, newSqlRel);

    // 7. Update Index
    try {
        await queryIndex.handleRename(oldSql, newSqlUri);
        vscode.commands.executeCommand("dp.view.refreshSavedQueries");
    } catch (e) {
        Logger.warn("Index rename failed", e);
    }

    if (conflicts.length > 0) {
        vscode.window.showWarningMessage(`Renamed bundle to ${newBaseName}, but skipped existing files: ${conflicts.join(', ')}`);
    } else {
        vscode.window.showInformationMessage(`Renamed query bundle to ${newBaseName}`);
    }
}
