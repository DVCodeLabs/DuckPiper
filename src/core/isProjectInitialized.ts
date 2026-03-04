import * as vscode from 'vscode';

/**
 * Checks if the current workspace is initialized as a DuckPiper project.
 * A project is considered initialized if the DP/ folder exists with required subfolders.
 * This is a read-only check with no side effects.
 */
export async function isProjectInitialized(): Promise<boolean> {
    const folders = vscode.workspace.workspaceFolders;
    if (!folders || folders.length === 0) {
        return false;
    }

    const root = folders[0].uri;
    const dpDir = vscode.Uri.joinPath(root, 'DP');

    // Check if DP directory exists
    try {
        const stat = await vscode.workspace.fs.stat(dpDir);
        if (stat.type !== vscode.FileType.Directory) {
            return false;
        }
    } catch {
        return false;
    }

    // Check minimum required subdirectories
    const requiredSubs = ['queries', 'schemas', 'notebooks', 'system'];
    for (const sub of requiredSubs) {
        try {
            const subUri = vscode.Uri.joinPath(dpDir, sub);
            const stat = await vscode.workspace.fs.stat(subUri);
            if (stat.type !== vscode.FileType.Directory) {
                return false;
            }
        } catch {
            return false;
        }
    }

    return true;
}

/**
 * Updates the dp.project.initialized context key based on current state.
 */
export async function updateProjectInitializedContext(): Promise<boolean> {
    const initialized = await isProjectInitialized();
    await vscode.commands.executeCommand('setContext', 'dp.project.initialized', initialized);
    return initialized;
}
