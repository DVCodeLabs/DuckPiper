import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { loadConnectionProfiles } from '../connections/connectionStore';
import { canonicalizeSql } from '../core/hashing';
import { ErrorHandler, ErrorSeverity, formatFileSystemError } from '../core/errorHandler';

export async function createSqlFile(context: vscode.ExtensionContext) {
    // 1. Get Workspace Folder
    const wsFolder = vscode.workspace.workspaceFolders?.[0];
    if (!wsFolder) {
        await ErrorHandler.handle(
            new Error(formatFileSystemError(
                'Create query',
                'No workspace folder open',
                'Open a folder in VS Code first'
            )),
            { severity: ErrorSeverity.Warning, context: 'Create SQL File' }
        );
        return;
    }

    // 2. Determine Folder
    const config = vscode.workspace.getConfiguration('dp');
    const relFolder = config.get<string>('query.defaultFolder', 'DP/queries');
    const folderPath = path.join(wsFolder.uri.fsPath, relFolder);

    if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath, { recursive: true });
    }

    // 3. Prompt for Name
    const input = await vscode.window.showInputBox({
        prompt: "Query Name (or path/query_name)",
        placeHolder: "monthly_active_users or reports/q1/revenue"
    });
    if (!input) return;

    // 4. Normalize Name and Handle Subfolders
    // Allow slashes for folders, but sanitize each segment
    const normalizedInput = input.replace(/\\/g, '/'); // normalize backslashes
    const segments = normalizedInput.split('/').map(s => s.replace(/[^a-zA-Z0-9_\-]/g, '_').toLowerCase());

    // Last segment is filename, rest are folders
    let fileName = segments.pop() || 'untitled';
    if (!fileName) fileName = 'untitled';
    const subFolder = segments.join(path.sep);

    const targetDir = path.join(folderPath, subFolder);
    if (!fs.existsSync(targetDir)) {
        fs.mkdirSync(targetDir, { recursive: true });
    }

    // 5. Check Collisions (simple increment)
    let iter = 0;
    let baseName = fileName;
    while (fs.existsSync(path.join(targetDir, `${baseName}.sql`))) {
        iter++;
        baseName = `${fileName}_${iter}`;
    }

    // 6. Prepare Content
    const sqlPath = path.join(targetDir, `${baseName}.sql`);
    const mdPath = path.join(targetDir, `${baseName}.md`);

    // Context for Metadata
    const activeConnId = context.workspaceState.get<string>("dp.activeConnectionId");
    let connName = "none";
    let connId = "";
    let dialect = "unknown";

    if (activeConnId) {
        const profiles = await loadConnectionProfiles();
        const profile = profiles.find(p => p.id === activeConnId);
        if (profile) {
            connName = profile.name;
            connId = profile.id;
            dialect = profile.dialect;
        }
    }

    const today = new Date().toISOString().split('T')[0];
    const niceTitle = baseName.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');

    const sqlContent = `SELECT
  1 AS example;
`;

    const { sqlHash } = canonicalizeSql(sqlContent);
    const sourcePath = path.relative(wsFolder.uri.fsPath, sqlPath).replace(/\\/g, '/');

    const mdContent = `---
title: "${niceTitle}"
created_at: "${today}"
connection: "${connName}"
connection_id: "${connId}"
dialect: "${dialect}"
tags: []
source_path: "${sourcePath}"
source_hash: "${sqlHash}"
---

<!-- DuckPiper:content:start -->
# Goal
-

# Context / Notes
-

# Inputs
- Tables:
- Key columns:

# Output
- What does this return?

# Caveats
-
<!-- DuckPiper:content:end -->
`;

    // 7. Write Files
    fs.writeFileSync(sqlPath, sqlContent);
    fs.writeFileSync(mdPath, mdContent);

    // 8. Open SQL File
    // Also trigger refresh of view? Indexer watcher should pick it up.
    const doc = await vscode.workspace.openTextDocument(vscode.Uri.file(sqlPath));
    await vscode.window.showTextDocument(doc);
}
