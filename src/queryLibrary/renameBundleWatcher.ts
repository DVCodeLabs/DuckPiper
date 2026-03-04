import * as vscode from "vscode";
import { fileExists } from "../core/fsWorkspace";
import { queryIndex } from "./queryIndex";
import {
    ignoredNewUris,
    ignoreNewUri,
    isCanonicalSql,
    patchJsonSourcePath,
    patchMdFrontmatter,
    siblingUri,
    stripSuffix,
    withPath
} from "./bundleUtils";

// Rebuild timer removed in favor of direct queryIndex updates
function notifyViews() {
    vscode.commands.executeCommand("dp.view.refreshSavedQueries");
}

async function safeRename(oldUri: vscode.Uri, newUri: vscode.Uri, conflicts: string[]) {
    if (!(await fileExists(oldUri))) return;
    if (await fileExists(newUri)) {
        conflicts.push(newUri.fsPath);
        return;
    }
    ignoreNewUri(newUri);
    await vscode.workspace.fs.rename(oldUri, newUri, { overwrite: false });
}

export async function handleRenames(
    files: readonly { oldUri: vscode.Uri; newUri: vscode.Uri }[]
): Promise<void> {
    const conflicts: string[] = [];

    for (const f of files) {
        // ignore events caused by our own sibling renames
        if (ignoredNewUris.has(f.newUri.toString())) continue;

        if (!isCanonicalSql(f.oldUri) && !isCanonicalSql(f.newUri)) continue;

        // We treat it as canonical sql rename/move
        const oldSql = f.oldUri;
        const newSql = f.newUri;

        const oldMd = siblingUri(oldSql, ".md");
        const newMd = siblingUri(newSql, ".md");

        const oldComments = siblingUri(oldSql, ".comments.json");
        const newComments = siblingUri(newSql, ".comments.json");

        const oldChart = siblingUri(oldSql, ".chart.json");
        const newChart = siblingUri(newSql, ".chart.json");

        const oldChartConfig = siblingUri(oldSql, ".chartconfig.json");
        const newChartConfig = siblingUri(newSql, ".chartconfig.json");

        const oldAnnotated = withPath(oldSql, stripSuffix(oldSql.path, ".sql") + ".annotated.sql");
        const newAnnotated = withPath(newSql, stripSuffix(newSql.path, ".sql") + ".annotated.sql");

        // Rename siblings if present
        await safeRename(oldMd, newMd, conflicts);
        await safeRename(oldComments, newComments, conflicts);
        await safeRename(oldChart, newChart, conflicts);
        await safeRename(oldChartConfig, newChartConfig, conflicts);
        await safeRename(oldAnnotated, newAnnotated, conflicts);

        // Patch internal references in moved siblings (only if they exist now)
        const newSqlRel = vscode.workspace.asRelativePath(newSql, false);

        if (await fileExists(newComments)) await patchJsonSourcePath(newComments, newSqlRel);
        if (await fileExists(newChart)) await patchJsonSourcePath(newChart, newSqlRel);
        if (await fileExists(newChartConfig)) await patchJsonSourcePath(newChartConfig, newSqlRel);
        if (await fileExists(newMd)) await patchMdFrontmatter(newMd, newSqlRel);



        // Update Index (Preserve Metadata)
        await queryIndex.handleRename(oldSql, newSql);
        notifyViews();
    }

    if (conflicts.length) {
        void vscode.window.showWarningMessage(
            `DuckPiper: Some companion files were not renamed because they already exist:\n${conflicts.join("\n")}`
        );
    }
}
