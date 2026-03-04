import * as vscode from "vscode";
import {
    isCanonicalSql,
    siblingUri,
    stripSuffix,
    withPath
} from "./bundleUtils";

async function safeDelete(uri: vscode.Uri) {
    try {
        await vscode.workspace.fs.delete(uri, { recursive: false, useTrash: true });
    } catch (error) {
        // Ignore if file doesn't exist or other errors (e.g. already deleted)
        if (error instanceof vscode.FileSystemError && error.code === 'FileNotFound') {
            return;
        }
    }
}

export async function handleDeletions(
    files: readonly vscode.Uri[]
): Promise<void> {

    for (const uri of files) {
        if (!isCanonicalSql(uri)) continue;

        const md = siblingUri(uri, ".md");
        const comments = siblingUri(uri, ".comments.json");
        const chart = siblingUri(uri, ".chart.json");
        const chartConfig = siblingUri(uri, ".chartconfig.json");
        const annotated = withPath(uri, stripSuffix(uri.path, ".sql") + ".annotated.sql");

        // Fire and forget deletions for siblings
        // We use Trash to be safe, matching mostly what user likely did for the SQL file
        await Promise.all([
            safeDelete(md),
            safeDelete(comments),
            safeDelete(chart),
            safeDelete(chartConfig),
            safeDelete(annotated)
        ]);

        // Note: queryIndex listens to file watcher separately, so we don't need to manually update it here.
        // It will detect the SQL deletion event on its own.
    }
}
