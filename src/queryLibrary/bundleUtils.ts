import * as vscode from "vscode";
import { readJson, writeJson } from "../core/fsWorkspace";

export const ignoredNewUris = new Set<string>();

export function ignoreNewUri(uri: vscode.Uri) {
    const key = uri.toString();
    ignoredNewUris.add(key);
    setTimeout(() => ignoredNewUris.delete(key), 2000);
}

export function stripSuffix(path: string, suffix: string) {
    return path.toLowerCase().endsWith(suffix) ? path.slice(0, -suffix.length) : path;
}

export function withPath(uri: vscode.Uri, newPath: string) {
    return uri.with({ path: newPath });
}

export function siblingUri(sqlUri: vscode.Uri, newExt: string): vscode.Uri {
    const base = stripSuffix(sqlUri.path, ".sql");
    return withPath(sqlUri, base + newExt);
}

// Update JSON sourcePath fields (comments/chart)
export async function patchJsonSourcePath(uri: vscode.Uri, newSqlRelPath: string) {
    const obj: Record<string, unknown> = await readJson<Record<string, unknown>>(uri);
    if (typeof obj !== "object" || obj === null) return;

    let changed = false;
    if (typeof obj.sourcePath === "string" && obj.sourcePath !== newSqlRelPath) {
        obj.sourcePath = newSqlRelPath;
        changed = true;
    }

    if (changed) {
        await writeJson(uri, obj);
    }
}

// Update markdown frontmatter source_path if present (minimal, safe)
export async function patchMdFrontmatter(uri: vscode.Uri, newSqlRelPath: string) {
    const bytes = await vscode.workspace.fs.readFile(uri);
    const text = Buffer.from(bytes).toString("utf8");

    // Only patch if it looks like frontmatter
    if (!text.startsWith("---")) return;

    const end = text.indexOf("\n---", 3);
    if (end === -1) return;

    const fm = text.slice(0, end + 4); // include closing ---
    const body = text.slice(end + 4);

    let updated = fm;
    if (fm.includes("\nsource_path:")) {
        updated = fm.replace(/\nsource_path:\s*["']?.*?["']?\n/, `\nsource_path: "${newSqlRelPath}"\n`);
    } else {
        return;
    }

    if (updated === fm) return;

    await vscode.workspace.fs.writeFile(uri, Buffer.from(updated + body, "utf8"));
}

export function isCanonicalSql(uri: vscode.Uri): boolean {
    const p = uri.path.toLowerCase();
    if (!p.endsWith(".sql")) return false;
    // exclude derived
    if (p.endsWith(".annotated.sql") || p.endsWith(".commented.sql")) return false;
    return true;
}
