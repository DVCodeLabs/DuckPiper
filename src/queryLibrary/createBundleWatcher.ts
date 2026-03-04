import * as vscode from 'vscode';
import * as path from 'path';
import { fileExists } from "../core/fsWorkspace";
import { Logger } from '../core/logger';

export async function handleCreations(files: readonly vscode.Uri[]) {
    // Race condition protection: wait for FS to settle
    await new Promise(resolve => setTimeout(resolve, 100));

    for (const uri of files) {
        // Only interested in .dpnb files
        if (!uri.path.endsWith('.dpnb')) {
            continue;
        }

        // Check if sibling .md exists
        const mdUri = uri.with({ path: uri.path.replace(/\.dpnb$/i, '.md') });
        if (await fileExists(mdUri)) {
            continue;
        }

        // Create default markdown
        try {
            const baseName = path.basename(uri.fsPath, '.dpnb');
            const title = baseName.split(/[_-]/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
            const today = new Date().toISOString().split('T')[0];
            const sourcePath = vscode.workspace.asRelativePath(uri, false);

            const content = `---
title: "${title}"
created_at: "${today}"
tags: []
source_path: "${sourcePath}"
---

<!-- DuckPiper:content:start -->
# Goal
- Describe the goal of this pipeline.

# Steps
- 

# Outputs
- 
<!-- DuckPiper:content:end -->
`;

            await vscode.workspace.fs.writeFile(mdUri, Buffer.from(content, 'utf8'));
        } catch (e) {
            Logger.error(`DuckPiper: Failed to create companion markdown for ${uri.fsPath}`, e);
        }
    }
}
