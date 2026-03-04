import * as vscode from 'vscode';
import * as crypto from 'crypto';
import { TextEncoder } from 'util';

import { NotebookData } from './notebookTypes';

export function registerNotebookCommands(context: vscode.ExtensionContext) {
    const createNotebook = async () => {
        const wsFolder = vscode.workspace.workspaceFolders?.[0];
        if (!wsFolder) {
            vscode.window.showErrorMessage("Open a workspace folder to create a notebook.");
            return;
        }

        const name = await vscode.window.showInputBox({
            prompt: "Notebook Name",
            placeHolder: "customer_revenue_pipeline"
        });
        if (!name) return;

        const fileName = name.endsWith('.dpnb') ? name : `${name}.dpnb`;
        // Use VS Code URI construction
        const notebooksDirUri = vscode.Uri.joinPath(wsFolder.uri, 'DP', 'notebooks');

        // Ensure directory exists
        try {
            await vscode.workspace.fs.stat(notebooksDirUri);
        } catch {
            await vscode.workspace.fs.createDirectory(notebooksDirUri);
        }

        const fileUri = vscode.Uri.joinPath(notebooksDirUri, fileName);

        // Initial Content
        const initialData: NotebookData = {
            version: "0.1",
            name: name,
            cells: [
                {
                    id: crypto.randomUUID(),
                    type: 'sql',
                    layer: 'bronze',
                    materialize: 'table',
                    outputName: 'raw_data',
                    sql: 'SELECT 1 as id'
                }
            ]
        };

        await vscode.workspace.fs.writeFile(fileUri, new TextEncoder().encode(JSON.stringify(initialData, null, 2)));

        // Create companion markdown file (same pattern as createSqlFile)
        const mdUri = vscode.Uri.joinPath(notebooksDirUri, fileName.replace(/\.dpnb$/i, '.md'));
        const niceTitle = name.split(/[_-]/).map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        const today = new Date().toISOString().split('T')[0];
        const sourcePath = vscode.workspace.asRelativePath(fileUri, false);

        const mdContent = `---
title: "${niceTitle}"
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
        await vscode.workspace.fs.writeFile(mdUri, new TextEncoder().encode(mdContent));

        await openNotebookFile(fileUri);
    };

    const openNotebook = async () => {
        const uris = await vscode.window.showOpenDialog({
            canSelectMany: false,
            filters: { 'DuckPiper Notebooks': ['dpnb'] }
        });
        if (uris && uris.length > 0) {
            await openNotebookFile(uris[0]);
        }
    };

    const openNotebookFile = async (uri: vscode.Uri) => {
        await vscode.commands.executeCommand('vscode.open', uri);
    };

    const setCellLayer = async (cell: vscode.NotebookCell) => {
        const layers = ['bronze', 'silver', 'gold'];
        const _currentLayer = cell.metadata.dp?.layer || 'bronze';
        const currentName = cell.metadata.dp?.outputName || '';

        const picked = await vscode.window.showQuickPick(layers, {
            placeHolder: 'Select target layer',
            title: 'Set Transform Layer'
        });

        if (picked) {
            const edit = new vscode.WorkspaceEdit();
            const newMeta = { ...cell.metadata };
            let newName = currentName;

            // Defaults map
            const defaults: Record<string, string> = {
                'bronze': 'raw_data',
                'silver': 'stg_data',
                'gold': 'mart_data'
            };

            // If name is empty OR matches a default from another layer, update it
            const isDefault = Object.values(defaults).includes(currentName);
            if (!currentName || isDefault) {
                newName = defaults[picked] || currentName;
            }

            newMeta.dp = { ...newMeta.dp, layer: picked, outputName: newName };

            const nbEdit = vscode.NotebookEdit.updateCellMetadata(cell.index, newMeta);
            edit.set(cell.notebook.uri, [nbEdit]);

            await vscode.workspace.applyEdit(edit);
        }
    };

    const setCellOutputName = async (cell: vscode.NotebookCell) => {
        const current = cell.metadata.dp?.outputName || '';

        const input = await vscode.window.showInputBox({
            prompt: 'Set Output Name (snake_case required).',
            value: current,
            validateInput: (val) => {
                if (!val) return null; // allow empty to clear
                if (!/^[a-z0-9_]+$/.test(val)) return "Name must be snake_case (lowercase alphanumeric + underscores only).";
                return null;
            }
        });

        if (input !== undefined) {
            const edit = new vscode.WorkspaceEdit();
            const newMeta = { ...cell.metadata };

            // Just update name, do NOT switch mode automatically (output name editing is independent in new spec)
            const metaUpdate: Record<string, unknown> = { ...newMeta.dp, outputName: input };

            newMeta.dp = metaUpdate;

            const nbEdit = vscode.NotebookEdit.updateCellMetadata(cell.index, newMeta);
            edit.set(cell.notebook.uri, [nbEdit]);

            await vscode.workspace.applyEdit(edit);
        }
    };

    const toggleCellMode = async (cell: vscode.NotebookCell) => {
        const currentMode = cell.metadata.dp?.mode || 'transform';
        const newMode = currentMode === 'transform' ? 'analyze' : 'transform';

        const edit = new vscode.WorkspaceEdit();
        const newMeta = { ...cell.metadata };
        const metaUpdate: Record<string, unknown> = { ...newMeta.dp, mode: newMode };

        // Logic: When switching TO transform, ensure outputName exists
        if (newMode === 'transform' && !metaUpdate.outputName) {
            // Create default if missing
            const stepNum = cell.index + 1;
            metaUpdate.outputName = `step_${String(stepNum).padStart(3, '0')}`;
        }
        // Logic: When switching TO analyze, spec says "recommend clearing it" but also "set outputName = ''"
        // Let's clear it for safety as requested
        if (newMode === 'analyze') {
            metaUpdate.outputName = '';
        }

        newMeta.dp = metaUpdate;
        const nbEdit = vscode.NotebookEdit.updateCellMetadata(cell.index, newMeta);
        edit.set(cell.notebook.uri, [nbEdit]);
        await vscode.workspace.applyEdit(edit);
    };

    context.subscriptions.push(
        vscode.commands.registerCommand('dp.transform.notebook.create', createNotebook),
        vscode.commands.registerCommand('dp.transform.notebook.open', openNotebook),
        vscode.commands.registerCommand('dp.transform.notebook.openFile', openNotebookFile),
        vscode.commands.registerCommand('dp.notebook.cell.setLayer', setCellLayer),
        vscode.commands.registerCommand('dp.notebook.cell.setOutputName', setCellOutputName),
        vscode.commands.registerCommand('dp.notebook.cell.toggleMode', toggleCellMode),
        vscode.commands.registerCommand('dp.notebook.generateMarkdownDoc', async () => {
            const { generateNotebookMarkdownDoc } = require('../ai/docGenerator');
            await generateNotebookMarkdownDoc(context);
        }),
        vscode.commands.registerCommand('dp.notebook.openMarkdownDoc', async () => {
            const { openNotebookMarkdownDoc } = require('../ai/docGenerator');
            await openNotebookMarkdownDoc(context);
        }),
        vscode.commands.registerCommand('dp.notebook.openLineage', async () => {
            const editor = vscode.window.activeNotebookEditor;
            if (!editor) {
                vscode.window.showWarningMessage("Open a notebook to view lineage.");
                return;
            }
            const { PipelineRepository } = require('../pipelines/pipelineRepository');
            const repo = PipelineRepository.getInstance();
            const pipeline = await repo.findPipelineByUri(editor.notebook.uri);

            if (pipeline) {
                vscode.commands.executeCommand("dp.pipeline.openLineage", pipeline.pipelineId);
            } else {
                vscode.window.showInformationMessage("No lineage found for this notebook. Try running it first.");
            }
        })
    );
}
