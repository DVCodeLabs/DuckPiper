import * as vscode from 'vscode';

export class NotebookLifecycleManager {
    constructor(context: vscode.ExtensionContext) {
        context.subscriptions.push(
            vscode.workspace.onDidChangeNotebookDocument(this.onNotebookChange.bind(this))
        );
    }

    private async onNotebookChange(e: vscode.NotebookDocumentChangeEvent) {
        if (e.notebook.notebookType !== 'dp-notebook') return;

        // Check for added cells
        for (const change of e.contentChanges) {
            for (const cell of change.addedCells) {
                if (cell.kind === vscode.NotebookCellKind.Code && cell.document.languageId === 'sql') {
                    await this.initializeCellDefaults(cell);
                }
            }
        }
    }

    private async initializeCellDefaults(cell: vscode.NotebookCell) {
        // If already has dp mode, skip (might be move or paste operation preserving meta usually)
        // But paste might drop custom metadata depending on VS Code behavior. 
        // We'll check if 'mode' is present.
        const currentMeta = cell.metadata.dp || {};
        if (currentMeta.mode) return;

        // Calculate defaults
        const nb = cell.notebook;

        // 1. Layer: Last used or default 'bronze'
        let layer = 'bronze';
        // Scan backwards for last sql cell with layer
        for (let i = cell.index - 1; i >= 0; i--) {
            const prev = nb.cellAt(i);
            if (prev.kind === vscode.NotebookCellKind.Code && prev.metadata.dp?.layer) {
                layer = prev.metadata.dp.layer;
                break;
            }
        }

        // 2. Output Name: step_{index+1} 3-digits
        // We need a unique name. Simplest is sequential index-based, but must check collisions.
        let stepNum = cell.index + 1;
        let candidate = `step_${String(stepNum).padStart(3, '0')}`;

        // Collision check (simple linear scan)
        const usedNames = new Set<string>();
        nb.getCells().forEach(c => {
            if (c !== cell && c.metadata.dp?.outputName) {
                usedNames.add(c.metadata.dp.outputName);
            }
        });

        // Loop until unused
        while (usedNames.has(candidate)) {
            stepNum++;
            candidate = `step_${String(stepNum).padStart(3, '0')}`;
        }

        const newMeta = {
            ...currentMeta,
            mode: 'transform', // Default mode
            layer: layer,
            materialize: 'table',
            outputName: candidate
        };

        const edit = new vscode.WorkspaceEdit();
        const nbEdit = vscode.NotebookEdit.updateCellMetadata(cell.index, { dp: newMeta });
        edit.set(nb.uri, [nbEdit]);

        // Apply without undo stop? Or with? 
        // Doing it immediately effectively merges with the add operation usually.
        await vscode.workspace.applyEdit(edit);
    }
}
