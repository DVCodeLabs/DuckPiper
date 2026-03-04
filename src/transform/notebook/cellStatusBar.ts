import * as vscode from 'vscode';

export class DPCellStatusBarProvider implements vscode.NotebookCellStatusBarItemProvider {
    provideCellStatusBarItems(
        cell: vscode.NotebookCell,
        _token: vscode.CancellationToken
    ): vscode.ProviderResult<vscode.NotebookCellStatusBarItem[]> {
        if (cell.notebook.notebookType !== 'dp-notebook' || cell.document.languageId !== 'sql') {
            return [];
        }

        const items: vscode.NotebookCellStatusBarItem[] = [];

        // Metadata extraction
        const meta = cell.metadata.dp || {};
        const outputName = meta.outputName;
        const mode = outputName ? 'transform' : 'analyze';
        const layer = meta.layer || 'bronze'; // default

        // 1. Engine Badge
        const engineItem = new vscode.NotebookCellStatusBarItem(
            '$(database) DuckDB',
            vscode.NotebookCellStatusBarAlignment.Left
        );
        engineItem.tooltip = 'Executed by DuckPiper Local DuckDB';
        items.push(engineItem);

        // 2. Mode Badge (Toggleable)
        const modeLabel = mode === 'transform' ? '$(beaker) Transform' : '$(search) Analyze';
        const modeItem = new vscode.NotebookCellStatusBarItem(
            modeLabel,
            vscode.NotebookCellStatusBarAlignment.Left
        );
        modeItem.tooltip = mode === 'transform' ? 'Mode: Transform (Click to switch to Analyze)' : 'Mode: Analyze (Click to switch to Transform)';
        modeItem.command = {
            command: 'dp.notebook.cell.toggleMode',
            title: 'Toggle Mode',
            arguments: [cell]
        };
        items.push(modeItem);

        // 3. Layer Selector (Always Visible)
        const layerItem = new vscode.NotebookCellStatusBarItem(
            `Destination Layer: ${layer}`,
            vscode.NotebookCellStatusBarAlignment.Left
        );
        layerItem.tooltip = 'Click to change target layer';
        layerItem.command = {
            command: 'dp.notebook.cell.setLayer',
            title: 'Set Layer',
            arguments: [cell]
        };
        items.push(layerItem);

        // 4. Output Name (Always Visible)
        const outputLabel = outputName ? `Output: ${outputName}` : `Output: (set)`;
        const outputItem = new vscode.NotebookCellStatusBarItem(
            outputLabel,
            vscode.NotebookCellStatusBarAlignment.Left
        );
        outputItem.tooltip = outputName
            ? 'Click to rename output'
            : 'Click to set output name (switches to Transform mode)';
        outputItem.command = {
            command: 'dp.notebook.cell.setOutputName',
            title: 'Set Output Name',
            arguments: [cell]
        };
        items.push(outputItem);

        return items;
    }
}
