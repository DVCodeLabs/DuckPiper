import * as vscode from 'vscode';
import * as path from 'path';
import { LocalDuckDB } from './localDuckDB';
import { PipelineRepository } from '../pipelines/pipelineRepository';

export class DuckDBLifecycleManager {
    private static instance: DuckDBLifecycleManager;
    private duckdb: LocalDuckDB;
    private repo: PipelineRepository;

    private constructor() {
        this.duckdb = LocalDuckDB.getInstance();
        this.repo = PipelineRepository.getInstance();
    }

    public static getInstance(): DuckDBLifecycleManager {
        if (!DuckDBLifecycleManager.instance) {
            DuckDBLifecycleManager.instance = new DuckDBLifecycleManager();
        }
        return DuckDBLifecycleManager.instance;
    }

    /**
     * Delete a table/view from DuckDB and update lineage.
     */
    async deleteObject(schema: string, table: string): Promise<boolean> {
        const catalog = this.duckdb.getCatalogName();
        const fullTableName = `"${catalog}".${schema}.${table}`;

        // 1. Confirm with User - include detailed warning
        const warningMessage = [
            `Delete ${fullTableName}?`,
            '',
            'This action will:',
            `• Drop the table/view from DuckDB`,
            `• Mark this table as "deleted" in all pipeline lineage files`,
            `• Downstream tables that depend on this may show broken lineage`,
            '',
            'This cannot be undone.'
        ].join('\n');

        const choice = await vscode.window.showWarningMessage(
            warningMessage,
            { modal: true, detail: 'Deleting this table will affect any pipelines that reference it.' },
            "Delete"
        );

        if (choice !== "Delete") return false;

        try {
            // 2. Drop Object 
            // LocalDuckDB.runUpdate executes SQL
            await this.duckdb.runUpdate(`DROP VIEW IF EXISTS ${fullTableName}`);
            await this.duckdb.runUpdate(`DROP TABLE IF EXISTS ${fullTableName}`);

            // 3. Update Lineage
            await this.markLineageAsDeleted(schema, table, 'user');

            // 4. Refresh UI
            vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
            vscode.window.showInformationMessage(`Deleted ${fullTableName}`);
            return true;

        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to delete ${fullTableName}: ${msg}`);
            return false;
        }
    }

    /**
     * Export table to CSV
     */
    async exportToCsv(schema: string, table: string): Promise<void> {
        const defaultFilename = `${schema}__${table}.csv`;

        const uri = await vscode.window.showSaveDialog({
            defaultUri: vscode.Uri.file(path.join(vscode.workspace.rootPath || '', defaultFilename)),
            filters: { 'CSV': ['csv'] }
        });

        if (!uri) return;

        try {
            // COPY (SELECT * FROM schema.table) TO 'path' (HEADER, DELIMITER ',')
            const catalog = this.duckdb.getCatalogName();
            const copySql = `COPY (SELECT * FROM "${catalog}".${schema}.${table}) TO '${uri.fsPath}' (HEADER, DELIMITER ',')`;
            await this.duckdb.runUpdate(copySql);

            vscode.window.showInformationMessage(`Exported ${schema}.${table} to CSV`);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Export failed: ${msg}`);
        }
    }

    /**
     * Reset a specific bronze/silver/gold layer
     */
    async resetLayer(layer: string): Promise<void> {
        if (!['bronze', 'silver', 'gold'].includes(layer)) return;

        const choice = await vscode.window.showWarningMessage(
            `Reset ${layer}? This deletes ALL tables/views in ${layer} and may break lineage.`,
            { modal: true },
            "Reset Layer"
        );

        if (choice !== "Reset Layer") return;

        try {
            const catalog = this.duckdb.getCatalogName();
            await this.duckdb.runUpdate(`DROP SCHEMA IF EXISTS "${catalog}".${layer} CASCADE`);
            await this.duckdb.runUpdate(`CREATE SCHEMA "${catalog}".${layer}`);

            // Update all lineage nodes in this layer to deleted
            await this.markLayerAsDeleted(layer, 'system');

            vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
            vscode.window.showInformationMessage(`Reset ${layer} layer.`);
        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to reset layer ${layer}: ${msg}`);
        }
    }

    /**
    * Reset pipeline outputs
    */
    async resetPipelineOutputs(pipelineId: string): Promise<void> {
        // 1. Get Lineage to find outputs
        const lineage = await this.repo.loadLineage(pipelineId);
        if (!lineage) {
            vscode.window.showErrorMessage("Lineage not found for this pipeline.");
            return;
        }

        const choice = await vscode.window.showWarningMessage(
            `Reset pipeline outputs? This deletes tables/views created by this pipeline.`,
            { modal: true },
            "Reset Outputs"
        );
        if (choice !== "Reset Outputs") return;

        // 2. Identify outputs (created by this pipeline's cells)
        const outputs = lineage.tables.filter(t =>
            t.createdBy && ['bronze', 'silver', 'gold'].includes(t.schema)
        );

        if (outputs.length === 0) {
            vscode.window.showInformationMessage("No managed outputs found for this pipeline.");
            return;
        }

        try {
            const catalog = this.duckdb.getCatalogName();
            for (const table of outputs) {
                const fullTableName = `"${catalog}".${table.schema}.${table.name}`;
                await this.duckdb.runUpdate(`DROP VIEW IF EXISTS ${fullTableName}`);
                await this.duckdb.runUpdate(`DROP TABLE IF EXISTS ${fullTableName}`);

                // Update lineage using repo method to propagate
                await this.repo.updateTableStatusInAllPipelines(table.fqn, 'deleted', 'system');
            }

            vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
            vscode.window.showInformationMessage(`Reset ${outputs.length} outputs.`);

        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Failed to reset outputs: ${msg}`);
        }
    }

    /**
     * Reconcile lineage with actual DuckDB state
     * Checks if tables exist and updates status if missing
     */
    async reconcileLineage(pipelineId: string): Promise<void> {
        const lineage = await this.repo.loadLineage(pipelineId);
        if (!lineage) return;

        let changed = false;

        for (const table of lineage.tables) {
            // Only check managed schemas
            if (!['bronze', 'silver', 'gold'].includes(table.schema)) continue;

            // Check existence
            const exists = await this.duckdb.checkTableExists(table.schema, table.name);

            if (exists) {
                if (table.status !== 'present') {
                    table.status = 'present';
                    table.deletedAt = undefined;
                    table.deletedBy = undefined;
                    changed = true;
                }
            } else {
                // It is missing
                // If already marked deleted, keep it deleted
                if (table.status === 'deleted') continue;

                // Else mark as missing
                if (table.status !== 'missing') {
                    table.status = 'missing';
                    changed = true;
                }
            }
        }

        if (changed) {
            await this.repo.saveLineage(lineage);
        }
    }

    /**
     * Mark all lineage nodes for this table as deleted in ALL pipeline files.
     */
    private async markLineageAsDeleted(schema: string, table: string, by: 'user' | 'system'): Promise<void> {
        const fqn = `${schema}.${table}`;
        await this.repo.updateTableStatusInAllPipelines(fqn, 'deleted', by);
    }

    private async markLayerAsDeleted(layer: string, by: 'user' | 'system'): Promise<void> {
        await this.repo.updateLayerStatusInAllPipelines(layer, 'deleted', by);
    }
}
