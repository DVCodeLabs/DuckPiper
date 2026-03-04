/**
 * Script Runner
 *
 * Executes a list of SQL statements sequentially with stop-on-error semantics.
 * Returns a ScriptExecutionResult with per-statement status and optional
 * tabular data from the last result-returning statement.
 */

import { DbAdapter } from '../connections/adapters/adapter';
import { ConnectionProfile, ConnectionSecrets, QueryResult, ScriptStatementResult, ScriptExecutionResult } from './types';
import { SplitStatement } from './sqlSplitter';
import { applyRowLimit, isResultReturningStatement } from './sqlLimitHelper';

export interface ScriptRunOptions {
    maxRows: number;
    bypassLimit: boolean;
}

export async function executeScript(
    statements: SplitStatement[],
    adapter: DbAdapter,
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    options: ScriptRunOptions,
): Promise<ScriptExecutionResult> {
    const statementResults: ScriptStatementResult[] = [];
    let lastTabularResult: QueryResult | undefined;
    let executedCount = 0;
    let failedAtIndex: number | undefined;

    for (let i = 0; i < statements.length; i++) {
        const stmt = statements[i];
        const index = i + 1;  // 1-based

        // If a prior statement failed, mark remaining as skipped
        if (failedAtIndex !== undefined) {
            statementResults.push({
                index,
                sql: stmt.sql,
                status: 'skipped',
            });
            continue;
        }

        const startTime = Date.now();

        try {
            const isTabular = isResultReturningStatement(stmt.sql);
            let sqlToRun = stmt.sql;
            let effectiveLimit = 0;

            // Apply row limit only to result-returning statements
            if (isTabular && !options.bypassLimit && options.maxRows > 0) {
                const limitResult = applyRowLimit(stmt.sql, options.maxRows);
                sqlToRun = limitResult.sql;
                effectiveLimit = limitResult.effectiveLimit;
            }

            const result = await adapter.runQuery(profile, secrets, sqlToRun, {
                maxRows: effectiveLimit,
            });

            const elapsedMs = Date.now() - startTime;
            executedCount++;

            // Detect tabular vs non-tabular from actual result
            const hasColumns = result.columns && result.columns.length > 0;

            if (hasColumns) {
                lastTabularResult = result;
                statementResults.push({
                    index,
                    sql: stmt.sql,
                    status: 'success',
                    kind: 'tabular',
                    rowCount: result.rowCount,
                    elapsedMs,
                });
            } else {
                statementResults.push({
                    index,
                    sql: stmt.sql,
                    status: 'success',
                    kind: 'non_tabular',
                    affectedRows: result.rowCount ?? null,
                    elapsedMs,
                });
            }
        } catch (e: unknown) {
            const elapsedMs = Date.now() - startTime;
            executedCount++;
            failedAtIndex = index;

            statementResults.push({
                index,
                sql: stmt.sql,
                status: 'error',
                elapsedMs,
                errorMessage: e instanceof Error ? e.message : String(e),
            });
        }
    }

    return {
        mode: 'script',
        totalStatements: statements.length,
        executedStatements: executedCount,
        failedAtIndex,
        statements: statementResults,
        lastTabularResult,
    };
}
