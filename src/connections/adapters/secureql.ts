import { DbAdapter } from './adapter';
import { getSchema, executeQuery, getKeyInfo, SecureQLRequestOptions, SecureQLApiError } from './secureqlClient';
import {
    ConnectionProfile,
    ConnectionSecrets,
    QueryResult,
    QueryRunOptions,
    QueryColumn,
    SchemaIntrospection,
    SchemaModel,
    TableModel,
    ColumnModel,
    IndexModel,
    RoutineModel,
    RoutineKind,
    RoutineParameterModel,
    NonQueryResult,
} from '../../core/types';

// ── Request options helpers ─────────────────────────────────────────────────

/**
 * Build request options, auto-resolving connection ID from the API key if not already set.
 * This handles the case where users skip the "Validate API Key" action.
 *
 * Always refreshes the allow_csv_export flag from the server so admin
 * changes take effect on the next query or schema refresh without
 * requiring the user to re-validate their API key.
 */
async function resolveAndBuildRequestOptions(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
): Promise<SecureQLRequestOptions> {
    const baseUrl = profile.secureqlBaseUrl;
    const apiKey = secrets.apiKey;

    if (!baseUrl) throw new Error('SecureQL Base URL is required.');
    if (!apiKey) throw new Error('API key is required.');

    // Always call /v1/key/me to refresh server-controlled flags
    const info = await getKeyInfo(baseUrl, apiKey);

    // Auto-resolve connection ID if not yet set
    if (!profile.secureqlConnectionId) {
        profile.secureqlConnectionId = String(info.connection_id);
    }
    if (!profile.secureqlTargetDbms) {
        profile.secureqlTargetDbms = info.dbms;
    }
    if (!profile.sqlDialect) {
        profile.sqlDialect = info.dbms;
    }

    // Always sync server-side CSV export flag
    profile.allowCsvExport = info.allow_csv_export;

    return { baseUrl, apiKey, connectionId: profile.secureqlConnectionId };
}

// ── Schema mapping ──────────────────────────────────────────────────────────

function mapSchemaResponse(
    raw: any,
    profileId: string,
    profileName: string,
): SchemaIntrospection {
    const tables: Array<{ schema_name: string; table_name: string; columns: any[] }> = raw?.tables ?? [];
    const schemaMap = new Map<string, { tables: TableModel[]; procedures: RoutineModel[]; functions: RoutineModel[] }>();

    for (const t of tables) {
        const sName = t.schema_name || 'default';
        if (!schemaMap.has(sName)) {
            schemaMap.set(sName, { tables: [], procedures: [], functions: [] });
        }

        const cols: ColumnModel[] = (t.columns ?? []).map((c: any) => ({
            name: c.column_name,
            type: c.data_type,
            nullable: c.is_nullable ?? undefined,
        }));

        const pk: string[] = (t.columns ?? [])
            .filter((c: any) => c.is_primary_key)
            .map((c: any) => c.column_name);

        // Map indexes if provided by the SecureQL API
        const rawIndexes: any[] = (t as any).indexes ?? [];
        const indexes: IndexModel[] = rawIndexes.map((idx: any) => ({
            name: idx.index_name ?? idx.name ?? '',
            columns: idx.columns ?? [],
            unique: idx.is_unique ?? idx.unique ?? false,
        })).filter((idx: IndexModel) => idx.columns.length > 0);

        schemaMap.get(sName)!.tables.push({
            name: t.table_name,
            columns: cols,
            primaryKey: pk.length > 0 ? pk : undefined,
            indexes: indexes.length > 0 ? indexes : undefined,
        });
    }

    const procedureRows: any[] = Array.isArray(raw?.procedures) ? raw.procedures : [];
    const functionRows: any[] = Array.isArray(raw?.functions) ? raw.functions : [];
    const routineRows: any[] = Array.isArray(raw?.routines) ? raw.routines : [];

    for (const row of procedureRows) {
        addRoutineToSchemaMap(schemaMap, row, 'procedure');
    }
    for (const row of functionRows) {
        addRoutineToSchemaMap(schemaMap, row, 'function');
    }
    for (const row of routineRows) {
        const parsedKind = normalizeRoutineKind(row?.routine_type ?? row?.kind);
        addRoutineToSchemaMap(schemaMap, row, parsedKind);
    }

    const schemas: SchemaModel[] = Array.from(schemaMap.entries()).map(([name, bucket]) => ({
        name,
        tables: bucket.tables,
        procedures: bucket.procedures,
        functions: bucket.functions,
    }));

    return {
        version: '0.2',
        generatedAt: new Date().toISOString(),
        connectionId: profileId,
        connectionName: profileName,
        dialect: 'secureql',
        schemas,
    };
}

function addRoutineToSchemaMap(
    schemaMap: Map<string, { tables: TableModel[]; procedures: RoutineModel[]; functions: RoutineModel[] }>,
    row: any,
    kind: RoutineKind,
): void {
    const schemaName = String(row?.schema_name ?? row?.routine_schema ?? row?.specific_schema ?? 'default') || 'default';
    const routineName = String(row?.routine_name ?? row?.name ?? '').trim();
    if (!routineName) {
        return;
    }

    if (!schemaMap.has(schemaName)) {
        schemaMap.set(schemaName, { tables: [], procedures: [], functions: [] });
    }
    const bucket = schemaMap.get(schemaName)!;

    const parameters = normalizeRoutineParameters(row?.parameters);
    const routine: RoutineModel = {
        name: routineName,
        kind,
        comment: asOptionalString(row?.comment ?? row?.routine_comment),
        returnType: asOptionalString(row?.return_type ?? row?.data_type),
        language: asOptionalString(row?.language ?? row?.routine_language),
        deterministic: typeof row?.deterministic === 'boolean' ? row.deterministic : undefined,
        schemaQualifiedName: asOptionalString(row?.schema_qualified_name ?? row?.full_name),
        signature: asOptionalString(row?.signature) ?? buildRoutineSignature(routineName, parameters),
        parameters,
    };

    if (kind === 'procedure') {
        bucket.procedures.push(routine);
    } else {
        bucket.functions.push(routine);
    }
}

function normalizeRoutineKind(rawKind: unknown): RoutineKind {
    const normalized = String(rawKind ?? '').trim().toLowerCase();
    return normalized === 'procedure' ? 'procedure' : 'function';
}

function normalizeRoutineParameters(raw: unknown): RoutineParameterModel[] | undefined {
    if (!Array.isArray(raw)) {
        return undefined;
    }

    const mapped = raw
        .map((item: any, index: number): RoutineParameterModel | undefined => {
            const name = String(item?.name ?? item?.parameter_name ?? `arg${index + 1}`).trim();
            if (!name) {
                return undefined;
            }

            const modeRaw = String(item?.mode ?? item?.parameter_mode ?? '').trim().toLowerCase();
            const mode = (modeRaw === 'in' || modeRaw === 'out' || modeRaw === 'inout' || modeRaw === 'variadic' || modeRaw === 'return')
                ? modeRaw
                : undefined;

            const positionCandidate = item?.position ?? item?.ordinal_position ?? index + 1;
            const position = typeof positionCandidate === 'number'
                ? positionCandidate
                : (Number.isFinite(Number(positionCandidate)) ? Number(positionCandidate) : undefined);

            return {
                name,
                mode,
                type: asOptionalString(item?.type ?? item?.data_type),
                position,
            };
        })
        .filter((item: RoutineParameterModel | undefined): item is RoutineParameterModel => Boolean(item));

    return mapped.length > 0 ? mapped : undefined;
}

function asOptionalString(value: unknown): string | undefined {
    if (typeof value !== 'string') {
        return undefined;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : undefined;
}

function buildRoutineSignature(name: string, parameters?: RoutineParameterModel[]): string {
    if (!parameters || parameters.length === 0) {
        return `${name}()`;
    }

    const args = [...parameters]
        .sort((a, b) => (a.position ?? 0) - (b.position ?? 0))
        .filter((parameter) => parameter.mode !== 'return')
        .map((parameter) => {
            const modePrefix = parameter.mode ? `${parameter.mode.toUpperCase()} ` : '';
            const typeSuffix = parameter.type ? ` ${parameter.type}` : '';
            return `${modePrefix}${parameter.name}${typeSuffix}`.trim();
        });

    return `${name}(${args.join(', ')})`;
}

// ── Query result mapping ────────────────────────────────────────────────────

interface SecureQLQueryReturn {
    affectedRows: number;
    error?: string;
    fields?: Array<{ field: string; colType: string }>;
    rows?: Record<string, unknown>[] | string;
    runtime?: number;
    queriesRun: number;
    [key: string]: unknown;
}

function mapQueryResponse(raw: any): QueryResult {
    const results: SecureQLQueryReturn[] = raw?.results ?? [];
    const log = raw?.log;
    const clientStart = Date.now();

    // Check for errors in any result
    for (const r of results) {
        if (r.error) {
            throw new SecureQLApiError(200, r.error);
        }
    }

    // Find first tabular result (has rows array + fields)
    const tabular = results.find(
        (r) => Array.isArray(r.rows) && Array.isArray(r.fields),
    );

    let warning: string | undefined;
    if (results.length > 1) {
        const tabularCount = results.filter((r) => Array.isArray(r.rows) && Array.isArray(r.fields)).length;
        if (tabularCount > 1) {
            warning = `Query returned ${tabularCount} result sets. Only the first tabular result is displayed.`;
        }
    }

    if (!tabular || !Array.isArray(tabular.rows) || !Array.isArray(tabular.fields)) {
        // DML or empty result
        const affected = results.reduce((sum, r) => sum + (r.affectedRows || 0), 0);
        return {
            columns: [],
            rows: [],
            rowCount: affected,
            elapsedMs: log?.runtime_ms ?? (Date.now() - clientStart),
            warning,
        };
    }

    const columns: QueryColumn[] = tabular.fields.map((f) => ({
        name: f.field,
        type: f.colType,
    }));

    // Keep rows as objects keyed by column name — AG Grid in runQL expects this format
    const objectRows = tabular.rows as Record<string, unknown>[];

    const elapsedMs = tabular.runtime ?? log?.runtime_ms ?? (Date.now() - clientStart);

    return {
        columns,
        rows: objectRows,
        rowCount: objectRows.length,
        elapsedMs,
        warning,
    };
}

// ── Adapter class ───────────────────────────────────────────────────────────

export class SecureQLAdapter implements DbAdapter {
    readonly dialect = 'secureql' as const;
    private _saveProfile?: (profile: ConnectionProfile) => Promise<void>;

    constructor(saveProfile?: (profile: ConnectionProfile) => Promise<void>) {
        this._saveProfile = saveProfile;
    }

    /**
     * Persist profile changes (e.g. allowCsvExport flag) back to disk
     * so they survive across sessions. Fire-and-forget to avoid blocking queries.
     */
    private persistProfile(profile: ConnectionProfile): void {
        // Unsaved form profiles do not have a stable ID yet. Persisting them here
        // would create an extra placeholder connection during "Test Connection".
        if (this._saveProfile && profile.id) {
            this._saveProfile(profile).catch(() => {
                // Silently ignore save errors — the in-memory value is still correct
            });
        }
    }

    async testConnection(profile: ConnectionProfile, secrets: ConnectionSecrets): Promise<void> {
        const opts = await resolveAndBuildRequestOptions(profile, secrets);
        this.persistProfile(profile);
        // testConnection validates by fetching schema — a 200 means success
        await getSchema(opts);
    }

    async runQuery(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        sql: string,
        _options: QueryRunOptions,
    ): Promise<QueryResult> {
        const opts = await resolveAndBuildRequestOptions(profile, secrets);
        this.persistProfile(profile);
        const raw = await executeQuery(opts, sql);
        return mapQueryResponse(raw);
    }

    async executeNonQuery(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        sql: string,
    ): Promise<NonQueryResult> {
        const opts = await resolveAndBuildRequestOptions(profile, secrets);
        this.persistProfile(profile);
        const raw = await executeQuery(opts, sql);
        const results: SecureQLQueryReturn[] = raw?.results ?? [];

        // Check for errors
        for (const r of results) {
            if (r.error) {
                throw new SecureQLApiError(200, r.error);
            }
        }

        const affected = results.reduce((sum, r) => sum + (r.affectedRows || 0), 0);
        return { affectedRows: affected };
    }

    async introspectSchema(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
    ): Promise<SchemaIntrospection> {
        const opts = await resolveAndBuildRequestOptions(profile, secrets);
        this.persistProfile(profile);
        const raw = await getSchema(opts);
        return mapSchemaResponse(raw, profile.id, profile.name);
    }
}
