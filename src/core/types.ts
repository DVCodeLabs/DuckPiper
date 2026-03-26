
export type DbDialect =
    | "postgres"
    | "mysql"
    | "sqlite"
    | "duckdb"
    | "snowflake"
    | "bigquery"
    | "redshift"
    | "databricks"
    | "trino"
    | "mssql"
    | "oracle"
    | (string & {});

export interface DPProviderDescriptor {
    providerId: string;
    displayName: string;
    dialect: string;
    icon?: string;
    formSchema: DPConnectionFormSchema;
    supports: {
        ssl: boolean;
        oauth: boolean;
        keypair: boolean;
        introspection: boolean;
        cancellation: boolean;
    };
}

export type DPConnectionFormTab = 'connection' | 'auth' | 'ssh';
export type DPConnectionFieldStorage = 'profile' | 'secrets' | 'local';

export interface DPConnectionFormOption {
    value: string;
    label: string;
}

export interface DPConnectionFieldVisibility {
    storage?: DPConnectionFieldStorage;
    key: string;
    equals?: string | number | boolean;
    notEquals?: string | number | boolean;
    truthy?: boolean;
    and?: DPConnectionFieldVisibility[];
}

export interface DPConnectionFieldPicker {
    mode?: 'open' | 'save';
    title?: string;
    openLabel?: string;
    canSelectFiles?: boolean;
    canSelectFolders?: boolean;
    filters?: Record<string, string[]>;
}

export interface DPConnectionFieldSchema {
    key: string;
    label: string;
    type: 'text' | 'password' | 'number' | 'checkbox' | 'select' | 'radio' | 'file' | 'textarea';
    tab?: DPConnectionFormTab;
    storage?: DPConnectionFieldStorage;
    required?: boolean;
    placeholder?: string;
    description?: string;
    defaultValue?: string | number | boolean;
    options?: DPConnectionFormOption[];
    min?: number;
    max?: number;
    step?: number;
    width?: 'full' | 'half';
    visibleWhen?: DPConnectionFieldVisibility;
    picker?: DPConnectionFieldPicker;
}

export interface DPConnectionFormAction {
    id: string;
    label: string;
    tab?: DPConnectionFormTab;
    style?: 'primary' | 'secondary' | 'link';
    payloadKeys?: string[];
}

export interface DPProviderActionStatus {
    type: 'info' | 'error' | 'success';
    text: string;
}

export interface DPProviderActionResult {
    profilePatch?: Record<string, unknown>;
    secretsPatch?: Record<string, unknown>;
    localPatch?: Record<string, unknown>;
    status?: DPProviderActionStatus;
}

export type DPProviderActionHandler = (
    actionId: string,
    payload: Record<string, unknown>
) => Promise<DPProviderActionResult | void> | DPProviderActionResult | void;

export interface DPConnectionReuseSchema {
    disabled?: boolean;
    label?: string;
    sourceDialects?: string[];
    includeProfileKeys?: string[];
    excludeProfileKeys?: string[];
    includeSecretKeys?: string[];
    excludeSecretKeys?: string[];
    autoApplyWhenSingle?: boolean;
}

export interface DPConnectionFormSchema {
    fields: DPConnectionFieldSchema[];
    actions?: DPConnectionFormAction[];
    reuse?: DPConnectionReuseSchema;
}

export interface ConnectionProfile {
    id: string;                 // uuid
    name: string;               // user friendly
    dialect: DbDialect;
    connectionTag?: string;     // optional safety tag, e.g. production/staging/dev

    // Non-secret fields only:
    host?: string;
    port?: number;
    database?: string; // or catalog in some systems
    schema?: string;
    username?: string;
    ssl?: boolean;
    sslMode?: string; // disable, require, verify-ca, verify-full
    authMode?: string; // password, oauth, keypair, etc.
    credentialStorageMode?: 'session' | 'secretStorage' | 'browser';
    sqlDialect?: DbDialect;

    // For file-based DBs
    filePath?: string;          // sqlite/duckdb

    // SecureQL connector fields
    secureqlBaseUrl?: string;
    secureqlConnectionId?: string;
    secureqlTargetDbms?: string;

    // Optional extras:
    warehouse?: string;         // snowflake/databricks
    httpPath?: string;          // databricks/trino
    account?: string;           // snowflake
    role?: string;              // snowflake
    projectId?: string;         // bigquery
    privateKeyPath?: string;    // snowflake keypair auth path
    allowCsvExport?: boolean;
    allowDataEdit?: boolean;

    // SSH tunnel (non-secret)
    sshEnabled?: boolean;
    sshHost?: string;
    sshPort?: number;
    sshUsername?: string;
    sshAuthMethod?: 'password' | 'privateKey';
    sshPrivateKeyPath?: string;

    createdAt: string;          // ISO
    updatedAt: string;          // ISO;
    isLocalDuckPiperCacheDb?: boolean; // True if this is the local DuckDB cache
}

export interface ConnectionSecrets {
    password?: string;
    token?: string;
    // privateKeyPem?: string;     // DEPRECATED: use profile.privateKeyPath
    privateKeyPassphrase?: string;
    oauthRefreshToken?: string;

    // SecureQL
    apiKey?: string;

    // SSH tunnel secrets
    sshPassword?: string;
    sshPrivateKey?: string;
    sshPrivateKeyPassphrase?: string;
}

export interface QueryRunOptions {
    maxRows: number;            // default 10000
    timeoutMs?: number;
}

export interface QueryColumn {
    name: string;
    type?: string;              // adapter native type
    normalizedType?: string;    // DuckPiper normalization
}

export interface QueryResult {
    columns: QueryColumn[];
    rows: unknown[];
    rowCount: number;
    elapsedMs: number;
    warning?: string;
    meta?: QueryResultMeta;
}

export interface QueryResultSource {
    catalog?: string;
    schema?: string;
    table: string;
}

export interface QueryResultEditableMeta {
    enabled: boolean;
    reason?: string;
    primaryKeyColumns: string[];
    editableColumns: string[];
}

export interface QueryResultMeta {
    resultId: string;
    source?: QueryResultSource;
    editable: QueryResultEditableMeta;
}

export interface ResultsetCellChange {
    column: string;
    oldValue: unknown;
    newValue: unknown;
}

export interface ResultsetRowEdit {
    rowKey: Record<string, unknown>;
    changes: ResultsetCellChange[];
}

export interface ApplyResultsetEditsRequest {
    resultId: string;
    source: QueryResultSource;
    edits: ResultsetRowEdit[];
}

export interface ApplyResultsetEditsRowResult {
    rowKey: Record<string, unknown>;
    status: 'applied' | 'conflict' | 'error';
    message?: string;
}

export interface ApplyResultsetEditsResult {
    ok: boolean;
    summary: {
        applied: number;
        conflicted: number;
        failed: number;
    };
    rowResults: ApplyResultsetEditsRowResult[];
}

export interface NonQueryResult {
    affectedRows: number | null;
}

// Script (multi-statement) execution
export interface ScriptStatementResult {
    index: number;                          // 1-based position
    sql: string;                            // statement text (trimmed)
    status: 'success' | 'error' | 'skipped';
    kind?: 'tabular' | 'non_tabular';
    affectedRows?: number | null;
    rowCount?: number;
    elapsedMs?: number;
    errorMessage?: string;
}

export interface ScriptExecutionResult {
    mode: 'script';
    totalStatements: number;
    executedStatements: number;
    failedAtIndex?: number;                 // 1-based
    statements: ScriptStatementResult[];
    lastTabularResult?: QueryResult;        // for grid display
}

export type RoutineKind = "procedure" | "function";

export interface RoutineParameterModel {
    name: string;
    mode?: "in" | "out" | "inout" | "variadic" | "return";
    type?: string;
    position?: number;
}

export interface RoutineModel {
    name: string;
    kind: RoutineKind;
    comment?: string;
    returnType?: string;
    language?: string;
    deterministic?: boolean;
    schemaQualifiedName?: string;
    signature?: string;
    parameters?: RoutineParameterModel[];
}

// Normalized Schema Model
export interface SchemaIntrospection {
    version: "0.1" | "0.2";
    generatedAt: string;          // ISO
    connectionId: string;
    connectionName?: string;
    dialect: DbDialect;
    docPath?: string;             // Absolute path to description.json
    customRelationshipsPath?: string;  // Absolute path to custom.relationships.json
    schemas: SchemaModel[];
}

export interface SchemaModel {
    name: string;                 // schema name (or dataset)
    tables: TableModel[];
    views?: TableModel[];         // database views (separate from tables)
    procedures?: RoutineModel[];
    functions?: RoutineModel[];
}

export interface TableModel {
    name: string;
    comment?: string;
    columns: ColumnModel[];
    primaryKey?: string[];        // col names
    foreignKeys?: ForeignKeyModel[];
    indexes?: IndexModel[];
}

export interface IndexModel {
    name: string;
    columns: string[];
    unique?: boolean;
}

export interface ForeignKeyModel {
    name?: string;               // constraint name
    column: string;              // local column
    foreignSchema: string;
    foreignTable: string;
    foreignColumn: string;
}

// Custom relationships defined by users (not auto-detected)
export interface CustomRelationship {
    source: string;              // "schema.table"
    sourceColumn: string;
    target: string;              // "schema.table"
    targetColumn: string;
}

export interface CustomRelationshipsFile {
    version: "0.1";
    connectionId: string;
    connectionName?: string;
    relationships: CustomRelationship[];
}

export interface ColumnModel {
    name: string;
    type: string;                 // adapter native type
    normalizedType?: string;      // optional normalized type
    nullable?: boolean;
    comment?: string;
}

// Meta + Relationships Types
export interface SchemaMetaFile {
    __dpHeader: string;        // "#Descriptions generated by AI (via DuckPiper open source extension)"
    version: "0.1";
    generatedAt: string;
    entities: Record<string, EntityMeta>;  // key: "schema.table"
    columns: Record<string, ColumnMeta>;   // key: "schema.table.column"
}

export interface EntityMeta {
    description?: string;
    businessMeaning?: string;
    grain?: string;
    keyColumns?: string[];
    commonFilters?: string[];
    qualityNotes?: string[];
    synonyms?: string[];
}

export interface ColumnMeta {
    description?: string;
    exampleValues?: string[];
    pii?: boolean;
    joinHints?: string[];
}

export interface SchemaRelationshipsFile {
    __dpHeader: string;
    version: "0.1";
    generatedAt: string;
    relationships: RelationshipEdge[];
}

export interface RelationshipEdge {
    from: string;                 // "schema.table.column"
    to: string;                   // "schema.table.column"
    type: "one-to-one" | "one-to-many" | "many-to-one" | "many-to-many";
    confidence?: number;          // 0..1
    note?: string;
}

// Canonical SQL Hashing
export interface CanonicalSql {
    canonicalText: string;        // after stripping comments/whitespace
    sqlHash: string;              // sha256 of canonicalText
}

// Query Index
export interface QueryIndexFile {
    version: "0.1";
    generatedAt: string;
    queries: QueryIndexEntry[];
}

export interface QueryIndexEntry {
    path: string;                 // workspace-relative
    title?: string;               // first comment line (optional)
    sqlHash: string;              // sha256(canonicalSql)
    dialectHint?: DbDialect;      // optional
    tables?: string[];            // best-effort
    updatedAt: string;            // ISO
}

// Comment Overlay
export type CommentStyle = "lineAbove" | "inlineVirtual" | "gutterNote" | "hoverOnly";

export interface CommentOverlayFile {
    version: "0.1";
    generatedAt: string;
    sourcePath: string;           // workspace-relative path to canonical .sql
    sourceHash: string;           // sqlHash at generation time
    commentStyle: CommentStyle;
    comments: InlineCommentAnchor[];
}

export interface InlineCommentAnchor {
    line: number;                 // 1-based line in canonical .sql
    text: string;
}

// Chart Configuration
export interface ChartConfig {
    type: 'bar' | 'line' | 'pie' | 'doughnut' | 'scatter' | 'radar';
    title?: string;
    labelColumn?: string;
    datasetColumns: string[];
    datasetStyles?: Record<string, DatasetStyle>;
}

export interface DatasetStyle {
    backgroundColor?: string;
    borderColor?: string;
    borderWidth?: number;
}

export interface ChartConfigPayload {
    version: number;
    sourcePath?: string;
    chart: ChartConfig;
    updatedAt: string;
}
