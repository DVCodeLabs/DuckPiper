import { ConnectionProfile, ConnectionSecrets, QueryResult, QueryRunOptions, SchemaIntrospection, DbDialect, NonQueryResult } from '../../core/types';

export interface DbAdapter {
    readonly dialect: DbDialect;

    testConnection(profile: ConnectionProfile, secrets: ConnectionSecrets): Promise<void>;

    runQuery(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        sql: string,
        options: QueryRunOptions
    ): Promise<QueryResult>;

    executeNonQuery(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        sql: string
    ): Promise<NonQueryResult>;

    introspectSchema(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets
    ): Promise<SchemaIntrospection>;

    exportTable?(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        schema: string,
        table: string,
        format: 'csv' | 'json',
        outputUri: import('vscode').Uri
    ): Promise<void>;
}
