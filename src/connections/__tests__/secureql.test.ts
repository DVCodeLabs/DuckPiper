jest.mock('../adapters/secureqlClient', () => ({
    getKeyInfo: jest.fn(),
    getSchema: jest.fn(),
    executeQuery: jest.fn(),
    SecureQLApiError: class SecureQLApiError extends Error {
        constructor(public statusCode: number, public userMessage: string) {
            super(userMessage);
            this.name = 'SecureQLApiError';
        }
    },
    normalizeBaseUrl: jest.fn((url: string) => url),
    mapSecureQLError: jest.fn(),
}));

import { SecureQLAdapter } from '../adapters/secureql';
import { getKeyInfo, getSchema, executeQuery, SecureQLApiError } from '../adapters/secureqlClient';
import { ConnectionProfile, ConnectionSecrets } from '../../core/types';

const mockedGetKeyInfo = getKeyInfo as jest.MockedFunction<typeof getKeyInfo>;
const mockedGetSchema = getSchema as jest.MockedFunction<typeof getSchema>;
const mockedExecuteQuery = executeQuery as jest.MockedFunction<typeof executeQuery>;

function makeProfile(overrides: Partial<ConnectionProfile> = {}): ConnectionProfile {
    return {
        id: 'conn-1',
        name: 'Test SecureQL',
        dialect: 'secureql',
        secureqlBaseUrl: 'https://secureql.example.com',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        ...overrides,
    };
}

const TEST_SECRETS: ConnectionSecrets = { apiKey: 'secret-key' };

const KEY_INFO_RESPONSE = {
    connection_id: 123,
    connection_name: 'Test Connection',
    dbms: 'postgres',
    database_name: 'appdb',
    allow_csv_export: true,
    user_id: 42,
};

describe('SecureQLAdapter', () => {
    beforeEach(() => {
        mockedGetKeyInfo.mockResolvedValue(KEY_INFO_RESPONSE);
        mockedGetSchema.mockResolvedValue({ tables: [] });
        mockedExecuteQuery.mockResolvedValue({
            results: [{
                affectedRows: 0,
                queriesRun: 1,
                fields: [{ field: 'id', colType: 'bigint' }],
                rows: [{ id: 1 }],
                runtime: 10,
            }],
            log: { runtime_ms: 10 },
        });
    });

    describe('testConnection', () => {
        it('resolves connection metadata and fetches schema', async () => {
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            await adapter.testConnection(profile, TEST_SECRETS);

            expect(mockedGetKeyInfo).toHaveBeenCalledWith('https://secureql.example.com', 'secret-key');
            expect(mockedGetSchema).toHaveBeenCalledTimes(1);
        });

        it('auto-resolves secureqlConnectionId from /v1/key/me', async () => {
            const adapter = new SecureQLAdapter();
            const profile = makeProfile({ secureqlConnectionId: undefined });
            await adapter.testConnection(profile, TEST_SECRETS);

            expect(profile.secureqlConnectionId).toBe('123');
            expect(profile.secureqlTargetDbms).toBe('postgres');
            expect(profile.sqlDialect).toBe('postgres');
        });

        it('does not persist an unsaved profile (no id)', async () => {
            const saveProfile = jest.fn().mockResolvedValue(undefined);
            const adapter = new SecureQLAdapter(saveProfile);
            const profile = makeProfile({ id: '' as any });
            await adapter.testConnection(profile, TEST_SECRETS);

            expect(saveProfile).not.toHaveBeenCalled();
        });

        it('persists an existing profile', async () => {
            const saveProfile = jest.fn().mockResolvedValue(undefined);
            const adapter = new SecureQLAdapter(saveProfile);
            const profile = makeProfile();
            await adapter.testConnection(profile, TEST_SECRETS);

            expect(saveProfile).toHaveBeenCalledWith(profile);
        });

        it('throws when base URL is missing', async () => {
            const adapter = new SecureQLAdapter();
            const profile = makeProfile({ secureqlBaseUrl: undefined });
            await expect(adapter.testConnection(profile, TEST_SECRETS)).rejects.toThrow('SecureQL Base URL is required.');
        });

        it('throws when API key is missing', async () => {
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            await expect(adapter.testConnection(profile, {})).rejects.toThrow('API key is required.');
        });
    });

    describe('runQuery', () => {
        it('maps a SELECT result', async () => {
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.runQuery(profile, TEST_SECRETS, 'SELECT 1', { maxRows: 100 });

            expect(result.columns).toEqual([{ name: 'id', type: 'bigint' }]);
            expect(result.rows).toEqual([{ id: 1 }]);
            expect(result.rowCount).toBe(1);
            expect(result.elapsedMs).toBe(10);
        });

        it('maps a DML result', async () => {
            mockedExecuteQuery.mockResolvedValue({
                results: [{ affectedRows: 5, queriesRun: 1 }],
                log: { runtime_ms: 15 },
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.runQuery(profile, TEST_SECRETS, 'UPDATE x SET a=1', { maxRows: 100 });

            expect(result.columns).toEqual([]);
            expect(result.rows).toEqual([]);
            expect(result.rowCount).toBe(5);
        });

        it('throws on query error in results', async () => {
            mockedExecuteQuery.mockResolvedValue({
                results: [{ affectedRows: 0, queriesRun: 1, error: 'relation "foo" does not exist' }],
                log: {},
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            await expect(adapter.runQuery(profile, TEST_SECRETS, 'SELECT * FROM foo', { maxRows: 100 }))
                .rejects.toThrow('relation "foo" does not exist');
        });

        it('adds warning for multiple tabular results', async () => {
            mockedExecuteQuery.mockResolvedValue({
                results: [
                    { affectedRows: 0, queriesRun: 1, fields: [{ field: 'a', colType: 'int' }], rows: [{ a: 1 }], runtime: 5 },
                    { affectedRows: 0, queriesRun: 1, fields: [{ field: 'b', colType: 'int' }], rows: [{ b: 2 }], runtime: 3 },
                ],
                log: {},
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.runQuery(profile, TEST_SECRETS, 'SELECT 1; SELECT 2', { maxRows: 100 });

            expect(result.columns[0].name).toBe('a');
            expect(result.warning).toContain('2 result sets');
        });
    });

    describe('executeNonQuery', () => {
        it('returns affected rows count', async () => {
            mockedExecuteQuery.mockResolvedValue({
                results: [{ affectedRows: 3, queriesRun: 1 }],
                log: {},
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.executeNonQuery(profile, TEST_SECRETS, 'DELETE FROM users WHERE id > 5');

            expect(result.affectedRows).toBe(3);
        });

        it('throws on error in results', async () => {
            mockedExecuteQuery.mockResolvedValue({
                results: [{ affectedRows: 0, queriesRun: 1, error: 'permission denied' }],
                log: {},
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            await expect(adapter.executeNonQuery(profile, TEST_SECRETS, 'DROP TABLE users'))
                .rejects.toThrow('permission denied');
        });
    });

    describe('introspectSchema', () => {
        it('maps a typical schema response', async () => {
            mockedGetSchema.mockResolvedValue({
                connection_id: 1,
                database_name: 'testdb',
                tables: [
                    {
                        schema_name: 'public',
                        table_name: 'users',
                        columns: [
                            { column_name: 'id', data_type: 'bigint', is_primary_key: true, is_nullable: false },
                            { column_name: 'email', data_type: 'varchar', is_primary_key: false, is_nullable: false },
                        ],
                    },
                ],
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.introspectSchema(profile, TEST_SECRETS);

            expect(result.version).toBe('0.2');
            expect(result.connectionId).toBe('conn-1');
            expect(result.dialect).toBe('secureql');
            expect(result.schemas).toHaveLength(1);
            expect(result.schemas[0].name).toBe('public');
            expect(result.schemas[0].tables).toHaveLength(1);
            expect(result.schemas[0].tables[0].name).toBe('users');
            expect(result.schemas[0].tables[0].columns).toHaveLength(2);
            expect(result.schemas[0].tables[0].primaryKey).toEqual(['id']);
        });

        it('handles empty schema', async () => {
            mockedGetSchema.mockResolvedValue({ tables: [] });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.introspectSchema(profile, TEST_SECRETS);

            expect(result.schemas).toEqual([]);
        });

        it('maps routines when present', async () => {
            mockedGetSchema.mockResolvedValue({
                tables: [],
                routines: [
                    {
                        schema_name: 'public',
                        routine_name: 'get_users',
                        routine_type: 'function',
                        return_type: 'TABLE',
                        parameters: [
                            { name: 'limit_rows', mode: 'in', type: 'integer', position: 1 },
                        ],
                    },
                ],
            });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile();
            const result = await adapter.introspectSchema(profile, TEST_SECRETS);

            expect(result.schemas[0].functions).toHaveLength(1);
            expect(result.schemas[0].functions![0].name).toBe('get_users');
            expect(result.schemas[0].functions![0].signature).toBe('get_users(IN limit_rows integer)');
        });
    });

    describe('allowCsvExport sync', () => {
        it('syncs allowCsvExport flag from server on each operation', async () => {
            mockedGetKeyInfo.mockResolvedValue({ ...KEY_INFO_RESPONSE, allow_csv_export: false });
            const adapter = new SecureQLAdapter();
            const profile = makeProfile({ allowCsvExport: true });
            await adapter.testConnection(profile, TEST_SECRETS);

            expect(profile.allowCsvExport).toBe(false);
        });
    });
});
