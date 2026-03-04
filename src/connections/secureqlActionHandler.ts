import { DPProviderActionHandler, DPProviderActionResult } from '../core/types';
import { getKeyInfo, SecureQLApiError } from './adapters/secureqlClient';

/**
 * Built-in action handler for the SecureQL "Validate API Key" button.
 * Calls GET /v1/key/me to resolve connection metadata from the API key.
 */
export const secureqlActionHandler: DPProviderActionHandler = async (
    actionId: string,
    payload: Record<string, unknown>,
): Promise<DPProviderActionResult | void> => {
    if (actionId !== 'validate-api-key') return;

    const baseUrl = payload.secureqlBaseUrl as string | undefined;
    const apiKey = payload.apiKey as string | undefined;

    if (!baseUrl || !apiKey) {
        return {
            status: { type: 'error' as const, text: 'Please enter both a SecureQL Base URL and API Key first.' },
        };
    }

    try {
        const info = await getKeyInfo(baseUrl, apiKey);
        return {
            profilePatch: {
                secureqlConnectionId: String(info.connection_id),
                secureqlTargetDbms: info.dbms,
                sqlDialect: info.dbms,
                allowCsvExport: info.allow_csv_export,
            },
            status: {
                type: 'success' as const,
                text: `Validated! Connection: "${info.connection_name}" (${info.dbms}, database: ${info.database_name})`,
            },
        };
    } catch (err: any) {
        const message = err instanceof SecureQLApiError
            ? err.userMessage
            : `Validation failed: ${err.message}`;
        return {
            status: { type: 'error' as const, text: message },
        };
    }
};
