import { ConnectionProfile, ConnectionSecrets } from '../core/types';
import { getKeyInfo } from './adapters/secureqlClient';
import { Logger } from '../core/logger';

/**
 * At startup, iterate all SecureQL-dialect connections and refresh
 * server-controlled flags (e.g. allow_csv_export) from /v1/key/me.
 * Runs in the background so it doesn't block extension activation.
 */
export async function refreshAllSecureQLProfiles(
    loadProfiles: () => Promise<ConnectionProfile[]>,
    getSecrets: (id: string) => Promise<ConnectionSecrets>,
    saveProfile: (profile: ConnectionProfile) => Promise<void>,
): Promise<void> {
    const profiles = await loadProfiles();
    const secureqlProfiles = profiles.filter((p) => p.dialect === 'secureql');

    for (const profile of secureqlProfiles) {
        try {
            const secrets = await getSecrets(profile.id);
            const baseUrl = profile.secureqlBaseUrl;
            const apiKey = secrets.apiKey;

            if (!baseUrl || !apiKey) continue;

            const info = await getKeyInfo(baseUrl, apiKey);

            // Update server-controlled fields
            let changed = false;
            if (profile.allowCsvExport !== info.allow_csv_export) {
                profile.allowCsvExport = info.allow_csv_export;
                changed = true;
            }
            if (!profile.secureqlConnectionId) {
                profile.secureqlConnectionId = String(info.connection_id);
                changed = true;
            }
            if (!profile.secureqlTargetDbms) {
                profile.secureqlTargetDbms = info.dbms;
                changed = true;
            }
            if (!profile.sqlDialect) {
                profile.sqlDialect = info.dbms;
                changed = true;
            }

            if (changed) {
                await saveProfile(profile);
            }
        } catch {
            // Skip this connection if refresh fails (server unreachable, key revoked, etc.)
            Logger.debug(`SecureQL startup refresh skipped for connection ${profile.id}`);
            continue;
        }
    }
}
