import {
    ConnectionProfile,
    ConnectionSecrets,
    DPConnectionFieldStorage,
    DPProviderDescriptor,
} from '../core/types';

/**
 * Fields that must never be copied from a source profile.
 * These are identity, destination-specific, or derived fields.
 */
const REUSE_DENYLIST = new Set<string>([
    'id',
    'name',
    'createdAt',
    'updatedAt',
    'database',
    'schema',
    'filePath',
    'secureqlConnectionId',
    'secureqlTargetDbms',
    'sqlDialect',
    'allowCsvExport',
    'allowDataEdit',
]);

export interface ReusableKeys {
    profileKeys: string[];
    secretKeys: string[];
}

/**
 * Summary of a connection profile for the reuse selector UI.
 */
export interface ReuseSourceSummary {
    id: string;
    name: string;
    dialect: string;
    database?: string;
    host?: string;
    account?: string;
    secureqlBaseUrl?: string;
}

/**
 * Infer which profile and secret keys are reusable for a given provider,
 * based on its form schema and the core denylist.
 */
export function inferReusableKeys(provider: DPProviderDescriptor): ReusableKeys {
    const reuse = provider.formSchema.reuse;

    const profileKeys: string[] = [];
    const secretKeys: string[] = [];

    for (const field of provider.formSchema.fields) {
        const storage: DPConnectionFieldStorage = field.storage ?? 'profile';
        if (storage === 'local') continue;

        if (REUSE_DENYLIST.has(field.key)) continue;

        if (storage === 'profile') {
            profileKeys.push(field.key);
        } else if (storage === 'secrets') {
            secretKeys.push(field.key);
        }
    }

    if (!reuse) {
        return { profileKeys, secretKeys };
    }

    // Apply provider-level include/exclude overrides
    let finalProfile = applyOverrides(
        profileKeys,
        reuse.includeProfileKeys,
        reuse.excludeProfileKeys
    );
    let finalSecrets = applyOverrides(
        secretKeys,
        reuse.includeSecretKeys,
        reuse.excludeSecretKeys
    );

    return { profileKeys: finalProfile, secretKeys: finalSecrets };
}

function applyOverrides(
    inferred: string[],
    include?: string[],
    exclude?: string[]
): string[] {
    let keys = [...inferred];

    if (include && include.length > 0) {
        // Include overrides: start from inferred, add any include keys not already present
        for (const k of include) {
            if (!keys.includes(k)) {
                keys.push(k);
            }
        }
    }

    if (exclude && exclude.length > 0) {
        const excludeSet = new Set(exclude);
        keys = keys.filter((k) => !excludeSet.has(k));
    }

    return keys;
}

/**
 * Return the set of dialects that are compatible reuse sources for a provider.
 */
function getSourceDialects(provider: DPProviderDescriptor): Set<string> {
    const reuse = provider.formSchema.reuse;
    if (reuse?.sourceDialects && reuse.sourceDialects.length > 0) {
        return new Set(reuse.sourceDialects);
    }
    return new Set([provider.dialect]);
}

/**
 * Filter existing profiles to those compatible with the given provider.
 */
export function getCompatibleSources(
    profiles: ConnectionProfile[],
    provider: DPProviderDescriptor
): ReuseSourceSummary[] {
    const dialects = getSourceDialects(provider);

    return profiles
        .filter((p) => dialects.has(p.dialect))
        .map((p) => ({
            id: p.id,
            name: p.name,
            dialect: p.dialect,
            database: p.database,
            host: p.host,
            account: p.account,
            secureqlBaseUrl: p.secureqlBaseUrl,
        }));
}

/**
 * Build a reuse draft (profile patch + secrets patch) from a source profile.
 * Only copies keys that are reusable per the provider schema.
 * When secretsAvailable is false, only profile keys are copied.
 */
export function buildReuseDraft(
    source: ConnectionProfile,
    sourceSecrets: ConnectionSecrets,
    provider: DPProviderDescriptor,
    secretsAvailable: boolean
): { profilePatch: Record<string, unknown>; secretsPatch: Record<string, unknown> } {
    const { profileKeys, secretKeys } = inferReusableKeys(provider);

    const profilePatch: Record<string, unknown> = {};
    const secretsPatch: Record<string, unknown> = {};

    const sourceRecord = source as unknown as Record<string, unknown>;
    for (const key of profileKeys) {
        if (sourceRecord[key] !== undefined) {
            profilePatch[key] = sourceRecord[key];
        }
    }

    if (secretsAvailable) {
        const secretsRecord = sourceSecrets as unknown as Record<string, unknown>;
        for (const key of secretKeys) {
            if (secretsRecord[key] !== undefined) {
                secretsPatch[key] = secretsRecord[key];
            }
        }
    }

    return { profilePatch, secretsPatch };
}

/**
 * Check if reuse is enabled for a provider.
 */
export function isReuseEnabled(provider: DPProviderDescriptor): boolean {
    return provider.formSchema.reuse?.disabled !== true;
}

/**
 * Format a source summary for display in the selector.
 * e.g. "Prod PG / app_main (postgres • db1 • db.example.com)"
 */
export function formatSourceLabel(source: ReuseSourceSummary): string {
    const parts: string[] = [source.dialect];
    if (source.database) parts.push(source.database);
    if (source.host) parts.push(source.host);
    if (source.account) parts.push(source.account);
    if (source.secureqlBaseUrl) parts.push(source.secureqlBaseUrl);

    const detail = parts.join(' \u2022 ');
    return `${source.name} (${detail})`;
}
