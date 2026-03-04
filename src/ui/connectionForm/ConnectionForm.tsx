import React, { useEffect, useRef, useState } from 'react';
import {
    ConnectionProfile,
    ConnectionSecrets,
    DPConnectionFieldSchema,
    DPConnectionFieldStorage,
    DPConnectionFormAction,
    DPProviderActionStatus,
    DPProviderDescriptor
} from '../../core/types';

interface ConnectionFormProps {
    vscode: any;
}

type Tab = 'connection' | 'auth' | 'ssh';
type ProfileState = Partial<ConnectionProfile> & Record<string, unknown>;
type SecretsState = ConnectionSecrets & Record<string, unknown>;
type LocalState = Record<string, unknown>;

const DEFAULT_PROFILE: ProfileState = {
    name: 'My Database Connection',
    credentialStorageMode: 'session'
};

const DEFAULT_SECRETS: SecretsState = {};
const DEFAULT_LOCAL: LocalState = {};
const CONNECTION_TAG_OPTIONS = ['production', 'staging', 'dev', 'reporting'];

function getFieldStorage(field: DPConnectionFieldSchema): DPConnectionFieldStorage {
    return field.storage ?? 'profile';
}

function isEmptyValue(value: unknown): boolean {
    return value === undefined || value === null || value === '';
}

export const ConnectionForm: React.FC<ConnectionFormProps> = ({ vscode }) => {
    const [providers, setProviders] = useState<DPProviderDescriptor[]>([]);
    const [selectedProvider, setSelectedProvider] = useState<string>('');
    const [activeTab, setActiveTab] = useState<Tab>('connection');
    const [profile, setProfile] = useState<ProfileState>({ ...DEFAULT_PROFILE });
    const [secrets, setSecrets] = useState<SecretsState>({ ...DEFAULT_SECRETS });
    const [localValues, setLocalValues] = useState<LocalState>({ ...DEFAULT_LOCAL });
    const [statusMsg, setStatusMsg] = useState<DPProviderActionStatus | null>(null);

    const providersRef = useRef<DPProviderDescriptor[]>([]);
    const profileRef = useRef<ProfileState>(profile);
    const secretsRef = useRef<SecretsState>(secrets);
    const localRef = useRef<LocalState>(localValues);
    const selectedProviderRef = useRef<string>(selectedProvider);

    useEffect(() => { providersRef.current = providers; }, [providers]);
    useEffect(() => { profileRef.current = profile; }, [profile]);
    useEffect(() => { secretsRef.current = secrets; }, [secrets]);
    useEffect(() => { localRef.current = localValues; }, [localValues]);
    useEffect(() => { selectedProviderRef.current = selectedProvider; }, [selectedProvider]);

    const findProvider = (dialect: string): DPProviderDescriptor | undefined => {
        return providersRef.current.find((p) => p.dialect === dialect);
    };

    const applyProviderDefaults = (
        dialect: string,
        profileInput: ProfileState,
        secretsInput: SecretsState,
        localInput: LocalState
    ): { profile: ProfileState; secrets: SecretsState; localValues: LocalState } => {
        const provider = findProvider(dialect);
        if (!provider) return { profile: profileInput, secrets: secretsInput, localValues: localInput };

        const nextProfile: ProfileState = { ...profileInput };
        const nextSecrets: SecretsState = { ...secretsInput };
        const nextLocal: LocalState = { ...localInput };

        for (const field of provider.formSchema.fields) {
            if (field.defaultValue === undefined) continue;
            const storage = getFieldStorage(field);
            if (storage === 'profile') {
                if (isEmptyValue(nextProfile[field.key])) {
                    nextProfile[field.key] = field.defaultValue;
                }
            } else if (storage === 'secrets') {
                if (isEmptyValue(nextSecrets[field.key])) {
                    nextSecrets[field.key] = field.defaultValue;
                }
            } else {
                if (isEmptyValue(nextLocal[field.key])) {
                    nextLocal[field.key] = field.defaultValue;
                }
            }
        }

        return { profile: nextProfile, secrets: nextSecrets, localValues: nextLocal };
    };

    const resetForProviderChange = (nextDialect: string) => {
        const previousProvider = findProvider(selectedProviderRef.current);

        let nextProfile: ProfileState = { ...profileRef.current };
        let nextSecrets: SecretsState = { ...secretsRef.current };
        let nextLocalValues: LocalState = { ...localRef.current };

        if (previousProvider) {
            for (const field of previousProvider.formSchema.fields) {
                const storage = getFieldStorage(field);
                if (storage === 'profile') {
                    delete nextProfile[field.key];
                } else if (storage === 'secrets') {
                    delete nextSecrets[field.key];
                } else {
                    delete nextLocalValues[field.key];
                }
            }
        }

        const withDefaults = applyProviderDefaults(nextDialect, nextProfile, nextSecrets, nextLocalValues);
        setProfile(withDefaults.profile);
        setSecrets(withDefaults.secrets);
        setLocalValues(withDefaults.localValues);
    };

    const currentProvider = providers.find((p) => p.dialect === selectedProvider);
    const hasAuthFields = (currentProvider?.formSchema.fields ?? []).some((field) => (field.tab ?? 'connection') === 'auth');

    const getStoreByStorage = (storage: DPConnectionFieldStorage): ProfileState | SecretsState | LocalState => {
        if (storage === 'secrets') return secrets;
        if (storage === 'local') return localValues;
        return profile;
    };

    const evaluateVisibilityRule = (rule: DPConnectionFieldSchema['visibleWhen'], fallbackStorage: DPConnectionFieldStorage): boolean => {
        if (!rule) return true;

        const storage = rule.storage ?? fallbackStorage;
        const source = getStoreByStorage(storage);
        const value = source[rule.key];

        let pass = true;
        if (rule.truthy !== undefined) {
            pass = Boolean(value) === rule.truthy;
        } else if (rule.equals !== undefined) {
            pass = value === rule.equals;
        } else if (rule.notEquals !== undefined) {
            pass = value !== rule.notEquals;
        }

        if (pass && rule.and) {
            return rule.and.every((sub) => evaluateVisibilityRule(sub, storage));
        }

        return pass;
    };

    const isFieldVisible = (field: DPConnectionFieldSchema): boolean => {
        return evaluateVisibilityRule(field.visibleWhen, getFieldStorage(field));
    };

    const getFieldValue = (field: DPConnectionFieldSchema): unknown => {
        const storage = getFieldStorage(field);
        const source = getStoreByStorage(storage);
        return source[field.key];
    };

    const setFieldValue = (field: DPConnectionFieldSchema, value: unknown) => {
        const storage = getFieldStorage(field);
        if (storage === 'profile') {
            setProfile((prev) => ({ ...prev, [field.key]: value }));
            return;
        }
        if (storage === 'secrets') {
            setSecrets((prev) => ({ ...prev, [field.key]: value }));
            return;
        }
        setLocalValues((prev) => ({ ...prev, [field.key]: value }));
    };

    const validate = (): boolean => {
        if (isEmptyValue(profile.name)) {
            setStatusMsg({ type: 'error', text: 'Connection name is required.' });
            return false;
        }
        if (!selectedProvider) {
            setStatusMsg({ type: 'error', text: 'Connection type is required.' });
            return false;
        }
        if (!currentProvider) {
            setStatusMsg({ type: 'error', text: 'Selected provider is not available.' });
            return false;
        }

        for (const field of currentProvider.formSchema.fields) {
            if (!field.required || !isFieldVisible(field)) continue;
            const value = getFieldValue(field);
            if (isEmptyValue(value)) {
                setStatusMsg({ type: 'error', text: `${field.label} is required.` });
                return false;
            }
        }

        setStatusMsg(null);
        return true;
    };

    const handleSave = () => {
        if (!validate()) return;
        setStatusMsg({ type: 'info', text: 'Testing connection before saving...' });
        vscode.postMessage({
            command: 'save',
            profile: { ...profile, dialect: selectedProvider },
            secrets
        });
    };

    const handleTest = () => {
        if (!validate()) return;
        setStatusMsg({ type: 'info', text: 'Testing connection...' });
        vscode.postMessage({
            command: 'test',
            profile: { ...profile, dialect: selectedProvider },
            secrets
        });
    };

    const buildActionPayload = (action: DPConnectionFormAction): Record<string, unknown> => {
        const allValues: Record<string, unknown> = {
            ...profileRef.current,
            ...secretsRef.current,
            ...localRef.current,
            dialect: selectedProviderRef.current
        };

        if (!action.payloadKeys || action.payloadKeys.length === 0) {
            return allValues;
        }

        const payload: Record<string, unknown> = {};
        for (const key of action.payloadKeys) {
            payload[key] = allValues[key];
        }
        payload.dialect = selectedProviderRef.current;
        return payload;
    };

    const runProviderAction = (action: DPConnectionFormAction) => {
        const dialect = selectedProviderRef.current;
        if (!dialect) return;

        vscode.postMessage({
            command: 'runProviderAction',
            dialect,
            actionId: action.id,
            payload: buildActionPayload(action)
        });
    };

    const renderActionControl = (action: DPConnectionFormAction, keyPrefix: string = '') => {
        if (action.style === 'link') {
            return (
                <a
                    key={`${keyPrefix}${action.id}`}
                    href="#"
                    className="cf-action-link"
                    onClick={(e) => {
                        e.preventDefault();
                        runProviderAction(action);
                    }}
                >
                    {action.label}
                </a>
            );
        }

        const isPrimary = action.style === 'primary';
        return (
            <button
                key={`${keyPrefix}${action.id}`}
                className={isPrimary ? 'primary' : undefined}
                onClick={() => runProviderAction(action)}
            >
                {action.label}
            </button>
        );
    };

    const getFieldBoundActions = (tab: Tab, fieldKey: string): DPConnectionFormAction[] => {
        return (currentProvider?.formSchema.actions ?? []).filter((action) => {
            const actionTab = action.tab ?? 'connection';
            return actionTab === tab && action.payloadKeys?.length === 1 && action.payloadKeys[0] === fieldKey;
        });
    };

    const renderField = (field: DPConnectionFieldSchema) => {
        if (!isFieldVisible(field)) {
            return null;
        }

        const value = getFieldValue(field);
        const fieldClass = `cf-field${field.width === 'half' ? ' half' : ''}`;

        let control: React.ReactNode;
        switch (field.type) {
            case 'textarea':
                control = (
                    <textarea
                        rows={4}
                        value={String(value ?? '')}
                        onChange={(e) => setFieldValue(field, e.target.value)}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case 'number':
                control = (
                    <input
                        type="number"
                        value={typeof value === 'number' ? value : ''}
                        min={field.min}
                        max={field.max}
                        step={field.step}
                        onChange={(e) => {
                            const raw = e.target.value;
                            if (raw === '') {
                                setFieldValue(field, undefined);
                                return;
                            }
                            const parsed = Number(raw);
                            setFieldValue(field, Number.isNaN(parsed) ? undefined : parsed);
                        }}
                        placeholder={field.placeholder}
                    />
                );
                break;
            case 'checkbox':
                control = (
                    <label className="cf-checkbox-label">
                        <input
                            type="checkbox"
                            checked={Boolean(value)}
                            onChange={(e) => setFieldValue(field, e.target.checked)}
                        />
                        <span>{field.label}</span>
                    </label>
                );
                break;
            case 'select':
                control = (
                    <select
                        value={String(value ?? '')}
                        onChange={(e) => setFieldValue(field, e.target.value)}
                    >
                        <option value="">Select...</option>
                        {(field.options ?? []).map((option) => (
                            <option key={option.value} value={option.value}>{option.label}</option>
                        ))}
                    </select>
                );
                break;
            case 'radio':
                control = (
                    <div className="cf-radio-group">
                        {(field.options ?? []).map((option) => (
                            <label key={option.value} className="cf-radio-label">
                                <input
                                    type="radio"
                                    checked={String(value ?? '') === option.value}
                                    onChange={() => setFieldValue(field, option.value)}
                                />
                                <span>{option.label}</span>
                            </label>
                        ))}
                    </div>
                );
                break;
            case 'file':
                control = (
                    <div className="cf-file-row">
                        <input
                            type="text"
                            value={String(value ?? '')}
                            onChange={(e) => setFieldValue(field, e.target.value)}
                            placeholder={field.placeholder}
                        />
                        {field.picker && (
                            <button
                                onClick={() => vscode.postMessage({
                                    command: 'pickFieldValue',
                                    field: {
                                        key: field.key,
                                        storage: getFieldStorage(field),
                                        picker: field.picker
                                    }
                                })}
                            >
                                Browse...
                            </button>
                        )}
                    </div>
                );
                break;
            default:
                control = (
                    <input
                        type={field.type === 'password' ? 'password' : 'text'}
                        value={String(value ?? '')}
                        onChange={(e) => setFieldValue(field, e.target.value)}
                        placeholder={field.placeholder}
                    />
                );
                break;
        }

        return (
            <div key={`${field.tab ?? 'connection'}:${field.key}`} className={fieldClass}>
                {field.type !== 'checkbox' && (
                    <label><span>{field.label}</span></label>
                )}
                {control}
                {field.description && (
                    <div className="cf-field-description">{field.description}</div>
                )}
            </div>
        );
    };

    const renderActions = (tab: Tab) => {
        const actions = (currentProvider?.formSchema.actions ?? []).filter((action) => (action.tab ?? 'connection') === tab);
        if (actions.length === 0) {
            return null;
        }

        return (
            <div className="cf-actions-row">
                {actions.map((action) => renderActionControl(action))}
            </div>
        );
    };

    useEffect(() => {
        vscode.postMessage({ command: 'ready' });

        const handleMessage = (event: any) => {
            const message = event.data;
            switch (message.command) {
                case 'setProviders': {
                    const nextProviders = message.providers ?? [];
                    providersRef.current = nextProviders;
                    setProviders(nextProviders);
                    if (!selectedProviderRef.current && nextProviders.length > 0) {
                        const nextDialect = nextProviders[0].dialect;
                        selectedProviderRef.current = nextDialect;
                        setSelectedProvider(nextDialect);
                        const defaults = applyProviderDefaults(nextDialect, profileRef.current, secretsRef.current, localRef.current);
                        setProfile(defaults.profile);
                        setSecrets(defaults.secrets);
                        setLocalValues(defaults.localValues);
                    }
                    break;
                }
                case 'setProfile': {
                    const incomingProfile: ProfileState = message.profile ?? {};
                    const incomingSecrets: SecretsState = message.secrets ?? {};
                    const dialect = String(incomingProfile.dialect ?? selectedProviderRef.current ?? '');
                    if (dialect) {
                        selectedProviderRef.current = dialect;
                        setSelectedProvider(dialect);
                    }

                    const mergedProfile = { ...DEFAULT_PROFILE, ...incomingProfile };
                    const mergedSecrets = { ...DEFAULT_SECRETS, ...incomingSecrets };
                    const withDefaults = dialect
                        ? applyProviderDefaults(dialect, mergedProfile, mergedSecrets, { ...DEFAULT_LOCAL })
                        : { profile: mergedProfile, secrets: mergedSecrets, localValues: { ...DEFAULT_LOCAL } };
                    setProfile(withDefaults.profile);
                    setSecrets(withDefaults.secrets);
                    setLocalValues(withDefaults.localValues);
                    break;
                }
                case 'testResult':
                    if (message.success) {
                        setStatusMsg({ type: 'success', text: message.message });
                    } else {
                        setStatusMsg({ type: 'error', text: message.message });
                    }
                    break;
                case 'saveResult':
                    if (message.success) {
                        setStatusMsg({ type: 'success', text: message.message ?? 'Connection saved.' });
                    } else {
                        setStatusMsg({ type: 'error', text: message.message ?? 'Connection test failed. Fix the settings and try again.' });
                    }
                    break;
                case 'fieldValueSelected': {
                    const key = String(message.key ?? '');
                    const storage: DPConnectionFieldStorage =
                        message.storage === 'secrets' ? 'secrets' : message.storage === 'local' ? 'local' : 'profile';
                    if (!key) break;
                    if (storage === 'profile') {
                        setProfile((prev) => ({ ...prev, [key]: message.value }));
                    } else if (storage === 'secrets') {
                        setSecrets((prev) => ({ ...prev, [key]: message.value }));
                    } else {
                        setLocalValues((prev) => ({ ...prev, [key]: message.value }));
                    }
                    break;
                }
                case 'providerActionResult': {
                    const result = message.result as {
                        profilePatch?: Record<string, unknown>;
                        secretsPatch?: Record<string, unknown>;
                        localPatch?: Record<string, unknown>;
                        status?: DPProviderActionStatus;
                    } | null;

                    if (!result) break;
                    if (result.profilePatch) {
                        setProfile((prev) => ({ ...prev, ...result.profilePatch }));
                    }
                    if (result.secretsPatch) {
                        setSecrets((prev) => ({ ...prev, ...result.secretsPatch }));
                    }
                    if (result.localPatch) {
                        setLocalValues((prev) => ({ ...prev, ...result.localPatch }));
                    }
                    if (result.status) {
                        setStatusMsg(result.status);
                    }
                    break;
                }
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, []);

    const providerConnectionFields = (currentProvider?.formSchema.fields ?? []).filter(
        (field) => (field.tab ?? 'connection') === 'connection'
    );
    const providerAuthFields = (currentProvider?.formSchema.fields ?? []).filter(
        (field) => (field.tab ?? 'connection') === 'auth'
    );
    const providerSshFields = (currentProvider?.formSchema.fields ?? []).filter(
        (field) => field.tab === 'ssh'
    );
    const hasSshFields = providerSshFields.length > 0;

    return (
        <div className="connection-form-root">
            <header className="cf-card">
                <h2>Add Connection</h2>

                <div className="cf-top-fields">
                    <div className="cf-field">
                        <label><span>Connection Type</span></label>
                        <select
                            value={selectedProvider}
                            onChange={(e) => {
                                const nextDialect = e.target.value;
                                setSelectedProvider(nextDialect);
                                selectedProviderRef.current = nextDialect;
                                resetForProviderChange(nextDialect);
                            }}
                        >
                            {providers.map((provider) => (
                                <option key={provider.dialect} value={provider.dialect}>{provider.displayName}</option>
                            ))}
                        </select>
                    </div>

                    <div className="cf-field">
                        <label><span>Connection Tag</span></label>
                        <select
                            value={String(profile.connectionTag ?? '')}
                            onChange={(e) => setProfile((prev) => ({ ...prev, connectionTag: e.target.value || undefined }))}
                        >
                            <option value="">None</option>
                            {CONNECTION_TAG_OPTIONS.map((option) => (
                                <option key={option} value={option}>{option}</option>
                            ))}
                        </select>
                    </div>

                    <div className="cf-field cf-field-full">
                        <label><span>Connection Name</span></label>
                        <input
                            type="text"
                            value={String(profile.name ?? '')}
                            onChange={(e) => setProfile((prev) => ({ ...prev, name: e.target.value }))}
                        />
                    </div>
                </div>
            </header>

            <section className="cf-card">
                <div className="cf-tabs">
                    <div
                        className={`cf-tab${activeTab === 'connection' ? ' active' : ''}`}
                        onClick={() => setActiveTab('connection')}
                    >
                        Connection
                    </div>
                    <div
                        className={`cf-tab${activeTab === 'auth' ? ' active' : ''}`}
                        onClick={() => setActiveTab('auth')}
                    >
                        Auth
                    </div>
                    {hasSshFields && (
                        <div
                            className={`cf-tab${activeTab === 'ssh' ? ' active' : ''}`}
                            onClick={() => setActiveTab('ssh')}
                        >
                            SSH Tunnel
                        </div>
                    )}
                </div>

                {activeTab === 'connection' && (
                    <div className="cf-tab-content">
                        {renderActions('connection')}
                        <div className="cf-fields-grid">
                            {providerConnectionFields.map(renderField)}
                        </div>
                    </div>
                )}

                {activeTab === 'auth' && (
                    <div className="cf-tab-content">
                        {renderActions('auth')}
                        {hasAuthFields && (
                            <div className="cf-fields-grid">
                                {providerAuthFields.map(renderField)}
                            </div>
                        )}

                        <div className="cf-credential-section">
                            <div className="cf-section-label">Credential Storage</div>
                            <div className="cf-radio-group">
                                <label className="cf-credential-option">
                                    <input
                                        type="radio"
                                        name="storage"
                                        value="session"
                                        checked={profile.credentialStorageMode === 'session'}
                                        onChange={() => setProfile((prev) => ({ ...prev, credentialStorageMode: 'session' }))}
                                    />
                                    <div>
                                        <strong>Session (Memory)</strong>
                                        <div className="cf-credential-hint">Credentials forgotten when you close the window.</div>
                                    </div>
                                </label>
                                <label className="cf-credential-option">
                                    <input
                                        type="radio"
                                        name="storage"
                                        value="secretStorage"
                                        checked={profile.credentialStorageMode === 'secretStorage'}
                                        onChange={() => setProfile((prev) => ({ ...prev, credentialStorageMode: 'secretStorage' }))}
                                    />
                                    <div>
                                        <strong>Secure Storage (Keychain)</strong>
                                        <div className="cf-credential-hint">Credentials encrypted and stored on disk.</div>
                                    </div>
                                </label>
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === 'ssh' && hasSshFields && (
                    <div className="cf-tab-content">
                        <div className="cf-fields-grid">
                            {providerSshFields.map(renderField)}
                        </div>
                    </div>
                )}
            </section>

            {statusMsg && (
                <div className={`cf-status ${statusMsg.type}`}>{statusMsg.text}</div>
            )}

            <div className="cf-footer">
                <button onClick={handleTest}>Test Connection</button>
                <button className="primary" onClick={handleSave}>Save Connection</button>
            </div>
        </div>
    );
};
