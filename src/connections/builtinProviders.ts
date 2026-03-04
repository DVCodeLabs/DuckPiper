import { DPConnectionFieldSchema, DPProviderDescriptor } from '../core/types';

const SSH_TUNNEL_FIELDS: DPConnectionFieldSchema[] = [
    { key: 'sshEnabled', label: 'Enable SSH Tunnel', type: 'checkbox', tab: 'ssh', storage: 'profile', defaultValue: false, width: 'full' },
    { key: 'sshHost', label: 'SSH Host', type: 'text', tab: 'ssh', storage: 'profile', required: true, placeholder: 'bastion.example.com', width: 'half', visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true } },
    { key: 'sshPort', label: 'SSH Port', type: 'number', tab: 'ssh', storage: 'profile', required: true, defaultValue: 22, min: 1, max: 65535, width: 'half', visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true } },
    { key: 'sshUsername', label: 'SSH Username', type: 'text', tab: 'ssh', storage: 'profile', required: true, width: 'full', visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true } },
    {
        key: 'sshAuthMethod', label: 'SSH Auth Method', type: 'select', tab: 'ssh', storage: 'profile', required: true, defaultValue: 'password', width: 'full',
        options: [
            { value: 'password', label: 'Password' },
            { value: 'privateKey', label: 'Private Key' }
        ],
        visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true }
    },
    { key: 'sshPassword', label: 'SSH Password', type: 'password', tab: 'ssh', storage: 'secrets', required: true, width: 'full', visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true, and: [{ storage: 'profile', key: 'sshAuthMethod', equals: 'password' }] } },
    {
        key: 'sshPrivateKeyPath', label: 'SSH Private Key File', type: 'file', tab: 'ssh', storage: 'profile', width: 'full',
        placeholder: '~/.ssh/id_rsa',
        description: 'Path to your SSH private key file. Alternatively, paste the key contents below.',
        picker: { mode: 'open', title: 'Select SSH Private Key', openLabel: 'Select Key', canSelectFiles: true, canSelectFolders: false },
        visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true, and: [{ storage: 'profile', key: 'sshAuthMethod', equals: 'privateKey' }] }
    },
    { key: 'sshPrivateKey', label: 'SSH Private Key (paste)', type: 'textarea', tab: 'ssh', storage: 'secrets', width: 'full', placeholder: '-----BEGIN OPENSSH PRIVATE KEY-----', description: 'Paste your private key here if not using a file path above.', visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true, and: [{ storage: 'profile', key: 'sshAuthMethod', equals: 'privateKey' }] } },
    { key: 'sshPrivateKeyPassphrase', label: 'SSH Key Passphrase', type: 'password', tab: 'ssh', storage: 'secrets', width: 'full', placeholder: 'Leave empty if key is not encrypted', visibleWhen: { storage: 'profile', key: 'sshEnabled', truthy: true, and: [{ storage: 'profile', key: 'sshAuthMethod', equals: 'privateKey' }] } },
];

export const BUILTIN_PROVIDERS: DPProviderDescriptor[] = [
    {
        providerId: 'secureql',
        displayName: 'SecureQL',
        dialect: 'secureql',
        formSchema: {
            fields: [
                {
                    key: 'secureqlBaseUrl',
                    label: 'SecureQL Base URL',
                    type: 'text',
                    tab: 'connection',
                    storage: 'profile',
                    required: true,
                    placeholder: 'https://api.secureql.company.com',
                    description: 'The base URL of your SecureQL server (e.g. localhost:3000 for dev).',
                    width: 'full',
                },
                {
                    key: 'credentialStorageMode',
                    label: 'Credential Storage',
                    type: 'select',
                    tab: 'connection',
                    storage: 'profile',
                    defaultValue: 'secretStorage',
                    options: [
                        { value: 'secretStorage', label: 'VS Code Secret Storage (Recommended)' },
                        { value: 'session', label: 'Session Only' },
                    ],
                    width: 'full',
                },
                {
                    key: 'apiKey',
                    label: 'API Key',
                    type: 'password',
                    tab: 'auth',
                    storage: 'secrets',
                    required: true,
                    placeholder: 'Paste your SecureQL API key',
                    description: 'Your SecureQL API key. Click "Validate API Key" after entering to auto-detect your connection.',
                    width: 'full',
                },
            ],
            actions: [
                {
                    id: 'validate-api-key',
                    label: 'Validate API Key',
                    tab: 'auth',
                    style: 'primary',
                    payloadKeys: ['secureqlBaseUrl', 'apiKey'],
                },
            ],
        },
        supports: {
            ssl: false,
            oauth: false,
            keypair: false,
            introspection: true,
            cancellation: false,
        },
    },
    {
        providerId: 'duckdb',
        displayName: 'DuckDB',
        dialect: 'duckdb',
        formSchema: {
            fields: [
                {
                    key: 'filePath',
                    label: 'Database Path or Connection String',
                    type: 'file',
                    tab: 'connection',
                    storage: 'profile',
                    required: true,
                    placeholder: '/path/to/my.duckdb or md:my_database',
                    description: 'Use a local file path or a MotherDuck-style connection string such as md:.',
                    picker: {
                        mode: 'open',
                        title: 'Select DuckDB Database',
                        openLabel: 'Select Database',
                        canSelectFiles: true,
                        canSelectFolders: false,
                        filters: {
                            'DuckDB Files': ['db', 'duckdb', 'ddb'],
                            'All Files': ['*']
                        }
                    }
                }
            ]
        },
        supports: { ssl: false, oauth: false, keypair: false, introspection: true, cancellation: true }
    },
    {
        providerId: 'postgres',
        displayName: 'PostgreSQL',
        dialect: 'postgres',
        formSchema: {
            fields: [
                { key: 'host', label: 'Host', type: 'text', tab: 'connection', storage: 'profile', required: true, defaultValue: 'localhost', width: 'half' },
                { key: 'port', label: 'Port', type: 'number', tab: 'connection', storage: 'profile', required: true, defaultValue: 5432, width: 'half' },
                { key: 'database', label: 'Database', type: 'text', tab: 'connection', storage: 'profile', required: true, width: 'full' },
                { key: 'ssl', label: 'Use SSL', type: 'checkbox', tab: 'connection', storage: 'profile', defaultValue: false, width: 'full' },
                {
                    key: 'sslMode',
                    label: 'SSL Mode',
                    type: 'select',
                    tab: 'connection',
                    storage: 'profile',
                    defaultValue: 'disable',
                    options: [
                        { value: 'disable', label: 'Disable' },
                        { value: 'require', label: 'Require' },
                        { value: 'verify-ca', label: 'Verify CA' },
                        { value: 'verify-full', label: 'Verify Full' }
                    ],
                    visibleWhen: { storage: 'profile', key: 'ssl', truthy: true }
                },
                { key: 'username', label: 'Username', type: 'text', tab: 'auth', storage: 'profile', required: true, width: 'full' },
                { key: 'password', label: 'Password', type: 'password', tab: 'auth', storage: 'secrets', required: true, width: 'full' },
                ...SSH_TUNNEL_FIELDS
            ]
        },
        supports: { ssl: true, oauth: false, keypair: false, introspection: true, cancellation: true }
    },
    {
        providerId: 'mysql',
        displayName: 'MySQL',
        dialect: 'mysql',
        formSchema: {
            fields: [
                { key: 'host', label: 'Host', type: 'text', tab: 'connection', storage: 'profile', required: true, defaultValue: 'localhost', width: 'half' },
                { key: 'port', label: 'Port', type: 'number', tab: 'connection', storage: 'profile', required: true, defaultValue: 3306, width: 'half' },
                { key: 'database', label: 'Database', type: 'text', tab: 'connection', storage: 'profile', required: true, width: 'full' },
                { key: 'ssl', label: 'Use SSL', type: 'checkbox', tab: 'connection', storage: 'profile', defaultValue: false, width: 'full' },
                {
                    key: 'sslMode',
                    label: 'SSL Mode',
                    type: 'select',
                    tab: 'connection',
                    storage: 'profile',
                    defaultValue: 'disable',
                    options: [
                        { value: 'disable', label: 'Disable' },
                        { value: 'require', label: 'Require' },
                        { value: 'verify-ca', label: 'Verify CA' },
                        { value: 'verify-full', label: 'Verify Full' }
                    ],
                    visibleWhen: { storage: 'profile', key: 'ssl', truthy: true }
                },
                { key: 'username', label: 'Username', type: 'text', tab: 'auth', storage: 'profile', required: true, width: 'full' },
                { key: 'password', label: 'Password', type: 'password', tab: 'auth', storage: 'secrets', required: true, width: 'full' },
                ...SSH_TUNNEL_FIELDS
            ]
        },
        supports: { ssl: true, oauth: false, keypair: false, introspection: true, cancellation: true }
    }
];
