import * as React from 'react';
import { useState, useEffect, useRef, useCallback } from 'react';

/* ── Types (mirrored from extension side) ─────────── */

interface BackupSchemaContext {
    connectionId: string;
    connectionName: string;
    schemaName: string;
    dialect: string;
    hasViews: boolean;
    hasRoutines: boolean;
    isLocalDuckDB?: boolean;
    defaultFilePath: string;
}

interface BackupOptions {
    addDropTable: boolean;
    addCreateTable: boolean;
    addInsertData: boolean;
    addCreateView: boolean;
    addCreateRoutine: boolean;
    wrapInTransaction: boolean;
    disableForeignKeyChecks: boolean;
}

const DEFAULT_OPTIONS: BackupOptions = {
    addDropTable: false,
    addCreateTable: true,
    addInsertData: true,
    addCreateView: false,
    addCreateRoutine: false,
    wrapInTransaction: false,
    disableForeignKeyChecks: false,
};

/* ── Option definitions ───────────────────────────── */

interface OptionDef {
    key: keyof BackupOptions;
    mysqlLabel: string;
    defaultLabel: string;
    defaultOn: boolean;
    section: 'structure' | 'data' | 'other';
    showWhen?: (ctx: BackupSchemaContext) => boolean;
}

const OPTION_DEFS: OptionDef[] = [
    {
        key: 'addDropTable',
        mysqlLabel: 'Add DROP TABLE / VIEW / PROCEDURE / FUNCTION / EVENT / TRIGGER statement',
        defaultLabel: 'Add DROP TABLE IF EXISTS statement',
        defaultOn: false,
        section: 'structure',
    },
    {
        key: 'addCreateTable',
        mysqlLabel: 'Add CREATE TABLE statement',
        defaultLabel: 'Add CREATE TABLE statement',
        defaultOn: true,
        section: 'structure',
    },
    {
        key: 'addCreateView',
        mysqlLabel: 'Add CREATE VIEW statement',
        defaultLabel: 'Add CREATE VIEW statement',
        defaultOn: false,
        section: 'structure',
        showWhen: (ctx) => ctx.hasViews,
    },
    {
        key: 'addCreateRoutine',
        mysqlLabel: 'Add CREATE PROCEDURE / FUNCTION / EVENT statement',
        defaultLabel: 'Add CREATE PROCEDURE / FUNCTION statement',
        defaultOn: false,
        section: 'structure',
        showWhen: (ctx) => ctx.hasRoutines,
    },
    {
        key: 'addInsertData',
        mysqlLabel: 'Add table data (INSERT statements)',
        defaultLabel: 'Add table data (INSERT statements)',
        defaultOn: true,
        section: 'data',
    },
    {
        key: 'wrapInTransaction',
        mysqlLabel: 'Enclose export in a transaction',
        defaultLabel: 'Wrap in transaction',
        defaultOn: false,
        section: 'other',
    },
    {
        key: 'disableForeignKeyChecks',
        mysqlLabel: 'Disable foreign key checks',
        defaultLabel: 'Disable foreign key checks',
        defaultOn: false,
        section: 'other',
        showWhen: (ctx) => ['mysql', 'postgres', 'duckdb', 'sqlite'].includes(ctx.dialect),
    },
];

/* ── Toggle Row ───────────────────────────────────── */

function ToggleRow({ label, checked, onChange, disabled }: {
    label: string;
    checked: boolean;
    onChange: (checked: boolean) => void;
    disabled?: boolean;
}) {
    const handleClick = () => {
        if (!disabled) onChange(!checked);
    };

    return (
        <div
            className={`toggle-row${disabled ? ' disabled' : ''}`}
            onClick={handleClick}
            role="switch"
            aria-checked={checked}
            aria-label={label}
        >
            <div className="toggle-label">
                <span className="toggle-label-text">{label}</span>
            </div>
            <div className={`toggle-switch${checked ? ' on' : ''}`}>
                <div className="toggle-knob" />
            </div>
        </div>
    );
}

/* ── Main Form ────────────────────────────────────── */

interface Props {
    vscode: { postMessage: (msg: any) => void };
}

export function BackupSchemaForm({ vscode }: Props) {
    const [context, setContext] = useState<BackupSchemaContext | null>(null);
    const [filePath, setFilePath] = useState('');
    const [options, setOptions] = useState<BackupOptions>({ ...DEFAULT_OPTIONS });
    const [status, setStatus] = useState<{ type: 'info' | 'error' | 'success'; text: string } | null>(null);
    const [executing, setExecuting] = useState(false);

    const contextRef = useRef(context);
    useEffect(() => { contextRef.current = context; }, [context]);

    // Message handler
    useEffect(() => {
        vscode.postMessage({ command: 'ready' });

        const handleMessage = (event: MessageEvent) => {
            const msg = event.data;
            if (!msg) return;

            switch (msg.command) {
                case 'setContext':
                    setContext(msg.data);
                    setFilePath(msg.data.defaultFilePath || '');
                    break;
                case 'filePath':
                    if (msg.path) setFilePath(msg.path);
                    break;
                case 'backupResult':
                    setExecuting(false);
                    if (msg.data?.ok) {
                        setStatus({ type: 'success', text: msg.data.message || 'Backup complete.' });
                    } else {
                        setStatus({ type: 'error', text: msg.data?.message || 'Backup failed.' });
                    }
                    break;
            }
        };

        window.addEventListener('message', handleMessage);
        return () => window.removeEventListener('message', handleMessage);
    }, []);

    const handleBrowse = useCallback(() => {
        vscode.postMessage({ command: 'browse', currentPath: filePath });
    }, [filePath]);

    const handleOptionChange = useCallback((key: keyof BackupOptions, value: boolean) => {
        setOptions(prev => ({ ...prev, [key]: value }));
    }, []);

    const handleBackup = useCallback(() => {
        if (!filePath.trim() || executing) return;
        setExecuting(true);
        setStatus({ type: 'info', text: 'Running backup...' });
        vscode.postMessage({ command: 'execute', data: { filePath: filePath.trim(), options } });
    }, [filePath, options, executing]);

    const handleCancel = useCallback(() => {
        vscode.postMessage({ command: 'cancel' });
    }, []);

    if (!context) {
        return <div className="backup-schema-root"><p>Loading...</p></div>;
    }

    const isMySQL = context.dialect === 'mysql';
    const getLabel = (def: OptionDef) => isMySQL ? def.mysqlLabel : def.defaultLabel;

    const visibleOptions = OPTION_DEFS.filter(
        def => !def.showWhen || def.showWhen(context)
    );

    const structureOpts = visibleOptions.filter(d => d.section === 'structure');
    const dataOpts = visibleOptions.filter(d => d.section === 'data');
    const otherOpts = visibleOptions.filter(d => d.section === 'other');

    return (
        <div className="backup-schema-root">
            {/* Header */}
            <div className="backup-card">
                <div className="backup-header">
                    <h1>Backup Schema</h1>
                    <span className="backup-badge">{context.connectionName}</span>
                    <span className="backup-badge">{context.schemaName}</span>
                    <span className="backup-badge dialect">{context.dialect.toUpperCase()}</span>
                </div>
            </div>

            {/* Output File */}
            <div className="backup-card backup-file-section">
                <label>Output File</label>
                <div className="file-path-row">
                    <input
                        type="text"
                        value={filePath}
                        onChange={(e) => setFilePath(e.target.value)}
                        placeholder="Select a file path..."
                    />
                    <button className="btn" onClick={handleBrowse}>Browse...</button>
                </div>
            </div>

            {/* Object Creation Options */}
            {structureOpts.length > 0 && (
                <div className="backup-card">
                    <h2 className="option-section-title">Object Creation Options</h2>
                    <div className="option-group">
                        {structureOpts.map(def => (
                            <ToggleRow
                                key={def.key}
                                label={getLabel(def)}
                                checked={options[def.key]}
                                onChange={(val) => handleOptionChange(def.key, val)}
                            />
                        ))}
                    </div>
                </div>
            )}

            {/* Data Options */}
            {dataOpts.length > 0 && (
                <div className="backup-card">
                    <h2 className="option-section-title">Data Options</h2>
                    <div className="option-group">
                        {dataOpts.map(def => (
                            <ToggleRow
                                key={def.key}
                                label={getLabel(def)}
                                checked={options[def.key]}
                                onChange={(val) => handleOptionChange(def.key, val)}
                            />
                        ))}
                    </div>
                </div>
            )}

            {/* Other Options */}
            {otherOpts.length > 0 && (
                <div className="backup-card">
                    <h2 className="option-section-title">Other Options</h2>
                    <div className="option-group">
                        {otherOpts.map(def => (
                            <ToggleRow
                                key={def.key}
                                label={getLabel(def)}
                                checked={options[def.key]}
                                onChange={(val) => handleOptionChange(def.key, val)}
                            />
                        ))}
                    </div>
                </div>
            )}

            {/* Status */}
            {status && (
                <div className={`backup-status ${status.type}`}>
                    {status.type === 'info' && <span className="spinner" />}
                    {status.text}
                </div>
            )}

            {/* Footer */}
            <div className="backup-footer">
                <button className="btn" onClick={handleCancel}>Cancel</button>
                <button
                    className="btn btn-primary"
                    onClick={handleBackup}
                    disabled={executing || !filePath.trim()}
                >
                    {executing ? 'Backing up...' : 'Backup'}
                </button>
            </div>
        </div>
    );
}
