import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';

// Type extension for VS Code webview API
declare const acquireVsCodeApi: () => {
    postMessage: (message: any) => void;
    getState: () => any;
    setState: (state: any) => void;
};

const vscode = acquireVsCodeApi();

type InitStructureEntry = {
    folder: string;
    files: string[];
};

// Folder + default file structure that Initialize creates
const INIT_STRUCTURE: InitStructureEntry[] = [
    { folder: '(project root)', files: ['AGENTS.md (or AGENTS_DP.md)', 'README_DP.md'] },
    { folder: 'DP', files: [] },
    { folder: 'DP/queries', files: [] },
    { folder: 'DP/schemas', files: [] },
    { folder: 'DP/notebooks', files: [] },
    { folder: 'DP/system', files: ['piper.duckdb'] },
    { folder: 'DP/system/queries', files: ['queryIndex.json', 'queryHistory.json (after first query run)'] },
    { folder: 'DP/system/pipelines', files: ['pipelineIndex.json'] },
    { folder: 'DP/system/erd', files: [] },
    {
        folder: 'DP/system/prompts',
        files: ['markdownDoc.txt', 'inlineComments.txt', 'describeSchema.txt', 'notebookMarkdownDoc.txt']
    }
];

// Styles
const styles: Record<string, React.CSSProperties> = {
    container: {
        fontFamily: 'var(--vscode-font-family)',
        color: 'var(--vscode-foreground)',
        backgroundColor: 'var(--vscode-editor-background)',
        padding: '24px',
        maxWidth: '800px',
        margin: '0 auto'
    },
    header: {
        marginBottom: '24px',
        borderBottom: '1px solid var(--vscode-panel-border)',
        paddingBottom: '16px'
    },
    title: {
        fontSize: '24px',
        fontWeight: 600,
        margin: 0,
        marginBottom: '8px',
        display: 'flex',
        alignItems: 'center',
        gap: '10px'
    },
    trustStatement: {
        fontSize: '13px',
        color: 'var(--vscode-descriptionForeground)',
        margin: 0,
        padding: '8px 12px',
        backgroundColor: 'var(--vscode-textBlockQuote-background)',
        borderLeft: '3px solid var(--vscode-textLink-activeForeground)',
        borderRadius: '2px'
    },
    card: {
        backgroundColor: 'var(--vscode-sideBar-background)',
        border: '1px solid var(--vscode-panel-border)',
        borderRadius: '6px',
        padding: '16px',
        marginBottom: '16px'
    },
    cardTitle: {
        fontSize: '14px',
        fontWeight: 600,
        marginBottom: '12px',
        marginTop: 0,
        marginLeft: 0,
        marginRight: 0
    },
    statusBadge: {
        display: 'inline-block',
        padding: '4px 12px',
        borderRadius: '12px',
        fontSize: '12px',
        fontWeight: 500
    },
    statusInitialized: {
        backgroundColor: 'var(--vscode-testing-iconPassed)',
        color: 'var(--vscode-editor-background)'
    },
    statusNotInitialized: {
        backgroundColor: 'var(--vscode-testing-iconFailed)',
        color: 'var(--vscode-editor-background)'
    },
    button: {
        padding: '8px 16px',
        fontSize: '13px',
        fontWeight: 500,
        border: 'none',
        borderRadius: '4px',
        cursor: 'pointer',
        marginRight: '8px',
        marginBottom: '8px',
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px'
    },
    primaryButton: {
        backgroundColor: 'var(--vscode-button-background)',
        color: 'var(--vscode-button-foreground)'
    },
    secondaryButton: {
        backgroundColor: 'var(--vscode-button-secondaryBackground)',
        color: 'var(--vscode-button-secondaryForeground)'
    },
    folderList: {
        margin: 0,
        padding: 0,
        listStyle: 'none',
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))',
        gap: '8px'
    },
    folderItem: {
        padding: '8px',
        backgroundColor: 'var(--vscode-textCodeBlock-background)',
        borderRadius: '3px'
    },
    folderName: {
        fontSize: '12px',
        fontFamily: 'var(--vscode-editor-font-family)',
        fontWeight: 600
    },
    fileList: {
        margin: '6px 0 0 0',
        padding: 0,
        listStyle: 'none'
    },
    fileItem: {
        fontSize: '12px',
        fontFamily: 'var(--vscode-editor-font-family)',
        color: 'var(--vscode-descriptionForeground)',
        marginTop: '2px'
    },
    emptyFolder: {
        marginTop: '6px',
        fontSize: '12px',
        color: 'var(--vscode-descriptionForeground)',
        fontStyle: 'italic'
    },
    linkList: {
        margin: 0,
        padding: 0,
        listStyle: 'none'
    },
    link: {
        color: 'var(--vscode-textLink-foreground)',
        textDecoration: 'none',
        cursor: 'pointer',
        fontSize: '13px',
        display: 'block',
        padding: '4px 0'
    }
};

function App() {
    const [initialized, setInitialized] = useState<boolean | null>(null);

    useEffect(() => {
        // Listen for messages from extension
        const handler = (event: MessageEvent) => {
            const message = event.data;
            if (message.command === 'setStatus') {
                setInitialized(message.initialized);
            }
        };
        window.addEventListener('message', handler);

        // Signal ready
        vscode.postMessage({ command: 'ready' });

        return () => window.removeEventListener('message', handler);
    }, []);

    const handleInitialize = () => {
        vscode.postMessage({ command: 'initialize' });
    };

    const handleAddConnection = () => {
        vscode.postMessage({ command: 'addConnection' });
    };

    const handleOpenSettings = () => {
        vscode.postMessage({ command: 'openSettings' });
    };

    const handleOpenReadme = () => {
        vscode.postMessage({ command: 'openReadme' });
    };

    return (
        <div style={styles.container}>
            {/* Header */}
            <div style={styles.header}>
                <h1 style={styles.title}>
                    Duck Piper
                </h1>
                <p style={styles.trustStatement}>
                    Duck Piper will not create project files until you click Initialize.
                </p>
            </div>

            {/* Workspace Status */}
            <div style={styles.card}>
                <h2 style={styles.cardTitle}><span style={{marginRight: '4px'}}>Workspace Status </span>
                {initialized === null ? (
                    <span style={{ ...styles.statusBadge, backgroundColor: 'var(--vscode-descriptionForeground)', fontSize: '9px' }}>
                        Checking...
                    </span>
                ) : initialized ? (
                    <span style={{ ...styles.statusBadge, ...styles.statusInitialized, fontSize: '9px' }}>
                        ✓ Initialized
                    </span>
                ) : (
                    <span style={{ ...styles.statusBadge, ...styles.statusNotInitialized, fontSize: '9px' }}>
                        Not Initialized
                    </span>
                )}
                </h2>
                {!initialized && (
                    <button
                        style={{ ...styles.button, ...styles.primaryButton }}
                        onClick={handleInitialize}
                    >
                        Initialize Duck Piper
                    </button>
                )}
            </div>

            {/* What Initialize Creates */}
                <div style={styles.card}>
                    <h2 style={styles.cardTitle}>What does Initialization do?</h2>
                <div style={{ marginTop: '10px', marginBottom: '10px'}}>
                    Enables the built-in duckdb, pipelines, default files, and creates the following folder/file structure.
                </div>
                <div style={{ marginTop: '10px', marginBottom: '10px', fontSize: '13px', color: 'var(--vscode-descriptionForeground)' }}>
                    Resultset editing is enabled by default. You can disable it in DuckPiper Settings
                    (<code>dp.results.editing.enabled</code>).
                </div>
                <ul style={styles.folderList}>
                        {INIT_STRUCTURE.map(({ folder, files }) => (
                            <li key={folder} style={styles.folderItem}>
                                <div style={styles.folderName}>📁 {folder}</div>
                                {files.length > 0 ? (
                                    <ul style={styles.fileList}>
                                        {files.map((file) => (
                                            <li key={`${folder}/${file}`} style={styles.fileItem}>📄 {file}</li>
                                        ))}
                                    </ul>
                                ) : (
                                    <div style={styles.emptyFolder}>No default files at initialization</div>
                                )}
                            </li>
                        ))}
                    </ul>
                    <div style={{ marginTop: '10px', marginBottom: '10px'}}>
                        <div style={styles.folderName}>📄Initialization files:</div>
                        Duck Piper creates <code>AGENTS.md</code> and <code>README_DP.md</code> in your project root.<br />
                        If <code>AGENTS.md</code> already exists, it creates <code>AGENTS_DP.md</code> instead.
                    </div>
                </div>

            {/* Documentation */}
            <div style={styles.card}>
                <h2 style={styles.cardTitle}>Documentation</h2>
                <ul style={styles.linkList}>
                    <li>
                        <a
                            style={styles.link}
                            href="https://duckpiper.dev/docs"
                            target="_blank"
                        >
                            Duck Piper Documentation
                        </a>
                    </li>
                    <li>
                        <a
                            style={styles.link}
                            href="https://duckpiper.dev/docs/getting-started"
                            target="_blank"
                        >
                            Getting Started Guide
                        </a>
                    </li>
                    <li>
                        <a
                            style={styles.link}
                            href="https://duckpiper.dev/community"
                            target="_blank"
                        >
                            Community & Support
                        </a>
                    </li>
                </ul>
            </div>

            {/* Quick Actions */}
            {initialized && (
                <div style={styles.card}>
                    <h2 style={styles.cardTitle}>Quick Actions</h2>
                    <div>
                        <button
                            style={{ ...styles.button, ...styles.primaryButton }}
                            onClick={handleAddConnection}
                        >
                            ➕ Add Connection
                        </button>
                        <button
                            style={{ ...styles.button, ...styles.secondaryButton }}
                            onClick={handleOpenSettings}
                        >
                            ⚙️ Open Duck Piper Settings
                        </button>
                        <button
                            style={{ ...styles.button, ...styles.secondaryButton }}
                            onClick={handleOpenReadme}
                        >
                            📘 Open README_DP.md
                        </button>
                    </div>
                </div>
            )}
        </div>
    );
}

// Mount
const rootEl = document.getElementById('root');
if (rootEl) {
    createRoot(rootEl).render(<App />);
}
