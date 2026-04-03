import React, { useState, useEffect, useCallback, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import './markdownViewApp.css';

declare function acquireVsCodeApi(): {
    postMessage(message: unknown): void;
    getState(): unknown;
    setState(state: unknown): void;
};

const vscode = acquireVsCodeApi();

type EmptyReason = 'no-sql-editor' | 'no-markdown' | 'file-deleted';

interface AppState {
    content: string;
    fileName: string;
    dirty: boolean;
    generating: boolean;
    emptyReason: EmptyReason | null;
    externalChange: boolean;
}

function App() {
    const [state, setState] = useState<AppState>({
        content: '',
        fileName: '',
        dirty: false,
        generating: false,
        emptyReason: null,
        externalChange: false,
    });
    const textareaRef = useRef<HTMLTextAreaElement>(null);

    useEffect(() => {
        const handler = (event: MessageEvent) => {
            const msg = event.data;
            switch (msg.command) {
                case 'updateContent':
                    setState(prev => ({
                        ...prev,
                        content: msg.data.content,
                        fileName: msg.data.fileName || prev.fileName,
                        dirty: msg.data.dirty ?? false,
                        emptyReason: null,
                        externalChange: false,
                    }));
                    break;
                case 'setGenerating':
                    setState(prev => ({ ...prev, generating: msg.data.generating }));
                    break;
                case 'showEmpty':
                    setState(prev => ({
                        ...prev,
                        emptyReason: msg.data.reason,
                        content: '',
                        fileName: '',
                        dirty: false,
                        externalChange: false,
                    }));
                    break;
                case 'externalChange':
                    setState(prev => ({ ...prev, externalChange: true }));
                    break;
                case 'setDirty':
                    setState(prev => ({ ...prev, dirty: msg.data.dirty }));
                    break;
            }
        };

        window.addEventListener('message', handler);
        vscode.postMessage({ command: 'viewReady' });
        return () => window.removeEventListener('message', handler);
    }, []);

    const handleChange = useCallback((e: React.ChangeEvent<HTMLTextAreaElement>) => {
        const newContent = e.target.value;
        setState(prev => ({ ...prev, content: newContent, dirty: true }));
        vscode.postMessage({ command: 'contentChanged', data: { content: newContent } });
    }, []);

    const handleSave = useCallback(() => {
        vscode.postMessage({ command: 'save' });
    }, []);

    const handleReload = useCallback(() => {
        vscode.postMessage({ command: 'reload' });
    }, []);

    const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
        if ((e.metaKey || e.ctrlKey) && e.key === 's') {
            e.preventDefault();
            handleSave();
        }
    }, [handleSave]);

    const handleCreate = useCallback(() => {
        vscode.postMessage({ command: 'createMarkdown' });
    }, []);

    const handleGenerate = useCallback(() => {
        vscode.postMessage({ command: 'generateMarkdown' });
    }, []);

    const handleDismissExternal = useCallback(() => {
        setState(prev => ({ ...prev, externalChange: false }));
    }, []);

    if (state.emptyReason) {
        return (
            <div className="markdown-panel">
                <div className="empty-state">
                    {state.emptyReason === 'no-sql-editor' && (
                        <span>Open a SQL query to view its markdown documentation.</span>
                    )}
                    {state.emptyReason === 'no-markdown' && (
                        <>
                            <span>No markdown documentation exists for this query yet.</span>
                            <div className="actions">
                                <button className="primary" onClick={handleCreate}>Create Markdown</button>
                                <button onClick={handleGenerate}>Generate Markdown</button>
                            </div>
                        </>
                    )}
                    {state.emptyReason === 'file-deleted' && (
                        <>
                            <span>Markdown file was deleted.</span>
                            <div className="actions">
                                <button className="primary" onClick={handleCreate}>Recreate Markdown</button>
                            </div>
                        </>
                    )}
                </div>
            </div>
        );
    }

    return (
        <div className="markdown-panel" onKeyDown={handleKeyDown}>
            {state.externalChange && (
                <div className="notification-bar">
                    <span className="message">File changed on disk.</span>
                    <button onClick={handleReload}>Reload</button>
                    <button onClick={handleDismissExternal}>Dismiss</button>
                </div>
            )}
            <div className="toolbar">
                <span className="filename">{state.fileName}</span>
                {state.dirty && <span className="dirty-dot" title="Unsaved changes" />}
                {state.generating && <span className="spinner" title="Generating..." />}
                <button onClick={handleSave} disabled={!state.dirty || state.generating}>Save</button>
                <button onClick={handleReload} disabled={state.generating}>Reload</button>
            </div>
            <div className="editor-area">
                <textarea
                    ref={textareaRef}
                    value={state.content}
                    onChange={handleChange}
                    disabled={state.generating}
                    spellCheck={false}
                />
            </div>
        </div>
    );
}

const root = createRoot(document.getElementById('root')!);
root.render(<App />);
