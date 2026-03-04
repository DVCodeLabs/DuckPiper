import * as React from 'react';
import * as ReactDOM from 'react-dom/client';
import { ConnectionForm } from './ConnectionForm';
import './connectionForm.css';

// VS Code API
declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

const rootElement = document.getElementById('root');
if (rootElement) {
    const root = ReactDOM.createRoot(rootElement);
    root.render(<ConnectionForm vscode={vscode} />);
}
