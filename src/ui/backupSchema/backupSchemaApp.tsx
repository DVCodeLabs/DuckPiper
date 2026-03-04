import * as React from 'react';
import * as ReactDOM from 'react-dom/client';
import { BackupSchemaForm } from './BackupSchemaForm';
import './backupSchema.css';

declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

const rootElement = document.getElementById('root');
if (rootElement) {
    const root = ReactDOM.createRoot(rootElement);
    root.render(<BackupSchemaForm vscode={vscode} />);
}
