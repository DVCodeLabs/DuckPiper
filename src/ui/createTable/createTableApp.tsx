import * as React from 'react';
import * as ReactDOM from 'react-dom/client';
import { CreateTableForm } from './CreateTableForm';
import './createTable.css';

declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

const rootElement = document.getElementById('root');
if (rootElement) {
  const root = ReactDOM.createRoot(rootElement);
  root.render(<CreateTableForm vscode={vscode} />);
}
