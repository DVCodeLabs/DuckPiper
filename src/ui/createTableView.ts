import * as vscode from 'vscode';
import { CreateTableDraft } from '../core/createTableSql';

export interface EditModeData {
  tableName: string;
  columns: Array<{ name: string; type: string; nullable?: boolean; comment?: string }>;
  primaryKey?: string[];
  foreignKeys?: Array<{ name?: string; column: string; foreignSchema: string; foreignTable: string; foreignColumn: string }>;
  indexes?: Array<{ name: string; columns: string[]; unique?: boolean }>;
}

export interface CreateTablePanelContext {
  connectionId: string;
  connectionName: string;
  schemaName: string;
  dialect: string;
  isLocalDuckDB?: boolean;
  editMode?: EditModeData;
}

export interface CreateTablePreviewPayload {
  connectionName: string;
  targetLabel: string;
  statements: string[];
}

export interface CreateTableResultPayload {
  ok: boolean;
  message: string;
}

interface CreateTablePanelHandlers {
  onPreview: (draft: CreateTableDraft) => Promise<CreateTablePreviewPayload>;
  onExecute: (draft: CreateTableDraft) => Promise<CreateTableResultPayload>;
  onPreviewAlter?: (original: CreateTableDraft, current: CreateTableDraft) => Promise<CreateTablePreviewPayload>;
  onExecuteAlter?: (original: CreateTableDraft, current: CreateTableDraft) => Promise<CreateTableResultPayload>;
  onDropTable?: () => Promise<CreateTableResultPayload>;
}

export class CreateTableView {
  public static currentPanel: CreateTableView | undefined;

  private readonly panel: vscode.WebviewPanel;
  private readonly extensionUri: vscode.Uri;
  private readonly panelContext: CreateTablePanelContext;
  private readonly handlers: CreateTablePanelHandlers;
  private readonly disposables: vscode.Disposable[] = [];

  private constructor(
    panel: vscode.WebviewPanel,
    extensionUri: vscode.Uri,
    panelContext: CreateTablePanelContext,
    handlers: CreateTablePanelHandlers
  ) {
    this.panel = panel;
    this.extensionUri = extensionUri;
    this.panelContext = panelContext;
    this.handlers = handlers;

    this.panel.onDidDispose(() => this.dispose(), null, this.disposables);
    this.panel.webview.html = this.getWebviewContent(this.panel.webview, this.extensionUri);
    this.setMessageListener(this.panel.webview);
  }

  public static render(
    extensionUri: vscode.Uri,
    panelContext: CreateTablePanelContext,
    handlers: CreateTablePanelHandlers
  ): void {
    if (CreateTableView.currentPanel) {
      CreateTableView.currentPanel.dispose();
    }

    const isEdit = Boolean(panelContext.editMode);
    const title = isEdit
      ? `Edit Table: ${panelContext.schemaName}.${panelContext.editMode!.tableName}`
      : `Create Table: ${panelContext.schemaName}`;

    const panel = vscode.window.createWebviewPanel(
      'createTable',
      title,
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
        localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'dist')]
      }
    );

    CreateTableView.currentPanel = new CreateTableView(panel, extensionUri, panelContext, handlers);
  }

  public dispose(): void {
    CreateTableView.currentPanel = undefined;
    this.panel.dispose();
    while (this.disposables.length) {
      const disposable = this.disposables.pop();
      if (disposable) {
        disposable.dispose();
      }
    }
  }

  private setMessageListener(webview: vscode.Webview): void {
    webview.onDidReceiveMessage(
      async (message: Record<string, unknown>) => {
        const command = message?.command;
        const data = message?.data as Record<string, unknown> | undefined;

        if (command === 'ready') {
          webview.postMessage({ command: 'setContext', data: this.panelContext });
          return;
        }

        if (command === 'cancel') {
          this.dispose();
          return;
        }

        if (command === 'previewCreateTable') {
          const draft = data?.draft as CreateTableDraft | undefined;
          if (!draft) {
            webview.postMessage({ command: 'createTableResult', data: { ok: false, message: 'Missing draft payload.' } });
            return;
          }

          try {
            const preview = await this.handlers.onPreview(draft);
            webview.postMessage({ command: 'createTablePreview', data: preview });
          } catch (error: unknown) {
            webview.postMessage({
              command: 'createTableResult',
              data: {
                ok: false,
                message: error instanceof Error ? error.message : 'Failed to generate SQL preview.'
              }
            });
          }
          return;
        }

        if (command === 'executeCreateTable') {
          const draft = data?.draft as CreateTableDraft | undefined;
          if (!draft) {
            webview.postMessage({ command: 'createTableResult', data: { ok: false, message: 'Missing draft payload.' } });
            return;
          }

          try {
            const result = await this.handlers.onExecute(draft);
            webview.postMessage({ command: 'createTableResult', data: result });
          } catch (error: unknown) {
            webview.postMessage({
              command: 'createTableResult',
              data: {
                ok: false,
                message: error instanceof Error ? error.message : 'Failed to create table.'
              }
            });
          }
          return;
        }

        if (command === 'previewAlterTable') {
          const original = data?.original as CreateTableDraft | undefined;
          const current = data?.current as CreateTableDraft | undefined;
          if (!original || !current || !this.handlers.onPreviewAlter) {
            webview.postMessage({ command: 'createTableResult', data: { ok: false, message: 'Missing alter payload.' } });
            return;
          }

          try {
            const preview = await this.handlers.onPreviewAlter(original, current);
            webview.postMessage({ command: 'createTablePreview', data: preview });
          } catch (error: unknown) {
            webview.postMessage({
              command: 'createTableResult',
              data: {
                ok: false,
                message: error instanceof Error ? error.message : 'Failed to generate ALTER TABLE preview.'
              }
            });
          }
          return;
        }

        if (command === 'executeAlterTable') {
          const original = data?.original as CreateTableDraft | undefined;
          const current = data?.current as CreateTableDraft | undefined;
          if (!original || !current || !this.handlers.onExecuteAlter) {
            webview.postMessage({ command: 'createTableResult', data: { ok: false, message: 'Missing alter payload.' } });
            return;
          }

          try {
            const result = await this.handlers.onExecuteAlter(original, current);
            webview.postMessage({ command: 'createTableResult', data: result });
          } catch (error: unknown) {
            webview.postMessage({
              command: 'createTableResult',
              data: {
                ok: false,
                message: error instanceof Error ? error.message : 'Failed to alter table.'
              }
            });
          }
          return;
        }

        if (command === 'dropTable') {
          if (!this.handlers.onDropTable) {
            webview.postMessage({ command: 'createTableResult', data: { ok: false, message: 'Drop handler not available.' } });
            return;
          }

          try {
            const result = await this.handlers.onDropTable();
            webview.postMessage({ command: 'createTableResult', data: result });
          } catch (error: unknown) {
            webview.postMessage({
              command: 'createTableResult',
              data: {
                ok: false,
                message: error instanceof Error ? error.message : 'Failed to drop table.'
              }
            });
          }
          return;
        }
      },
      undefined,
      this.disposables
    );
  }

  private getWebviewContent(webview: vscode.Webview, extensionUri: vscode.Uri): string {
    const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'dist', 'createTableApp.js'));
    const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'dist', 'createTableApp.css'));
    const isEdit = Boolean(this.panelContext.editMode);
    const pageTitle = isEdit ? 'Edit Table' : 'Create Table';

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src ${webview.cspSource} 'unsafe-inline';">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="${styleUri}">
  <title>${pageTitle}</title>
</head>
<body>
  <div id="root"></div>
  <script src="${scriptUri}"></script>
</body>
</html>`;
  }
}
