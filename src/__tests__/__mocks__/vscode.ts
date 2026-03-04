/**
 * Mock implementation of the vscode module for unit testing.
 * This provides stubs for the most commonly used VSCode APIs.
 */

export enum ExtensionMode {
  Production = 1,
  Development = 2,
  Test = 3,
}

export enum ConfigurationTarget {
  Global = 1,
  Workspace = 2,
  WorkspaceFolder = 3,
}

export enum StatusBarAlignment {
  Left = 1,
  Right = 2,
}

export enum TreeItemCollapsibleState {
  None = 0,
  Collapsed = 1,
  Expanded = 2,
}

export enum ViewColumn {
  Active = -1,
  Beside = -2,
  One = 1,
  Two = 2,
  Three = 3,
}

export class Uri {
  static file(path: string): Uri {
    return new Uri('file', path);
  }

  static parse(value: string): Uri {
    const parts = value.split(':');
    return new Uri(parts[0], parts.slice(1).join(':'));
  }

  static joinPath(base: Uri, ...pathSegments: string[]): Uri {
    const joined = [base.path, ...pathSegments].join('/').replace(/\/+/g, '/');
    return new Uri(base.scheme, joined);
  }

  constructor(public scheme: string, public path: string) {}

  toString(): string {
    return `${this.scheme}:${this.path}`;
  }

  get fsPath(): string {
    return this.path;
  }

  with(change: { scheme?: string; path?: string }): Uri {
    return new Uri(change.scheme || this.scheme, change.path || this.path);
  }
}

export class Range {
  constructor(
    public start: Position,
    public end: Position
  ) {}
}

export class Position {
  constructor(
    public line: number,
    public character: number
  ) {}
}

export class Selection extends Range {
  constructor(
    public anchor: Position,
    public active: Position
  ) {
    super(anchor, active);
  }
}

export class EventEmitter<T> {
  private listeners: Array<(e: T) => void> = [];

  get event() {
    return (listener: (e: T) => void) => {
      this.listeners.push(listener);
      return {
        dispose: () => {
          const index = this.listeners.indexOf(listener);
          if (index > -1) {
            this.listeners.splice(index, 1);
          }
        },
      };
    };
  }

  fire(data: T): void {
    this.listeners.forEach((listener) => listener(data));
  }

  dispose(): void {
    this.listeners = [];
  }
}

export class Disposable {
  constructor(private callOnDispose: () => void) {}

  dispose(): void {
    this.callOnDispose();
  }

  static from(...disposables: Disposable[]): Disposable {
    return new Disposable(() => {
      disposables.forEach((d) => d.dispose());
    });
  }
}

export const workspace = {
  getConfiguration: jest.fn().mockReturnValue({
    get: jest.fn(),
    has: jest.fn(),
    inspect: jest.fn(),
    update: jest.fn(),
  }),
  workspaceFolders: [],
  fs: {
    readFile: jest.fn(),
    writeFile: jest.fn(),
    delete: jest.fn(),
    createDirectory: jest.fn(),
    readDirectory: jest.fn(),
    stat: jest.fn(),
  },
  onDidChangeConfiguration: jest.fn(),
  onDidSaveTextDocument: jest.fn(),
  onDidChangeTextDocument: jest.fn(),
  openTextDocument: jest.fn(),
  applyEdit: jest.fn(),
  textDocuments: [],
  registerTextDocumentContentProvider: jest.fn(),
};

export const window = {
  showInformationMessage: jest.fn(),
  showWarningMessage: jest.fn(),
  showErrorMessage: jest.fn(),
  showQuickPick: jest.fn(),
  showInputBox: jest.fn(),
  createOutputChannel: jest.fn().mockReturnValue({
    appendLine: jest.fn(),
    append: jest.fn(),
    clear: jest.fn(),
    show: jest.fn(),
    hide: jest.fn(),
    dispose: jest.fn(),
  }),
  createStatusBarItem: jest.fn().mockReturnValue({
    text: '',
    tooltip: '',
    command: undefined,
    color: undefined,
    backgroundColor: undefined,
    show: jest.fn(),
    hide: jest.fn(),
    dispose: jest.fn(),
  }),
  createTreeView: jest.fn(),
  registerTreeDataProvider: jest.fn(),
  registerWebviewViewProvider: jest.fn(),
  createWebviewPanel: jest.fn(),
  showTextDocument: jest.fn(),
  activeTextEditor: undefined,
  onDidChangeActiveTextEditor: jest.fn(),
  onDidChangeTextEditorSelection: jest.fn(),
  visibleTextEditors: [],
  withProgress: jest.fn((options, task) => task({ report: jest.fn() }, { isCancellationRequested: false })),
};

export const commands = {
  registerCommand: jest.fn(),
  executeCommand: jest.fn(),
  getCommands: jest.fn().mockResolvedValue([]),
};

export const languages = {
  registerCompletionItemProvider: jest.fn(),
  registerCodeLensProvider: jest.fn(),
  registerHoverProvider: jest.fn(),
  registerDocumentFormattingEditProvider: jest.fn(),
  createDiagnosticCollection: jest.fn(),
};

export class CancellationTokenSource {
  token = {
    isCancellationRequested: false,
    onCancellationRequested: jest.fn(),
  };

  cancel(): void {
    this.token.isCancellationRequested = true;
  }

  dispose(): void {}
}

export enum DiagnosticSeverity {
  Error = 0,
  Warning = 1,
  Information = 2,
  Hint = 3,
}

export class Diagnostic {
  constructor(
    public range: Range,
    public message: string,
    public severity?: DiagnosticSeverity
  ) {}
}

export class WorkspaceEdit {
  private edits: Map<string, unknown[]> = new Map();

  set(uri: Uri, edits: unknown[]): void {
    this.edits.set(uri.toString(), edits);
  }

  get(uri: Uri): unknown[] {
    return this.edits.get(uri.toString()) || [];
  }
}

export class ThemeIcon {
  constructor(public id: string) {}
}

export class ThemeColor {
  constructor(public id: string) {}
}

export class MarkdownString {
  constructor(public value: string = '') {}

  appendMarkdown(value: string): MarkdownString {
    this.value += value;
    return this;
  }

  appendCodeblock(value: string, language?: string): MarkdownString {
    this.value += `\`\`\`${language || ''}\n${value}\n\`\`\``;
    return this;
  }
}

export const env = {
  appName: 'Visual Studio Code',
  appRoot: '/test/app/root',
  language: 'en',
  clipboard: {
    writeText: jest.fn(),
    readText: jest.fn(),
  },
  openExternal: jest.fn(),
};

export const ExtensionContext = jest.fn().mockImplementation(() => ({
  subscriptions: [],
  workspaceState: {
    get: jest.fn(),
    update: jest.fn(),
    keys: jest.fn().mockReturnValue([]),
  },
  globalState: {
    get: jest.fn(),
    update: jest.fn(),
    keys: jest.fn().mockReturnValue([]),
    setKeysForSync: jest.fn(),
  },
  extensionPath: '/test/extension/path',
  extensionUri: Uri.file('/test/extension/path'),
  storagePath: '/test/storage/path',
  globalStoragePath: '/test/global/storage/path',
  logPath: '/test/log/path',
  extensionMode: ExtensionMode.Test,
}));
