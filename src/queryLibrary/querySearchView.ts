import * as vscode from 'vscode';
import { QueryIndex } from './queryIndex';
import { searchEntries, getRecentEntries } from './querySearch';
import { QueryIndexEntry } from './queryIndexer';
import { parseMdMetadata } from './mdParser';

const CMD_PREFIX = 'dp';
const OPEN_CMD = 'dp.query.openSaved';
const VIEW_ID = 'dp.querySearchView';

export class QuerySearchViewProvider implements vscode.WebviewViewProvider {
    private webviewView?: vscode.WebviewView;
    private query = '';
    private debounceTimer: ReturnType<typeof setTimeout> | undefined;

    constructor(private readonly queryIndex: QueryIndex) {
        queryIndex.onDidChange(() => this.update());
    }

    resolveWebviewView(
        webviewView: vscode.WebviewView,
        _context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken
    ): void {
        this.webviewView = webviewView;

        webviewView.webview.options = { enableScripts: true };
        webviewView.webview.html = this.getHtml();

        webviewView.webview.onDidReceiveMessage((msg) => {
            switch (msg.type) {
                case 'search':
                    this.query = msg.query;
                    if (this.debounceTimer) clearTimeout(this.debounceTimer);
                    this.debounceTimer = setTimeout(() => this.update(), 200);
                    break;
                case 'open':
                    this.openResult(msg.path, msg.connectionId);
                    break;
                case 'ready':
                    this.update();
                    break;
            }
        });
    }

    clearSearch(): void {
        this.query = '';
        this.webviewView?.webview.postMessage({ type: 'clear' });
        this.update();
    }

    private async update(): Promise<void> {
        if (!this.webviewView) return;

        const entries = this.queryIndex.getAllEntries();
        let results: Array<Record<string, unknown>>;
        let resultCount = 0;
        let mode: 'search' | 'recent' | 'empty';

        if (this.query.trim()) {
            const searchResults = searchEntries(entries, this.query, 'all');
            resultCount = searchResults.length;
            results = await Promise.all(searchResults.map(r => this.mapEntry(r.entry, r.snippet)));
            mode = resultCount > 0 ? 'search' : 'empty';
        } else {
            const recent = getRecentEntries(entries);
            results = await Promise.all(recent.map(e => this.mapEntry(e)));
            resultCount = results.length;
            mode = resultCount > 0 ? 'recent' : 'empty';
        }

        this.webviewView.webview.postMessage({
            type: 'results',
            mode,
            query: this.query,
            resultCount,
            results,
        });
    }

    private async mapEntry(entry: QueryIndexEntry, snippet?: string) {
        const result: Record<string, unknown> = {
            title: entry.mdTitle ?? entry.title ?? entry.path.split('/').pop() ?? entry.path,
            path: entry.path,
            tags: entry.mdTags,
            connectionName: entry.connectionName ?? undefined,
            snippet,
            docPath: entry.docPath,
            connectionId: entry.connectionId,
            lastRunAt: entry.lastRunAt,
        };

        // Read companion .md for tooltip
        const root = vscode.workspace.workspaceFolders?.[0]?.uri;
        if (root) {
            const mdRelPath = entry.path.replace(/\.(sql|postgres)$/i, '.md');
            const mdUri = vscode.Uri.joinPath(root, mdRelPath);
            try {
                const mdBytes = await vscode.workspace.fs.readFile(mdUri);
                const mdContent = Buffer.from(mdBytes).toString('utf8');
                const mdMeta = parseMdMetadata(mdContent);

                let rawBody = '';
                if (mdContent.startsWith('---')) {
                    const endIdx = mdContent.indexOf('\n---', 3);
                    if (endIdx !== -1) {
                        rawBody = mdContent.slice(endIdx + 4);
                    } else {
                        rawBody = mdContent;
                    }
                } else {
                    rawBody = mdContent;
                }

                let answersSection = '';
                const sectionStart = rawBody.indexOf('# What this query answers');
                if (sectionStart !== -1) {
                    const afterHeading = rawBody.slice(sectionStart + '# What this query answers'.length);
                    const nextSection = afterHeading.search(/^#\s/m);
                    answersSection = (nextSection !== -1 ? afterHeading.slice(0, nextSection) : afterHeading).trim();
                }

                const parts: string[] = [];
                if (mdMeta.title) parts.push(mdMeta.title);
                if (answersSection) {
                    // Strip markdown formatting for plain-text title attribute
                    parts.push(answersSection.replace(/[*_~`#]/g, '').replace(/\n{2,}/g, '\n').trim());
                }
                if (parts.length) result.tooltip = parts.join('\n\n');
            } catch {
                // No companion .md
            }
        }

        return result;
    }

    private openResult(path: string, connectionId?: string | null) {
        const root = vscode.workspace.workspaceFolders?.[0]?.uri;
        if (!root) return;
        const uri = vscode.Uri.joinPath(root, path);
        vscode.commands.executeCommand(OPEN_CMD, uri, connectionId);
    }

    private getHtml(): string {
        return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: var(--vscode-font-family);
    font-size: var(--vscode-font-size);
    color: var(--vscode-foreground);
    background: var(--vscode-sideBar-background);
    overflow-x: hidden;
  }
  .search-box {
    position: sticky; top: 0; z-index: 10;
    padding: 8px 8px 4px;
    background: var(--vscode-sideBar-background);
  }
  .search-box input {
    width: 100%;
    padding: 5px 8px;
    border: 1px solid var(--vscode-input-border, transparent);
    background: var(--vscode-input-background);
    color: var(--vscode-input-foreground);
    border-radius: 3px;
    outline: none;
    font-size: var(--vscode-font-size);
    font-family: var(--vscode-font-family);
  }
  .search-box input:focus {
    border-color: var(--vscode-focusBorder);
  }
  .search-box input::placeholder {
    color: var(--vscode-input-placeholderForeground);
  }
  .summary {
    padding: 4px 12px 4px;
    font-size: 11px;
    color: var(--vscode-descriptionForeground);
  }
  .results { padding: 0 0 8px; }
  .result-item {
    display: flex; flex-direction: column;
    padding: 5px 12px;
    cursor: pointer;
    border-left: 2px solid transparent;
  }
  .result-item:hover {
    background: var(--vscode-list-hoverBackground);
  }
  .result-item:focus {
    background: var(--vscode-list-focusBackground);
    border-left-color: var(--vscode-focusBorder);
    outline: none;
  }
  .result-title {
    font-size: 13px;
    color: var(--vscode-foreground);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .result-path {
    font-size: 11px;
    color: var(--vscode-descriptionForeground);
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .result-meta {
    display: flex; gap: 6px; flex-wrap: wrap; margin-top: 2px;
  }
  .tag {
    font-size: 10px;
    padding: 0 5px;
    border-radius: 3px;
    background: var(--vscode-badge-background);
    color: var(--vscode-badge-foreground);
  }
  .conn-badge {
    font-size: 10px;
    color: var(--vscode-descriptionForeground);
    font-style: italic;
  }
  .snippet {
    font-size: 11px;
    color: var(--vscode-descriptionForeground);
    margin-top: 2px;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .empty-state {
    padding: 20px 12px;
    text-align: center;
    color: var(--vscode-descriptionForeground);
    font-size: 12px;
  }
  .empty-state .icon { font-size: 24px; margin-bottom: 8px; }
</style>
</head>
<body>
  <div class="search-box">
    <input id="search" type="text" placeholder="Search title, tags, notes, SQL..." autofocus />
  </div>
  <div class="summary" id="summary"></div>
  <div class="results" id="results"></div>

<script>
  const vscode = acquireVsCodeApi();
  const input = document.getElementById('search');
  const summary = document.getElementById('summary');
  const results = document.getElementById('results');

  input.addEventListener('input', () => {
    vscode.postMessage({ type: 'search', query: input.value });
  });

  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const first = results.querySelector('.result-item');
      if (first) first.focus();
    }
    if (e.key === 'Escape') {
      input.value = '';
      vscode.postMessage({ type: 'search', query: '' });
    }
  });

  window.addEventListener('message', (event) => {
    const msg = event.data;
    if (msg.type === 'clear') {
      input.value = '';
      return;
    }
    if (msg.type === 'results') {
      renderResults(msg);
    }
  });

  function renderResults(msg) {
    if (msg.mode === 'empty' && msg.query) {
      summary.textContent = '';
      results.innerHTML = '<div class="empty-state">No queries found<br><span style="font-size:11px">Try broadening your search terms</span></div>';
      return;
    }
    if (msg.mode === 'empty') {
      summary.textContent = '';
      results.innerHTML = '<div class="empty-state">No saved queries<br><span style="font-size:11px">Add .sql files to your workspace</span></div>';
      return;
    }

    if (msg.mode === 'search') {
      summary.textContent = msg.resultCount + ' result' + (msg.resultCount === 1 ? '' : 's');
    } else {
      summary.textContent = 'Recent queries';
    }

    results.innerHTML = '';
    for (const r of msg.results) {
      const el = document.createElement('div');
      el.className = 'result-item';
      el.tabIndex = 0;

      if (r.tooltip) el.title = r.tooltip;

      let html = '<div class="result-title">' + esc(r.title) + '</div>';
      html += '<div class="result-path">' + esc(r.path) + '</div>';

      const hasMeta = (r.tags && r.tags.length) || r.connectionName;
      if (hasMeta) {
        html += '<div class="result-meta">';
        if (r.tags) {
          for (const t of r.tags) html += '<span class="tag">' + esc(t) + '</span>';
        }
        if (r.connectionName) html += '<span class="conn-badge">' + esc(r.connectionName) + '</span>';
        html += '</div>';
      }

      if (r.snippet) {
        html += '<div class="snippet">' + esc(r.snippet) + '</div>';
      }

      el.innerHTML = html;

      el.addEventListener('click', () => {
        vscode.postMessage({ type: 'open', path: r.path, connectionId: r.connectionId });
      });

      el.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
          vscode.postMessage({ type: 'open', path: r.path, connectionId: r.connectionId });
        }
        if (e.key === 'ArrowDown') {
          e.preventDefault();
          const next = el.nextElementSibling;
          if (next) next.focus();
        }
        if (e.key === 'ArrowUp') {
          e.preventDefault();
          const prev = el.previousElementSibling;
          if (prev) prev.focus();
          else input.focus();
        }
      });

      results.appendChild(el);
    }
  }

  function esc(s) {
    if (!s) return '';
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  }

  vscode.postMessage({ type: 'ready' });
</script>
</body>
</html>`;
    }
}

/**
 * Register query search commands and view for DuckPiper.
 */
export function registerQuerySearchView(
    context: vscode.ExtensionContext,
    queryIndex: QueryIndex
): QuerySearchViewProvider {
    const provider = new QuerySearchViewProvider(queryIndex);

    context.subscriptions.push(
        vscode.window.registerWebviewViewProvider(VIEW_ID, provider)
    );

    // Focus command
    context.subscriptions.push(
        vscode.commands.registerCommand(`${CMD_PREFIX}.query.search.focus`, () => {
            vscode.commands.executeCommand(`${VIEW_ID}.focus`);
        })
    );

    // Clear command
    context.subscriptions.push(
        vscode.commands.registerCommand(`${CMD_PREFIX}.query.search.clear`, () => {
            provider.clearSearch();
        })
    );

    // Rebuild index command
    context.subscriptions.push(
        vscode.commands.registerCommand(`${CMD_PREFIX}.query.search.rebuildIndex`, async () => {
            try {
                await vscode.window.withProgress(
                    { location: vscode.ProgressLocation.Notification, title: 'Rebuilding search index...' },
                    async () => {
                        await queryIndex.rebuildSearchMetadata();
                    }
                );
                vscode.window.showInformationMessage('Search index rebuilt successfully.');
            } catch (e) {
                vscode.window.showErrorMessage(`Failed to rebuild search index: ${e}`);
            }
        })
    );

    return provider;
}
