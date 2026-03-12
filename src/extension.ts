import * as vscode from "vscode";
import * as path from 'path';

import type { ScriptExecutionResult } from "./core/types";
import { canonicalizeSql } from "./core/hashing";
import { Logger } from "./core/logger";

// Tree views + watchers
import { ExplorerViewProvider, ExplorerItem } from "./connections/explorerView";
import { SavedQueriesViewProvider } from "./queryLibrary/savedQueriesView";
import { registerQuerySearchView } from "./queryLibrary/querySearchView";
import { registerDPWatchers } from "./core/watchers";

// Store initialization
import { initConnectionStore } from "./connections/connectionStore";
import { registerConnectionCommands } from "./connections/connectionCommands";
import { registerDuckDBCommands } from "./connections/duckdbCommands";
import { saveLoadedResultsToDuckDB, saveTableToDuckDB } from "./connections/systemCommands";
import { registerActionsMenus, attachConnectionsSelectionTracking } from "./ui/actionsMenus";
import { DPDocConnectionStore, DPSqlCodelensProvider } from "./ui/sqlCodelens";
import { registerSqlCodelensCommands } from "./ui/sqlCodelensCommands";
import { loadConnectionProfiles, saveConnectionProfile, getConnectionSecrets } from "./connections/connectionStore";
import { SqlFormattingProvider } from "./formatting/formatSql";
import { quoteIdentifier, resolveEffectiveSqlDialect, toSqlLiteral } from "./core/sqlUtils";

// NEW: context keys for enabling/disabling commands/UI
import { setHasActiveConnection, setHasActiveSchema, setHasSimilarQueries } from "./core/context";

// Schema diff
import { registerSchemaDiffCommands } from "./schema/diffCommands";
import { SchemaDiffContentProvider } from "./schema/diffProvider";

import {
  ConnectionProfile,
  ConnectionSecrets,
  DbDialect,
  QueryColumn,
  QueryResultMeta,
  QueryResultSource,
  ApplyResultsetEditsRequest,
  ApplyResultsetEditsResult,
  ResultsetRowEdit,
  SchemaIntrospection,
  TableModel,
  ColumnModel,
  RoutineParameterModel
} from './core/types';
import { getAdapter, registerAdapter } from './connections/adapterFactory';
import { setSecureQLSaveProfile } from './connections/adapterFactory';
import { DPCompletionProvider } from './completion/completionProvider';
import { ProviderRegistry } from './connections/providerRegistry';
import { DuckPiperExtensionApi } from './api';
import { refreshAllSecureQLProfiles } from './connections/secureqlStartupRefresh';

import { queryIndex } from './queryLibrary/queryIndex';

import { LocalDuckDBViewProvider } from './transform/localDuckDBView';
import { ResultsViewProvider } from './results/resultsView';
import { ERDViewProvider } from './erd/erdViewProvider';
import { LineageViewProvider } from './pipelines/lineageViewProvider';
import { updateProjectInitializedContext, isProjectInitialized } from './core/isProjectInitialized';
import { WelcomeView } from './ui/welcomeView';
import { CreateTableView, CreateTablePanelContext, CreateTableResultPayload } from './ui/createTableView';
import { buildCreateTableSql, buildAlterTableSql, buildDropTableSql, CreateTableDraft } from './core/createTableSql';

type ApplyResultsetEditsCommandPayload = ApplyResultsetEditsRequest & {
  confirmed?: boolean;
};

export async function activate(context: vscode.ExtensionContext): Promise<DuckPiperExtensionApi> {
  // Initialize logger first
  Logger.initialize("DuckPiper");
  Logger.info("DuckPiper extension activating...");

  let projectInitializedAtStartup = false;
  let autoWelcomeShownThisSession = false;
  const tablePreviewContextByDocUri = new Map<string, {
    sql: string;
    source: QueryResultSource;
    primaryKeyColumns: string[];
    editableColumns: string[];
  }>();
  const lastRunContextByDocUri = new Map<string, {
    refreshSql: string;
    userSql: string;
  }>();

  // 1. Register Panel View Providers IMMEDIATELY
  const resultsViewProvider = new ResultsViewProvider(context);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(ResultsViewProvider.viewType, resultsViewProvider, {
      webviewOptions: { retainContextWhenHidden: true }
    })
  );

  const erdViewProvider = new ERDViewProvider(context);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(ERDViewProvider.viewType, erdViewProvider, {
      webviewOptions: { retainContextWhenHidden: true }
    })
  );

  const lineageViewProvider = new LineageViewProvider(context);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider(LineageViewProvider.viewType, lineageViewProvider, {
      webviewOptions: { retainContextWhenHidden: true }
    })
  );

  // 2. Initialize Core Systems
  try {
    initConnectionStore(context);

    // Wire up SecureQL adapter's save callback so it can persist profile changes
    setSecureQLSaveProfile(saveConnectionProfile);

    // Check project initialization status (read-only)
    const initialized = await updateProjectInitializedContext();
    projectInitializedAtStartup = initialized;

    // Only initialize write-path systems if project is already initialized
    // This prevents automatic file creation before user explicitly initializes
    if (initialized) {
      // Initialize all project components
      await initializeProjectComponents(context);
    }

    // Startup refresh: sync server-controlled flags for all SecureQL connections (background, non-blocking)
    refreshAllSecureQLProfiles(loadConnectionProfiles, getConnectionSecrets, saveConnectionProfile).catch(() => {
      // Silently ignore startup refresh errors — flags will be refreshed on next query
    });
  } catch (err) {
    Logger.error("Failed to initialize core systems", err);
  }

  // -----------------------------
  // Tree Views registration
  // -----------------------------
  const explorerProvider = new ExplorerViewProvider(context);
  const savedQueriesProvider = new SavedQueriesViewProvider();
  // CodeLens Provider & Store (New)
  const codeLensStore = new DPDocConnectionStore(context.workspaceState);
  codeLensStore.loadFromWorkspaceState();

  // Simple cache for synchronous label lookup and default fallback
  // LOAD from workspace state immediately to avoid "pop-in" delay
  const CACHE_KEY = "dp.connectionNamesCache.v1";
  const FIRST_ID_KEY = "dp.firstConnectionId.v1";

  const savedCache = context.workspaceState.get<Record<string, string>>(CACHE_KEY, {});
  const connectionNameCache = new Map<string, string>(Object.entries(savedCache));

  let firstConnectionId: string | undefined = context.workspaceState.get<string>(FIRST_ID_KEY);
  const productionWarningItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 1000);
  productionWarningItem.text = '$(warning) PRODUCTION CONNECTION';
  productionWarningItem.tooltip = 'DuckPiper warning: this SQL editor is using a production-tagged connection.';
  productionWarningItem.backgroundColor = new vscode.ThemeColor('statusBarItem.errorBackground');
  productionWarningItem.color = new vscode.ThemeColor('statusBarItem.errorForeground');
  productionWarningItem.hide();
  context.subscriptions.push(productionWarningItem);

  const updateConnectionCache = async () => {
    try {
      const profiles = await loadConnectionProfiles();
      connectionNameCache.clear();
      profiles.forEach(p => connectionNameCache.set(p.id, p.name));

      if (profiles.length > 0) {
        firstConnectionId = profiles[0].id;
      } else {
        firstConnectionId = undefined;
      }

      // Persist the cache for next session speedup
      await context.workspaceState.update(CACHE_KEY, Object.fromEntries(connectionNameCache));
      await context.workspaceState.update(FIRST_ID_KEY, firstConnectionId);

      // Trigger forced refresh of lenses
      codeLensProvider.refresh();
      await refreshProductionWarningBar();
    } catch (e) {
      Logger.error("Failed to update connection cache", e);
    }
  };


  const getEffectiveConnectionId = (docId?: string) => {
    // 1. Check doc specific
    if (docId) return docId;
    // 2. Check global active
    const active = context.workspaceState.get<string>("dp.activeConnectionId");
    if (active) return active;
    // 3. Fallback to first
    return firstConnectionId;
  };

  const buildResultMeta = async (
    profile: ConnectionProfile,
    docUri: vscode.Uri,
    userSql: string,
    columns: QueryColumn[]
  ): Promise<QueryResultMeta> => {
    const resultId = createResultId();
    const editingEnabled = vscode.workspace.getConfiguration('dp').get<boolean>('results.editing.enabled', true);
    const base: QueryResultMeta = {
      resultId,
      editable: {
        enabled: false,
        reason: 'Resultset editing is disabled in settings.',
        primaryKeyColumns: [],
        editableColumns: []
      }
    };

    if (!editingEnabled) {
      return base;
    }

    if (profile.allowDataEdit === false) {
      base.editable.reason = 'Connection is configured as read-only for edits.';
      return base;
    }

    const previewCtx = tablePreviewContextByDocUri.get(docUri.toString());
    if (!previewCtx) {
      const resolved = await resolveEditableSourceFromQuery(profile, userSql);
      if (!resolved) {
        base.editable.reason = 'Only table preview or simple single-table SELECT queries are editable.';
        return base;
      }

      const mapped = mapEditableMetadataToResultColumns(columns, resolved.primaryKeyColumns, resolved.editableColumns);
      if (!mapped.ok) {
        base.editable.reason = mapped.reason;
        return base;
      }

      base.source = resolved.source;
      base.editable = {
        enabled: true,
        primaryKeyColumns: mapped.primaryKeyColumns,
        editableColumns: mapped.editableColumns
      };
      return base;
    }

    if (normalizeSqlForComparison(userSql) !== normalizeSqlForComparison(previewCtx.sql)) {
      const resolved = await resolveEditableSourceFromQuery(profile, userSql);
      if (!resolved) {
        base.editable.reason = 'Query text changed from table preview and is not a supported editable query.';
        return base;
      }

      const mapped = mapEditableMetadataToResultColumns(columns, resolved.primaryKeyColumns, resolved.editableColumns);
      if (!mapped.ok) {
        base.editable.reason = mapped.reason;
        return base;
      }

      base.source = resolved.source;
      base.editable = {
        enabled: true,
        primaryKeyColumns: mapped.primaryKeyColumns,
        editableColumns: mapped.editableColumns
      };
      return base;
    }

    const mappedPreview = mapEditableMetadataToResultColumns(columns, previewCtx.primaryKeyColumns, previewCtx.editableColumns);
    if (!mappedPreview.ok) {
      base.editable.reason = mappedPreview.reason;
      return base;
    }

    base.source = previewCtx.source;
    base.editable = {
      enabled: true,
      primaryKeyColumns: mappedPreview.primaryKeyColumns,
      editableColumns: mappedPreview.editableColumns
    };

    return base;
  };

  const resolveEditableSourceFromQuery = async (
    profile: ConnectionProfile,
    sql: string
  ): Promise<{ source: QueryResultSource; primaryKeyColumns: string[]; editableColumns: string[] } | null> => {
    const parsed = parseSimpleSelectSource(sql);
    if (!parsed) {
      return null;
    }

    const { loadSchemas } = require('./schema/schemaStore');
    const allSchemas = await loadSchemas();
    const intro = allSchemas.find((s: SchemaIntrospection) => s.connectionId === profile.id);
    if (!intro) {
      return null;
    }

    const schemaMatches = (schemaName: string, target: string | undefined): boolean => {
      if (!target) return true;
      const left = schemaName.toLowerCase();
      const right = target.toLowerCase();
      return left === right || left.endsWith(`.${right}`);
    };

    const tableMatches = (tableName: string, target: string): boolean =>
      tableName.toLowerCase() === target.toLowerCase();

    const candidates: Array<{ schemaName: string; table: TableModel }> = [];
    for (const schema of intro.schemas || []) {
      if (!schemaMatches(schema.name, parsed.schema)) continue;
      for (const table of schema.tables || []) {
        if (tableMatches(table.name, parsed.table)) {
          candidates.push({ schemaName: schema.name, table });
        }
      }
      for (const view of (schema.views || [])) {
        if (tableMatches(view.name, parsed.table)) {
          candidates.push({ schemaName: schema.name, table: view });
        }
      }
    }

    if (candidates.length !== 1) {
      return null;
    }

    const match = candidates[0];
    const primaryKeyColumns: string[] = Array.isArray(match.table.primaryKey) ? match.table.primaryKey : [];
    if (primaryKeyColumns.length === 0) {
      return null;
    }

    const editableColumns: string[] = Array.isArray(match.table.columns)
      ? match.table.columns.map((c: ColumnModel) => c.name)
      : [];

    return {
      source: {
        catalog: parsed.catalog,
        schema: parsed.schema || match.schemaName,
        table: match.table.name
      },
      primaryKeyColumns,
      editableColumns
    };
  };

  const mapEditableMetadataToResultColumns = (
    columns: QueryColumn[],
    primaryKeyColumns: string[],
    editableColumns: string[]
  ): { ok: true; primaryKeyColumns: string[]; editableColumns: string[] } | { ok: false; reason: string } => {
    const resultColumns = columns.map((c) => c.name);
    const findResultColumn = (target: string): string | undefined =>
      resultColumns.find((col) => col.toLowerCase() === target.toLowerCase());

    const mappedPrimaryKeys: string[] = [];
    for (const pk of primaryKeyColumns) {
      const resolved = findResultColumn(pk);
      if (!resolved) {
        return { ok: false, reason: `Primary key column missing from result: ${pk}` };
      }
      mappedPrimaryKeys.push(resolved);
    }

    const pkSet = new Set(mappedPrimaryKeys.map((pk) => pk.toLowerCase()));
    const mappedEditable = editableColumns
      .map((name) => findResultColumn(name))
      .filter((name): name is string => !!name)
      .filter((name) => !pkSet.has(name.toLowerCase()));

    return {
      ok: true,
      primaryKeyColumns: mappedPrimaryKeys,
      editableColumns: Array.from(new Set(mappedEditable))
    };
  };

  const normalizeConnectionTag = (value: unknown): string => {
    if (typeof value !== 'string') return '';
    return value.trim().toLowerCase();
  };

  const getActiveSqlConnectionProfile = async (): Promise<ConnectionProfile | undefined> => {
    const editor = vscode.window.activeTextEditor;
    if (!editor || !isSqlDoc(editor.document)) return undefined;

    const docConnectionId = codeLensStore.get(editor.document);
    const effectiveId = getEffectiveConnectionId(docConnectionId);
    if (!effectiveId) return undefined;

    const profiles = await loadConnectionProfiles();
    return profiles.find((p) => p.id === effectiveId);
  };

  const refreshProductionWarningBar = async (): Promise<void> => {
    const profile = await getActiveSqlConnectionProfile();
    const taggedProfile = profile as (ConnectionProfile & { tag?: string }) | undefined;
    const tag = normalizeConnectionTag(taggedProfile?.connectionTag ?? taggedProfile?.tag);

    if (!profile || tag !== 'production') {
      productionWarningItem.hide();
      return;
    }

    productionWarningItem.text = `$(warning) PRODUCTION: ${profile.name}`;
    productionWarningItem.tooltip = `DuckPiper warning: "${profile.name}" is tagged as production.`;
    productionWarningItem.show();
  };

  const getConnectionLabel = (id?: string) => {
    // If we passed an ID, look it up. 
    // BUT the provider logic usually passes the stored ID. 
    // If the stored ID is undefined, we need to resolve what the "effective" ID would be to show the label.
    // However, the provider calls this with the *result* of store.get(doc).
    // So if id is undefined, it means "no doc override".

    // We actually need to refactor the provider slightly or handle it here.
    // The provider currently calls store.get(doc), then passes that to us.
    // If that is undefined, we should check active -> first.

    const effectiveId = getEffectiveConnectionId(id);

    if (!effectiveId) return "Select Connection";

    // Return cached name or "Loading..." if not yet cached (avoid showing raw ID)
    const cachedName = connectionNameCache.get(effectiveId);
    if (cachedName) return cachedName;

    // If not in cache, return a placeholder - the cache will update and trigger refresh
    return "Loading...";
  };

  const codeLensProvider = new DPSqlCodelensProvider(codeLensStore, getConnectionLabel);

  context.subscriptions.push(
    vscode.languages.registerCodeLensProvider({ language: "sql" }, codeLensProvider)
  );

  // SQL Formatting Provider
  const formattingProvider = new SqlFormattingProvider(codeLensStore, context);
  context.subscriptions.push(
    vscode.commands.registerCommand('dp.sql.formatDocument', async () => {
      const editor = vscode.window.activeTextEditor;
      if (editor) {
        await formattingProvider.formatDocument(editor);
      }
    })
  );

  // Initial cache load (only if project is initialized to avoid default connection creation)
  if (await isProjectInitialized()) {
    updateConnectionCache();
  }


  registerSqlCodelensCommands(
    context,
    codeLensStore,
    () => codeLensProvider.refresh(),
    () => { void refreshProductionWarningBar(); }
  );

  // Local DuckDB Provider
  const localDuckDBProvider = new LocalDuckDBViewProvider(context);

  // Note: Local DuckDB initialization is now handled in initializeProjectComponents called above
  // if the project is initialized.

  // ERD and Lineage View Providers registered early above

  // Toggle System Schemas Command
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.schema.toggleSystemSchemas", async () => {
      const key = 'dp.ui.showSystemSchemas';
      const current = context.workspaceState.get<boolean>(key, false);
      await context.workspaceState.update(key, !current);

      localDuckDBProvider.refresh();
      explorerProvider.refresh();
    })
  );

  // Explorer View
  const explorerTreeView = vscode.window.createTreeView("dp.explorerView", {
    treeDataProvider: explorerProvider
  });
  const explorerTreeViewBuiltin = vscode.window.createTreeView("dp.explorerViewBuiltin", {
    treeDataProvider: explorerProvider
  });
  // Attach selection tracking
  attachConnectionsSelectionTracking(explorerTreeView);
  attachConnectionsSelectionTracking(explorerTreeViewBuiltin);

  context.subscriptions.push(
    explorerTreeView,
    explorerTreeViewBuiltin,
    vscode.window.registerTreeDataProvider("dp.savedQueriesView", savedQueriesProvider),
    vscode.window.registerTreeDataProvider("dp.localDuckDBView", localDuckDBProvider),
    vscode.window.registerTreeDataProvider("dp.localDuckDBViewBuiltin", localDuckDBProvider)
  );

  // Register Query Search sidebar view
  registerQuerySearchView(context, queryIndex);

  // Register Menus
  registerActionsMenus(context);

  // Register Connection Commands
  registerConnectionCommands(context, explorerProvider);

  // Register Schema Diff Commands
  registerSchemaDiffCommands(context, explorerProvider);
  context.subscriptions.push(
    vscode.workspace.registerTextDocumentContentProvider('dp-diff', new SchemaDiffContentProvider())
  );

  // -----------------------------
  // Default contexts (NEW)
  // -----------------------------
  await setHasActiveConnection(false);
  await setHasActiveSchema(false);

  // -----------------------------
  // View header refresh commands (NEW)
  // -----------------------------
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.view.refreshConnections", async () => {
      explorerProvider.refresh();
      // Avoid creating DP files before explicit initialization
      if (await isProjectInitialized()) {
        updateConnectionCache(); // Also refresh our internal name cache
      }
    }),
    vscode.commands.registerCommand("dp.openSettings", async () => {
      await vscode.commands.executeCommand("workbench.action.openSettings", "dp");
    }),
    vscode.commands.registerCommand("dp.welcome.open", () => {
      WelcomeView.render(context.extensionUri);
    }),
    vscode.commands.registerCommand("dp.project.initialize", async () => {
      try {
        const { ensureDPDirs, ensureAgentsMd } = require('./core/fsWorkspace');

        // Create folder structure
        await ensureDPDirs();

        // AGENTS.md should only be handled during explicit initialization,
        // not during extension startup component initialization.
        await ensureAgentsMd();

        // Initialize all systems
        await initializeProjectComponents(context);

        await updateProjectInitializedContext();
        explorerProvider.refresh();
        localDuckDBProvider.refresh();

        vscode.window.showInformationMessage('DuckPiper project initialized successfully!');
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Initialization failed: ${msg}`);
      }
    }),
    vscode.commands.registerCommand("dp.view.refreshSchemas", async (skipIntrospection?: boolean) => {
      const { loadConnectionProfiles } = require('./connections/connectionStore');
      const { performIntrospection } = require('./connections/connectionCommands');

      if (skipIntrospection !== true) {
        // Introspect ALL user connections (not just the active one)
        const profiles = await loadConnectionProfiles();
        for (const profile of profiles) {
          await performIntrospection(profile, true); // silent
        }
      }

      explorerProvider.refresh(); // Refreshes everything (connections + schemas)
    }),
    vscode.commands.registerCommand("dp.view.refreshSavedQueries", () => savedQueriesProvider.refresh()),
    vscode.commands.registerCommand("dp.view.refreshLocalDuckDB", () => localDuckDBProvider.refresh()),
    vscode.commands.registerCommand("dp.query.deleteSaved", async (item: unknown) => {
      const { deleteSavedQuery } = require('./queryLibrary/deleteSavedQuery');
      await deleteSavedQuery(item);
    })
  );

  const maybeAutoOpenWelcome = async () => {
    if (autoWelcomeShownThisSession) return;
    if ((vscode.workspace.workspaceFolders?.length ?? 0) === 0) return;
    if (await isProjectInitialized()) return;

    autoWelcomeShownThisSession = true;
    await vscode.commands.executeCommand("workbench.view.extension.dp");
    await vscode.commands.executeCommand("dp.welcome.open");
  };

  // Auto-open sidebar + Welcome when project is not initialized.
  // Covers both activation-time workspaces and folders added after activation.
  if (!projectInitializedAtStartup) {
    await maybeAutoOpenWelcome();
  }

  context.subscriptions.push(
    vscode.workspace.onDidChangeWorkspaceFolders(() => {
      void maybeAutoOpenWelcome();
    })
  );

  // Insert text helper used by schema tree clicks
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.editor.insertText", async (text: string) => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) return;
      await editor.edit((eb) => eb.insert(editor.selection.active, text));
    }),
    vscode.commands.registerCommand("dp.editor.insertRoutineCall", async (item?: ExplorerItem) => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) return;

      const routine = item?.routine;
      const schemaName = typeof item?.schemaName === 'string' ? item.schemaName : undefined;
      const routineName = typeof routine?.name === 'string' ? routine.name : undefined;
      const identifier = typeof routine?.schemaQualifiedName === 'string'
        ? routine.schemaQualifiedName
        : (schemaName && routineName ? `${schemaName}.${routineName}` : (routineName || 'routine_name'));

      const params: RoutineParameterModel[] = Array.isArray(routine?.parameters)
        ? routine.parameters
          .filter((p: RoutineParameterModel) => p?.mode !== 'return')
          .sort((a: RoutineParameterModel, b: RoutineParameterModel) => (a?.position ?? 0) - (b?.position ?? 0))
        : [];
      const args = params
        .map((p: RoutineParameterModel, index: number) => {
          const name = typeof p?.name === 'string' && p.name.length > 0 ? p.name : `arg${index + 1}`;
          const type = typeof p?.type === 'string' && p.type.length > 0 ? `: ${p.type}` : '';
          return `/* ${name}${type} */`;
        })
        .join(', ');

      const sql = routine?.kind === 'procedure'
        ? `CALL ${identifier}(${args});`
        : `SELECT ${identifier}(${args});`;

      await editor.edit((eb) => eb.insert(editor.selection.active, sql));
    })
  );

  // Active connection selection (UPDATED: persist + set context)
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.connection.select", async (itemOrProfile) => {
      let profile = itemOrProfile;
      // Handle call from Tree View context (ConnectionItem)
      if (itemOrProfile && itemOrProfile.profile) {
        profile = itemOrProfile.profile;
      }

      if (!profile?.id) return;
      await context.workspaceState.update("dp.activeConnectionId", profile.id);
      await setHasActiveConnection(true);

      // Set context for IsLocalCache
      await vscode.commands.executeCommand('setContext', 'dp.activeConnectionIsLocalCache', !!profile.isLocalDuckPiperCacheDb);

      explorerProvider.setActiveId(profile.id); // Update view

      void vscode.window.setStatusBarMessage(`DuckPiper: selected connection "${profile.name}"`, 2500);

      // Persist to queryIndex for active editor
      const editor = vscode.window.activeTextEditor;
      if (editor && isSqlDoc(editor.document)) {
        await queryIndex.updateConnectionContext(editor.document.uri, profile.id, profile.name, profile.dialect);
      }

      void refreshProductionWarningBar();
    })
  );

  // ERD Command
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.erd.open", async () => {
      const connId = context.workspaceState.get<string>("dp.activeConnectionId");
      if (!connId) {
        vscode.window.showErrorMessage("No active connection selected.");
        return;
      }

      const { getConnection, getConnectionSecrets } = require('./connections/connectionStore');
      const profile = await getConnection(connId);
      const secrets = await getConnectionSecrets(connId);

      if (profile) {
        await erdViewProvider.showERD(profile, secrets);
      }
    }),
    vscode.commands.registerCommand("dp.erd.openLocalDuckDB", async () => {
      const { getConnection, getConnectionSecrets } = require('./connections/connectionStore');
      // Hardcoded ID for Local DuckDB in Data Work panel
      const profile = await getConnection('duck-piper-local-data-work');

      if (!profile) {
        vscode.window.showErrorMessage("Duck_Piper connection not found.");
        return;
      }

      const secrets = await getConnectionSecrets('duck-piper-local-data-work');
      await erdViewProvider.showERD(profile, secrets);
    })
  );

  // Initial load of active connection
  const initialConnId = context.workspaceState.get<string>("dp.activeConnectionId");
  if (initialConnId) {
    explorerProvider.setActiveId(initialConnId);
    await setHasActiveConnection(true);
  }
  void refreshProductionWarningBar();

  // RUN QUERY COMMAND
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.query.createSqlFile", async () => {
      const { createSqlFile } = require('./queryLibrary/createSqlFile');
      await createSqlFile(context);
    }),
    vscode.commands.registerCommand("dp.query.renameBundle", async (uri?: vscode.Uri) => {
      const { renameQueryBundle } = require('./queryLibrary/renameQueryBundle');
      await renameQueryBundle(context, uri);
    }),
    vscode.commands.registerCommand("dp.query.openSaved", async (uri: vscode.Uri, connectionId?: string) => {
      const doc = await vscode.workspace.openTextDocument(uri);
      await vscode.window.showTextDocument(doc);
      if (connectionId) {
        // Set connection context
        await vscode.commands.executeCommand('dp.sql.setConnectionForDoc', doc.uri, connectionId);
      }
    }),
    vscode.commands.registerCommand("dp.query.openTablePreview", async (item?: ExplorerItem) => {
      await openTablePreviewAndRun(item);
    }),
    vscode.commands.registerCommand("dp.query.run", async () => {
      await runQuery(false);
    }),
    vscode.commands.registerCommand("dp.query.runNoLimit", async () => {
      await runQuery(true);
    }),
    vscode.commands.registerCommand("dp.query.runCurrentStatement", async () => {
      await runCurrentStatement();
    }),
    vscode.commands.registerCommand("dp.results.applyEdits", async (docUri: vscode.Uri, payload?: ApplyResultsetEditsCommandPayload) => {
      if (!docUri || !payload) {
        return;
      }

      const confirmed = payload.confirmed === true;
      const request: ApplyResultsetEditsRequest = {
        resultId: payload.resultId,
        source: payload.source,
        edits: payload.edits
      };

      const failure = (message: string): ApplyResultsetEditsResult => ({
        ok: false,
        summary: { applied: 0, conflicted: 0, failed: 1 },
        rowResults: [{ rowKey: {}, status: 'error', message }]
      });

      try {
        const lastResult = resultsViewProvider.getLastResult(docUri);
        if (!lastResult?.meta) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('No editable resultset is active.'));
          return;
        }

        if (lastResult.meta.resultId !== request.resultId) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('Resultset is stale. Re-run query and retry.'));
          return;
        }

        if (!lastResult.meta.editable.enabled || !lastResult.meta.source) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure(lastResult.meta.editable.reason || 'Resultset is read-only.'));
          return;
        }

        let doc: vscode.TextDocument;
        try {
          doc = await vscode.workspace.openTextDocument(docUri);
        } catch {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('Could not open source SQL document.'));
          return;
        }

        const docConnectionId = codeLensStore.get(doc);
        const activeConnId = getEffectiveConnectionId(docConnectionId);
        if (!activeConnId) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('No active connection found for this resultset.'));
          return;
        }

        const profiles: ConnectionProfile[] = await loadConnectionProfiles();
        const profile = profiles.find((p) => p.id === activeConnId);
        if (!profile) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('Connection profile was not found.'));
          return;
        }

        const adapter = getAdapter(profile.dialect);
        const dialect = resolveEffectiveSqlDialect(profile);
        const pkColumns = lastResult.meta.editable.primaryKeyColumns;
        const editableColumns = new Set(lastResult.meta.editable.editableColumns);
        const source = lastResult.meta.source;

        const rowResults: ApplyResultsetEditsResult['rowResults'] = [];
        let applied = 0;
        let conflicted = 0;
        let failed = 0;
        let attempted = 0;
        const stagedStatements: Array<{ rowEdit: ResultsetRowEdit; sql: string }> = [];

        for (const edit of request.edits || []) {
          const normalized = normalizeRowEdit(edit, editableColumns);
          if (!normalized) {
            continue;
          }
          attempted += 1;

          const missingPk = pkColumns.filter((pk) => !(pk in normalized.rowKey));
          if (missingPk.length > 0) {
            failed += 1;
            rowResults.push({
              rowKey: normalized.rowKey,
              status: 'error',
              message: `Missing primary key values: ${missingPk.join(', ')}`
            });
            continue;
          }

          const sql = buildUpdateStatement({
            dialect,
            source,
            rowEdit: normalized,
            primaryKeyColumns: pkColumns
          });

          stagedStatements.push({ rowEdit: normalized, sql });
        }

        if (attempted === 0) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('No editable changes were detected to save.'));
          return;
        }

        if (!confirmed && stagedStatements.length > 0) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsPreview', {
            request,
            connectionName: profile.name,
            targetLabel: formatSourceLabel(source),
            statements: stagedStatements.map((statement) => statement.sql)
          });
          return;
        }

        const { ensureConnectionSecrets } = require('./connections/connectionCommands');
        const secrets = await ensureConnectionSecrets(profile);
        if (!secrets) {
          resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', failure('Credentials were not provided.'));
          return;
        }

        for (const statement of stagedStatements) {
          try {
            const execResult = await executeNonQueryCompat(adapter, profile, secrets, statement.sql);
            if (execResult.affectedRows === 0) {
              conflicted += 1;
              rowResults.push({ rowKey: statement.rowEdit.rowKey, status: 'conflict', message: 'Row changed since it was loaded.' });
              continue;
            }
            applied += 1;
            rowResults.push({ rowKey: statement.rowEdit.rowKey, status: 'applied' });
          } catch (e: unknown) {
            failed += 1;
            rowResults.push({
              rowKey: statement.rowEdit.rowKey,
              status: 'error',
              message: e instanceof Error ? e.message : 'Failed to apply row update.'
            });
          }
        }

        const response: ApplyResultsetEditsResult = {
          ok: conflicted === 0 && failed === 0,
          summary: { applied, conflicted, failed },
          rowResults
        };
        resultsViewProvider.postMessage(docUri, 'applyResultsetEditsResult', response);

        if (applied > 0) {
          try {
            const runCtx = lastRunContextByDocUri.get(docUri.toString());
            const previewCtx = tablePreviewContextByDocUri.get(docUri.toString());
            const refreshSql = runCtx?.refreshSql || previewCtx?.sql;
            const userSql = runCtx?.userSql || refreshSql;
            if (refreshSql && userSql) {
              const refreshed = await adapter.runQuery(profile, secrets, refreshSql, { maxRows: 0 });
              refreshed.meta = await buildResultMeta(profile, docUri, userSql, refreshed.columns);
              resultsViewProvider.postMessage(docUri, 'updateResults', refreshed);
            }
          } catch (e: unknown) {
            const message = e instanceof Error ? e.message : 'unknown error';
            vscode.window.showWarningMessage(`Edits applied, but result refresh failed: ${message}`);
          }
        }
      } catch (e: unknown) {
        const message = e instanceof Error ? e.message : 'Unexpected error while applying edits.';
        resultsViewProvider.postMessage(
          docUri,
          'applyResultsetEditsResult',
          failure(message)
        );
      }
    })
  );

  // Helper function for query execution
  async function runQuery(bypassLimit: boolean) {
    const editor = vscode.window.activeTextEditor;
    if (!editor) return;

    // 1. Get SQL
    const selection = editor.selection;
    const text = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);
    if (!text.trim()) {
      vscode.window.showWarningMessage("No SQL to run.");
      return;
    }

    // 2. Get Connection for this Doc (CodeLens override)
    const docConnectionId = codeLensStore.get(editor.document);

    // Use the same effective ID logic as the label
    const activeConnId = getEffectiveConnectionId(docConnectionId);

    if (!activeConnId) {
      const choice = await vscode.window.showErrorMessage("No connections available. Add one first.", "Add Connection");
      if (choice === "Add Connection") {
        vscode.commands.executeCommand("dp.connection.add");
      }
      return;
    }

    // Load profile
    const { loadConnectionProfiles } = require('./connections/connectionStore');
    const profiles: ConnectionProfile[] = await loadConnectionProfiles();
    const profile = profiles.find((p: ConnectionProfile) => p.id === activeConnId);

    if (!profile) {
      vscode.window.showErrorMessage("Connection not found (maybe deleted?). Select another.");
      return;
    }

    // 3. Show Results Panel (loading state?)
    const docUri = editor.document.uri;
    resultsViewProvider.show(docUri);

    // 4. Run Query (with interaction feedback)
    const { splitStatements } = require('./core/sqlSplitter');
    const statements = splitStatements(text);
    const isScriptMode = statements.length > 1;

    vscode.window.withProgress({
      location: vscode.ProgressLocation.Notification,
      title: isScriptMode
        ? `Running script (${statements.length} statements) on ${profile.name}...`
        : `Running query on ${profile.name}...`,
      cancellable: true
    }, async (_progress, _token) => {
      try {
        const { ensureConnectionSecrets } = require('./connections/connectionCommands');
        const secrets = await ensureConnectionSecrets(profile);
        if (!secrets) return; // User cancelled

        const adapter = getAdapter(profile.dialect);
        const config = vscode.workspace.getConfiguration('dp');
        const maxRowsLimit = bypassLimit ? 0 : config.get<number>('query.maxRowsLimit', 10000);

        if (isScriptMode) {
          // ── Script mode: execute statements sequentially ──
          const { executeScript } = require('./core/scriptRunner');
          const scriptResult: ScriptExecutionResult = await executeScript(statements, adapter, profile, secrets, {
            maxRows: maxRowsLimit,
            bypassLimit,
          });

          lastRunContextByDocUri.set(docUri.toString(), {
            refreshSql: text,
            userSql: text
          });

          // Update panel with script results
          resultsViewProvider.show(docUri);
          resultsViewProvider.postMessage(docUri, 'setAllowCsvExport', profile.allowCsvExport ?? true);
          resultsViewProvider.postMessage(docUri, 'updateScriptResults', scriptResult);

          // Notification for failures
          if (scriptResult.failedAtIndex) {
            const failedStmt = scriptResult.statements.find(s => s.status === 'error');
            vscode.window.showErrorMessage(
              `Statement ${scriptResult.failedAtIndex} failed: ${failedStmt?.errorMessage || 'Unknown error'}`
            );
          }

          // Update Last Run
          await queryIndex.updateLastRun(docUri);

          // History
          const { HistoryService } = require('./services/historyService');
          let schemaName = profile.database;
          if (!schemaName) {
            const schemaMatch = text.match(/(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\./i);
            schemaName = schemaMatch ? schemaMatch[1] : context.workspaceState.get<string>("dp.activeSchemaName") || 'main';
          }
          HistoryService.getInstance().addEntry({
            query: text,
            connectionName: profile.name,
            schemaName,
            connectionId: profile.id,
            status: scriptResult.failedAtIndex ? 'error' : 'success',
            rows: scriptResult.lastTabularResult?.rows?.length,
            duration: scriptResult.statements.reduce((sum, s) => sum + (s.elapsedMs || 0), 0)
          });

          // DDL Auto-Refresh — check all executed statements
          const anyDDL = scriptResult.statements.some(
            s => s.status === 'success' && checkForDDL(s.sql)
          );
          if (anyDDL) {
            vscode.commands.executeCommand("dp.view.refreshSchemas");
            vscode.commands.executeCommand("dp.view.refreshLocalDuckDB");
          }
        } else {
          // ── Single statement mode (unchanged) ──
          const { applyRowLimit } = require('./core/sqlLimitHelper');
          const limitResult = applyRowLimit(text, maxRowsLimit);
          lastRunContextByDocUri.set(docUri.toString(), {
            refreshSql: limitResult.sql,
            userSql: text
          });

          const results = await adapter.runQuery(profile, secrets, limitResult.sql, { maxRows: limitResult.effectiveLimit });
          results.meta = await buildResultMeta(profile, docUri, text, results.columns);

          // Show notice if user's limit was clamped
          if (limitResult.clamped) {
            vscode.window.showInformationMessage(
              `Query LIMIT was capped to ${maxRowsLimit} rows. Use "Run (no LIMIT)" to bypass.`
            );
          }

          // Update panel
          resultsViewProvider.show(docUri);
          resultsViewProvider.postMessage(docUri, 'setAllowCsvExport', profile.allowCsvExport ?? true);
          resultsViewProvider.postMessage(docUri, 'updateResults', results);

          // Update Last Run
          await queryIndex.updateLastRun(docUri);

          // MEMORY RECALL: Save to history
          const { HistoryService } = require('./services/historyService');
          let schemaName = profile.database;
          if (!schemaName) {
            const schemaMatch = text.match(/(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)\./i);
            schemaName = schemaMatch ? schemaMatch[1] : context.workspaceState.get<string>("dp.activeSchemaName") || 'main';
          }

          HistoryService.getInstance().addEntry({
            query: text,
            connectionName: profile.name,
            schemaName,
            connectionId: profile.id,
            status: 'success',
            rows: results.rows?.length,
            duration: results.elapsedMs
          });

          // DDL Auto-Refresh
          if (checkForDDL(text)) {
            vscode.commands.executeCommand("dp.view.refreshSchemas");
            vscode.commands.executeCommand("dp.view.refreshLocalDuckDB");
          }
        }
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Query failed: ${msg}`);
      }
    });
  }

  // Run Current Statement — executes only the statement under the cursor
  async function runCurrentStatement() {
    const editor = vscode.window.activeTextEditor;
    if (!editor) return;

    const fullText = editor.document.getText();
    if (!fullText.trim()) {
      vscode.window.showWarningMessage("No SQL to run.");
      return;
    }

    const { findStatementAtOffset } = require('./core/sqlSplitter');
    const cursorOffset = editor.document.offsetAt(editor.selection.active);
    const stmt = findStatementAtOffset(fullText, cursorOffset);

    if (!stmt) {
      vscode.window.showWarningMessage("No statement found at cursor position.");
      return;
    }

    // Select the found statement in the editor for visual feedback
    const startPos = editor.document.positionAt(stmt.startOffset);
    const endPos = editor.document.positionAt(stmt.endOffset);
    editor.selection = new vscode.Selection(startPos, endPos);

    // Execute as single statement via runQuery — the selection will be picked up
    await runQuery(false);
  }

  async function openTablePreviewAndRun(item?: ExplorerItem) {
    if (!item) {
      vscode.window.showWarningMessage("No table/view item selected.");
      return;
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const itemAny = item as any;
    const tableName: string | undefined = item.table?.name || itemAny.tableName;
    const schemaName: string | undefined = item.schemaName;

    if (!tableName) {
      vscode.window.showWarningMessage("Could not determine the selected table/view.");
      return;
    }

    let connectionId: string | undefined = item.connectionId || item.introspection?.connectionId;

    // Local Data Work panel items don't carry a connectionId; they always target the managed local DB.
    if (!connectionId && itemAny.tableName) {
      connectionId = 'duck-piper-local-data-work';
    }

    if (!connectionId) {
      connectionId = context.workspaceState.get<string>("dp.activeConnectionId");
    }

    if (!connectionId) {
      vscode.window.showErrorMessage("No connection found for this table/view.");
      return;
    }

    const { getConnection } = require('./connections/connectionStore');
    const profile = await getConnection(connectionId);

    if (!profile) {
      vscode.window.showErrorMessage(`Connection not found for selected item (${connectionId}).`);
      return;
    }

    const effectiveDialect = resolveEffectiveSqlDialect(profile) || item.introspection?.dialect || 'duckdb';
    const tableFqn = buildTableFqnForPreview(schemaName, tableName, effectiveDialect);
    const sql = `SELECT * FROM ${tableFqn} LIMIT 100;`;

    const doc = await vscode.workspace.openTextDocument({ content: sql, language: 'sql' });
    const primaryKeyColumns: string[] = Array.isArray(item.table?.primaryKey) ? item.table.primaryKey : [];
    const editableColumns: string[] = Array.isArray(item.table?.columns)
      ? item.table.columns.map((c: ColumnModel) => c.name)
      : [];
    tablePreviewContextByDocUri.set(doc.uri.toString(), {
      sql,
      source: {
        schema: schemaName,
        table: tableName
      },
      primaryKeyColumns,
      editableColumns
    });

    // Pre-set the connection in the CodeLens store BEFORE showing the document.
    // This prevents onDidChangeActiveTextEditor from overwriting it with the
    // global active connection before setConnectionForDoc has a chance to run.
    await codeLensStore.set(doc, connectionId);
    await vscode.window.showTextDocument(doc, { preview: false });
    await vscode.commands.executeCommand('dp.sql.setConnectionForDoc', doc.uri, connectionId);
    await vscode.commands.executeCommand('dp.query.runNoLimit');
  }

  const isLocalSchemaTreeItem = (item: vscode.TreeItem | undefined): boolean =>
    typeof item?.contextValue === 'string' && item.contextValue.startsWith('dp.duckdb.schema');

  const resolveCreateTableTarget = async (item?: ExplorerItem): Promise<CreateTablePanelContext | null> => {
    const schemaName = typeof item?.schemaName === 'string'
      ? item.schemaName
      : (typeof item?.schemaModel?.name === 'string' ? item.schemaModel.name : undefined);
    if (!schemaName) {
      vscode.window.showErrorMessage('Select a schema node to create a table.');
      return null;
    }

    const fromLocalTree = isLocalSchemaTreeItem(item);
    let connectionId = typeof item?.connectionId === 'string' ? item.connectionId : undefined;
    if (!connectionId && typeof item?.introspection?.connectionId === 'string') {
      connectionId = item.introspection.connectionId;
    }
    if (!connectionId && fromLocalTree) {
      connectionId = 'duck-piper-local-data-work';
    }
    if (!connectionId) {
      connectionId = context.workspaceState.get<string>('dp.activeConnectionId');
    }
    if (!connectionId) {
      vscode.window.showErrorMessage('Could not resolve connection for selected schema.');
      return null;
    }

    const { getConnection } = require('./connections/connectionStore');
    const profile = await getConnection(connectionId) as ConnectionProfile | undefined;
    if (!profile) {
      vscode.window.showErrorMessage(`Connection not found (${connectionId}).`);
      return null;
    }

    const isLocalDuckDB = fromLocalTree || profile.id === 'duck-piper-local-data-work' || profile.isLocalDuckPiperCacheDb === true;
    const dialect = isLocalDuckDB
      ? 'duckdb'
      : (resolveEffectiveSqlDialect(profile) || profile.dialect || 'duckdb');

    return {
      connectionId: profile.id,
      connectionName: profile.name,
      schemaName,
      dialect,
      isLocalDuckDB
    };
  };

  const previewCreateTable = (target: CreateTablePanelContext, draft: CreateTableDraft) => {
    const buildResult = buildCreateTableSql({
      dialect: target.dialect as DbDialect,
      schemaName: target.schemaName,
      draft
    });
    return {
      connectionName: target.connectionName,
      targetLabel: buildResult.targetLabel,
      statements: buildResult.statements
    };
  };

  const executeCreateTable = async (
    target: CreateTablePanelContext,
    draft: CreateTableDraft
  ): Promise<CreateTableResultPayload> => {
    const { getConnection } = require('./connections/connectionStore');
    const profile = await getConnection(target.connectionId) as ConnectionProfile | undefined;
    if (!profile) {
      return { ok: false, message: `Connection not found (${target.connectionId}).` };
    }

    const effectiveDialect = (target.isLocalDuckDB ? 'duckdb' : (resolveEffectiveSqlDialect(profile) || target.dialect)) as DbDialect;
    const sqlBatch = buildCreateTableSql({
      dialect: effectiveDialect,
      schemaName: target.schemaName,
      draft
    });

    if (target.isLocalDuckDB) {
      const { LocalDuckDB } = require('./transform/localDuckDB');
      const { DuckDBAdapter } = require('./connections/adapters/duckdb');

      for (const statement of sqlBatch.statements) {
        await LocalDuckDB.getInstance().runUpdate(statement);
      }

      DuckDBAdapter.closeConnection(profile.id);
      localDuckDBProvider.refresh();
      explorerProvider.refresh();

      return {
        ok: true,
        message: `Created ${sqlBatch.targetLabel} using ${sqlBatch.statements.length} statement${sqlBatch.statements.length === 1 ? '' : 's'}.`
      };
    }

    const { ensureConnectionSecrets, performIntrospection } = require('./connections/connectionCommands');
    const secrets = await ensureConnectionSecrets(profile) as ConnectionSecrets | undefined;
    if (!secrets) {
      return { ok: false, message: 'Credentials were not provided.' };
    }

    const adapter = getAdapter(profile.dialect);
    for (const statement of sqlBatch.statements) {
      await executeNonQueryCompat(adapter, profile, secrets, statement);
    }

    await performIntrospection(profile, true);
    explorerProvider.refresh();

    return {
      ok: true,
      message: `Created ${sqlBatch.targetLabel} using ${sqlBatch.statements.length} statement${sqlBatch.statements.length === 1 ? '' : 's'}.`
    };
  };

  const isLocalTableTreeItem = (item: vscode.TreeItem | undefined): boolean =>
    typeof item?.contextValue === 'string' && item.contextValue.startsWith('dp.duckdb.table');

  const resolveEditTableTarget = async (item?: ExplorerItem): Promise<CreateTablePanelContext | null> => {
    // Table name: from ExplorerItem (item.table.name) or LocalDuckDBView TableItem (item.tableName)
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const itemAny = item as any;
    const tableName = typeof item?.table?.name === 'string'
      ? item.table.name
      : (typeof itemAny?.tableName === 'string' ? itemAny.tableName : undefined);
    const schemaName = typeof item?.schemaName === 'string' ? item.schemaName : undefined;

    if (!tableName || !schemaName) {
      vscode.window.showErrorMessage('Select a table node to edit.');
      return null;
    }

    const fromLocalTree = isLocalTableTreeItem(item);
    let connectionId = typeof item?.connectionId === 'string' ? item.connectionId : undefined;
    if (!connectionId && typeof item?.introspection?.connectionId === 'string') {
      connectionId = item.introspection.connectionId;
    }
    if (!connectionId && fromLocalTree) {
      connectionId = 'duck-piper-local-data-work';
    }
    if (!connectionId) {
      connectionId = context.workspaceState.get<string>('dp.activeConnectionId');
    }
    if (!connectionId) {
      vscode.window.showErrorMessage('Could not resolve connection for selected table.');
      return null;
    }

    const { getConnection } = require('./connections/connectionStore');
    const profile = await getConnection(connectionId) as ConnectionProfile | undefined;
    if (!profile) {
      vscode.window.showErrorMessage(`Connection not found (${connectionId}).`);
      return null;
    }

    const isLocalDuckDB = fromLocalTree || profile.id === 'duck-piper-local-data-work' || profile.isLocalDuckPiperCacheDb === true;
    const dialect = isLocalDuckDB
      ? 'duckdb'
      : (resolveEffectiveSqlDialect(profile) || profile.dialect || 'duckdb');

    // Get table model - from ExplorerItem directly, or query local DuckDB
    let tableModel = item?.table;

    if (!tableModel && fromLocalTree) {
      // For localDuckDBView items, query the metadata
      try {
        const { LocalDuckDB } = require('./transform/localDuckDB');
        const db = LocalDuckDB.getInstance();
        const colResult = await db.runQuery(
          `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_schema = '${schemaName}' AND table_name = '${tableName}' ORDER BY ordinal_position`
        );
        const columns = (colResult?.rows ?? []).map((row: Record<string, unknown>) => ({
          name: String(row.column_name),
          type: String(row.data_type),
          nullable: row.is_nullable === 'YES'
        }));

        tableModel = { name: tableName, columns, primaryKey: [], foreignKeys: [] };
      } catch {
        vscode.window.showErrorMessage('Failed to load table metadata from local DuckDB.');
        return null;
      }
    }

    if (!tableModel || !tableModel.columns) {
      vscode.window.showErrorMessage('Table metadata not available. Try refreshing the explorer.');
      return null;
    }

    return {
      connectionId: profile.id,
      connectionName: profile.name,
      schemaName,
      dialect,
      isLocalDuckDB,
      editMode: {
        tableName: tableModel.name,
        columns: (tableModel.columns || []).map((c: ColumnModel) => ({
          name: c.name,
          type: c.type,
          nullable: c.nullable,
          comment: c.comment
        })),
        primaryKey: tableModel.primaryKey,
        foreignKeys: tableModel.foreignKeys,
        indexes: tableModel.indexes
      }
    };
  };

  const previewAlterTable = (target: CreateTablePanelContext, original: CreateTableDraft, current: CreateTableDraft) => {
    const buildResult = buildAlterTableSql({
      dialect: target.dialect as DbDialect,
      schemaName: target.schemaName,
      tableName: target.editMode!.tableName,
      original,
      current
    });
    return {
      connectionName: target.connectionName,
      targetLabel: buildResult.targetLabel,
      statements: buildResult.statements
    };
  };

  const executeAlterTable = async (
    target: CreateTablePanelContext,
    original: CreateTableDraft,
    current: CreateTableDraft
  ): Promise<CreateTableResultPayload> => {
    const { getConnection } = require('./connections/connectionStore');
    const profile = await getConnection(target.connectionId) as ConnectionProfile | undefined;
    if (!profile) {
      return { ok: false, message: `Connection not found (${target.connectionId}).` };
    }

    const effectiveDialect = (target.isLocalDuckDB ? 'duckdb' : (resolveEffectiveSqlDialect(profile) || target.dialect)) as DbDialect;
    const sqlBatch = buildAlterTableSql({
      dialect: effectiveDialect,
      schemaName: target.schemaName,
      tableName: target.editMode!.tableName,
      original,
      current
    });

    if (target.isLocalDuckDB) {
      const { LocalDuckDB } = require('./transform/localDuckDB');
      const { DuckDBAdapter } = require('./connections/adapters/duckdb');

      for (const statement of sqlBatch.statements) {
        await LocalDuckDB.getInstance().runUpdate(statement);
      }

      DuckDBAdapter.closeConnection(profile.id);
      localDuckDBProvider.refresh();
      explorerProvider.refresh();

      return {
        ok: true,
        message: `Altered ${sqlBatch.targetLabel} using ${sqlBatch.statements.length} statement${sqlBatch.statements.length === 1 ? '' : 's'}.`
      };
    }

    const { ensureConnectionSecrets, performIntrospection } = require('./connections/connectionCommands');
    const secrets = await ensureConnectionSecrets(profile) as ConnectionSecrets | undefined;
    if (!secrets) {
      return { ok: false, message: 'Credentials were not provided.' };
    }

    const adapter = getAdapter(profile.dialect);
    for (const statement of sqlBatch.statements) {
      await executeNonQueryCompat(adapter, profile, secrets, statement);
    }

    await performIntrospection(profile, true);
    explorerProvider.refresh();

    return {
      ok: true,
      message: `Altered ${sqlBatch.targetLabel} using ${sqlBatch.statements.length} statement${sqlBatch.statements.length === 1 ? '' : 's'}.`
    };
  };

  const dropTable = async (target: CreateTablePanelContext): Promise<CreateTableResultPayload> => {
    const { getConnection } = require('./connections/connectionStore');
    const profile = await getConnection(target.connectionId) as ConnectionProfile | undefined;
    if (!profile) {
      return { ok: false, message: `Connection not found (${target.connectionId}).` };
    }

    const effectiveDialect = (target.isLocalDuckDB ? 'duckdb' : (resolveEffectiveSqlDialect(profile) || target.dialect)) as DbDialect;
    const sqlBatch = buildDropTableSql({
      dialect: effectiveDialect,
      schemaName: target.schemaName,
      tableName: target.editMode!.tableName
    });

    if (target.isLocalDuckDB) {
      const { LocalDuckDB } = require('./transform/localDuckDB');
      const { DuckDBAdapter } = require('./connections/adapters/duckdb');

      for (const statement of sqlBatch.statements) {
        await LocalDuckDB.getInstance().runUpdate(statement);
      }

      DuckDBAdapter.closeConnection(profile.id);
      localDuckDBProvider.refresh();
      explorerProvider.refresh();

      return {
        ok: true,
        message: `Dropped ${sqlBatch.targetLabel}.`
      };
    }

    const { ensureConnectionSecrets, performIntrospection } = require('./connections/connectionCommands');
    const secrets = await ensureConnectionSecrets(profile) as ConnectionSecrets | undefined;
    if (!secrets) {
      return { ok: false, message: 'Credentials were not provided.' };
    }

    const adapter = getAdapter(profile.dialect);
    for (const statement of sqlBatch.statements) {
      await executeNonQueryCompat(adapter, profile, secrets, statement);
    }

    await performIntrospection(profile, true);
    explorerProvider.refresh();

    return {
      ok: true,
      message: `Dropped ${sqlBatch.targetLabel}.`
    };
  };

  // Active schema selection (NEW)
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.schema.createTable", async (item?: ExplorerItem) => {
      const target = await resolveCreateTableTarget(item);
      if (!target) {
        return;
      }

      CreateTableView.render(context.extensionUri, target, {
        onPreview: async (draft) => previewCreateTable(target, draft),
        onExecute: async (draft) => executeCreateTable(target, draft)
      });
    }),
    vscode.commands.registerCommand("dp.schema.editTable", async (item?: ExplorerItem) => {
      const target = await resolveEditTableTarget(item);
      if (!target) {
        return;
      }

      CreateTableView.render(context.extensionUri, target, {
        onPreview: async (draft) => previewCreateTable(target, draft),
        onExecute: async (draft) => executeCreateTable(target, draft),
        onPreviewAlter: async (original, current) => previewAlterTable(target, original, current),
        onExecuteAlter: async (original, current) => executeAlterTable(target, original, current),
        onDropTable: async () => dropTable(target)
      });
    }),
    vscode.commands.registerCommand("dp.schema.select", async (schemaName: string) => {
      await context.workspaceState.update("dp.activeSchemaName", schemaName);
      await setHasActiveSchema(!!schemaName);
      void vscode.window.setStatusBarMessage(`DuckPiper: selected schema "${schemaName}"`, 2500);
      explorerProvider.refresh();
    })
  );

  // Watchers to refresh the views when JSON files change
  const watchers = registerDPWatchers(
    () => {
      explorerProvider.refresh();
      updateConnectionCache();
    },
    () => explorerProvider.refresh(),
    () => savedQueriesProvider.refresh()
  );
  context.subscriptions.push(watchers);

  // Bundle Rename Watcher

  context.subscriptions.push(
    vscode.workspace.onDidDeleteFiles(async (e) => {
      const { handleDeletions } = require('./queryLibrary/deleteBundleWatcher');
      await handleDeletions(e.files);
    }),
    vscode.workspace.onDidCreateFiles(async (e) => {
      const { handleCreations } = require('./queryLibrary/createBundleWatcher');
      await handleCreations(e.files);
    })
  );

  // -----------------------------
  // Query Logic
  // -----------------------------



  // Show Similar Query Command
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.query.findSimilarSavedQueries", async (hash?: string) => {
      const { queryIndex } = require('./queryLibrary/queryIndex');
      // If hash is not provided (e.g. invoked from palette), get it from active editor
      if (!hash) {
        const editor = vscode.window.activeTextEditor;
        if (!editor) return;
        const { canonicalizeSql } = require('./core/hashing');
        hash = canonicalizeSql(editor.document.getText()).sqlHash;
      }

      const matches = queryIndex.getMatches(hash);
      if (matches.length === 0) return;

      const items = matches.map((m: { path: string; title?: string }) => {
        const root = vscode.workspace.workspaceFolders?.[0].uri;
        if (!root) return undefined;
        const uri = vscode.Uri.joinPath(root, m.path);

        return {
          label: m.path,
          description: m.title || uri.fsPath,
          uri: uri
        };
      }).filter((item: { label: string; description: string; uri: vscode.Uri } | undefined): item is { label: string; description: string; uri: vscode.Uri } => item !== undefined);

      const picked = await vscode.window.showQuickPick(items, {
        placeHolder: `Select a similar query (${matches.length} found)`
      });

      if (picked) {
        const doc = await vscode.workspace.openTextDocument((picked as unknown as { uri: vscode.Uri }).uri);
        await vscode.window.showTextDocument(doc);
      }
    })
  );

  // -----------------------------
  // Query index (existing)
  // -----------------------------
  // queryIndex.initialize() is already called above

  // Helper to update similar queries context
  const updateSimilarQueriesContext = async (editor: vscode.TextEditor | undefined) => {
    if (!editor) {
      await setHasSimilarQueries(false);
      return;
    }
    if (!isSqlDoc(editor.document)) {
      await setHasSimilarQueries(false);
      return;
    }

    const { sqlHash } = canonicalizeSql(editor.document.getText());
    const matches = queryIndex.getMatches(sqlHash);
    const currentPath = vscode.workspace.asRelativePath(editor.document.uri, false);
    const others = matches.filter((m: { path: string }) => m.path !== currentPath);
    await setHasSimilarQueries(others.length > 0);
  };

  // -----------------------------
  // Comment overlays (existing)
  // -----------------------------

  // Re-render overlays when active editor changes
  context.subscriptions.push(
    vscode.window.onDidChangeActiveTextEditor(async (editor) => {
      await refreshProductionWarningBar();

      // Update similar queries context
      await updateSimilarQueriesContext(editor);

      if (!editor) return;
      if (!isSqlDoc(editor.document)) return;

      // Auto-restore connection for the document
      const entry = queryIndex.getEntry(editor.document.uri);
      if (entry && entry.connectionId) {
        // Document has a previously stored connection - verify it still exists
        const { getConnection } = require('./connections/connectionStore');
        const exists = await getConnection(entry.connectionId);
        if (exists) {
          // Connection still exists - restore it to the CodeLens store (no change to queryIndex)
          codeLensStore.set(editor.document, entry.connectionId);
          codeLensProvider.refresh();
        } else {
          // Stored connection no longer exists - fallback to active connection
          const active = context.workspaceState.get<string>("dp.activeConnectionId");
          if (active) await vscode.commands.executeCommand('dp.sql.setConnectionForDoc', editor.document.uri, active);
        }
      } else {
        // New/untracked file - only set connection if not already tracked in CodeLens store
        const currentDocConnection = codeLensStore.get(editor.document);
        if (!currentDocConnection) {
          const active = context.workspaceState.get<string>("dp.activeConnectionId");
          if (active) await vscode.commands.executeCommand('dp.sql.setConnectionForDoc', editor.document.uri, active);
        }
      }

      // Switch Results View to this doc just in case the panel is visible,
      // so we don't show stale results from potential previous run of another file.
      // (Assuming provider implementation of show(uri) handles updating existing view)
      resultsViewProvider.show(editor.document.uri);
    })
  );

  // Re-render overlays when a SQL doc changes (light debounce recommended)
  context.subscriptions.push(
    vscode.workspace.onDidChangeTextDocument(async (evt) => {
      const editor = vscode.window.activeTextEditor;
      if (!editor) return;
      if (evt.document.uri.toString() !== editor.document.uri.toString()) return;
      if (!isSqlDoc(evt.document)) return;

      await updateSimilarQueriesContext(editor);
    })
  );

  context.subscriptions.push(
    vscode.workspace.onDidCloseTextDocument((doc) => {
      tablePreviewContextByDocUri.delete(doc.uri.toString());
      lastRunContextByDocUri.delete(doc.uri.toString());
    })
  );

  // Command: add inline comments (overlay). v0 uses heuristic comments; replace with AI later.
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.query.addInlineComments", async () => {
      const editor = vscode.window.activeTextEditor;
      if (!editor || !isSqlDoc(editor.document)) return;

      const { generateAndStreamInlineComments } = require('./ai/inlineComments');
      try {
        await generateAndStreamInlineComments(context, editor);
      } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Inline comments failed: ${msg}`);
      }
    })
  );

  // Initial render if editor already open
  if (vscode.window.activeTextEditor?.document && isSqlDoc(vscode.window.activeTextEditor.document)) {
    await updateSimilarQueriesContext(vscode.window.activeTextEditor);
  }

  // Initial refresh of tree views
  explorerProvider.refresh();
  savedQueriesProvider.refresh();

  // Completion Provider
  const completionProvider = new DPCompletionProvider((doc) => {
    const docId = codeLensStore.get(doc);
    return getEffectiveConnectionId(docId);
  });
  context.subscriptions.push(
    vscode.languages.registerCompletionItemProvider(
      [{ language: 'sql' }, { language: 'postgres' }],
      completionProvider,
      '.', ' '
    )
  );

  // AI Commands
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.query.generateMarkdownDoc", async () => {
      const { generateMarkdownDoc } = require('./ai/docGenerator');
      await generateMarkdownDoc(context);
    }),
    vscode.commands.registerCommand("dp.query.openMarkdownDoc", async () => {
      const { openMarkdownDoc } = require('./ai/docGenerator');
      await openMarkdownDoc(context);
    }),
    vscode.commands.registerCommand("dp.chart.createFromResults", async (uri?: vscode.Uri) => {
      const { generateChart } = require('./ai/chartgen');
      await generateChart(context, uri);
    }),
    vscode.commands.registerCommand("dp.ai.selectModel", async () => {
      const { selectAIModel } = require('./ai/aiService');
      await selectAIModel();
    })
  );

  registerDuckDBCommands(context);

  context.subscriptions.push(
    vscode.commands.registerCommand('dp.system.saveLoadedResultsToDuckDB', async (results?: unknown) => {
      // If coming from webview message or keybinding, ensure we pass it
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await saveLoadedResultsToDuckDB(context, results as any);
    }),
    vscode.commands.registerCommand('dp.system.saveTableDataToDuckDB', async (item?: ExplorerItem) => {
      await saveTableToDuckDB(context, item);
    })
  );

  // Lifecycle Commands (Delete, Export, Reset)
  const { DuckDBLifecycleManager } = require('./transform/duckdbLifecycle');
  const lifecycle = DuckDBLifecycleManager.getInstance();

  context.subscriptions.push(
    vscode.commands.registerCommand('dp.duckdb.deleteObject', async (item?: { schemaName?: string; tableName?: string }) => {
      // item is TreeItem from LocalDuckDBView with schemaName and tableName public properties.
      if (item && item.schemaName && item.tableName) {
        await lifecycle.deleteObject(item.schemaName, item.tableName);
      }
    }),
    // Re-map existing generic commands if needed, or register new specifics
    vscode.commands.registerCommand('dp.duckdb.exportTableToCsv', async (item?: { schemaName?: string; tableName?: string }) => {
      if (item && item.schemaName && item.tableName) {
        await lifecycle.exportToCsv(item.schemaName, item.tableName);
      }
    }),
    vscode.commands.registerCommand('dp.duckdb.resetLayer', async (item?: { schemaName?: string }) => {
      if (item && item.schemaName) {
        await lifecycle.resetLayer(item.schemaName);
      }
    }),
    vscode.commands.registerCommand('dp.pipeline.resetOutputs', async (item?: { pipelineId?: string } | string) => {
      // item from pipelines view with pipelineId property.
      const pid = (typeof item === 'object' ? item?.pipelineId : undefined) || (typeof item === 'string' ? item : undefined);
      if (pid) {
        await lifecycle.resetPipelineOutputs(pid);
      }
    })
  );

  // Register Notebook Commands
  const { registerNotebookCommands } = require('./transform/notebookCommands');
  registerNotebookCommands(context);



  // Register Native Notebook Serializer (Epic 13)
  const { DPNotebookSerializer } = require('./transform/notebook/serializer');
  context.subscriptions.push(
    vscode.workspace.registerNotebookSerializer('dp-notebook', new DPNotebookSerializer())
  );

  // Register Native Notebook Controller
  const { DPNotebookController } = require('./transform/notebook/controller');
  context.subscriptions.push(new DPNotebookController());

  // Register Lifecycle Manager (Epic 16.4)
  const { NotebookLifecycleManager } = require('./transform/notebook/lifecycleManager');
  new NotebookLifecycleManager(context);

  // Register Cell Status Bar Provider (Epic 16)
  const { DPCellStatusBarProvider } = require('./transform/notebook/cellStatusBar');
  context.subscriptions.push(
    vscode.notebooks.registerNotebookCellStatusBarItemProvider(
      'dp-notebook',
      new DPCellStatusBarProvider()
    )
  );


  // Pipelines Provider
  const { PipelinesViewProvider } = require('./pipelines/pipelinesView');
  const pipelinesProvider = new PipelinesViewProvider();

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("dp.pipelinesView", pipelinesProvider)
  );

  // Commands: Open Lineage
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.pipeline.openLineage", async (arg?: string | { pipelineId?: string; resourceUri?: vscode.Uri }) => {
      let pipelineId = typeof arg === 'string' ? arg : arg?.pipelineId;

      // If no pipelineId, check if it's a NotebookItem from Sidebar (has resourceUri)
      if (!pipelineId && typeof arg === 'object' && arg?.resourceUri) {
        const { PipelineRepository } = require('./pipelines/pipelineRepository');
        const repo = PipelineRepository.getInstance();
        const pipeline = await repo.findPipelineByUri(arg.resourceUri);
        if (pipeline) {
          pipelineId = pipeline.pipelineId;
        } else {
          // Fallback: try to see if we can derive or create one? 
          // For now, imply it might not exist in index yet.
        }
      }

      if (!pipelineId) {
        vscode.window.showErrorMessage("No pipeline found for this notebook. Run the notebook to generate lineage.");
        return;
      }
      await lineageViewProvider.showLineage(pipelineId);
    }),
    vscode.commands.registerCommand("dp.pipelinesView.refresh", () => pipelinesProvider.refresh())
  );

  // Pipeline File Watcher (Deletions + Creations)
  const pipelineWatcher = vscode.workspace.createFileSystemWatcher('**/*.dpnb');
  pipelineWatcher.onDidDelete(async (uri) => {
    const { PipelineRepository } = require('./pipelines/pipelineRepository');
    await PipelineRepository.getInstance().deletePipeline(uri);
    pipelinesProvider.refresh();
  });
  pipelineWatcher.onDidCreate(async (uri) => {
    const { PipelineRepository } = require('./pipelines/pipelineRepository');
    await PipelineRepository.getInstance().addPipeline(uri);
    pipelinesProvider.refresh();
  });
  context.subscriptions.push(pipelineWatcher);

  // Helper: Rename Watcher for Notebooks
  context.subscriptions.push(
    vscode.workspace.onDidRenameFiles(async (e) => {
      // 1. Saved Query Bundles (V1)
      const { handleRenames } = require('./queryLibrary/renameBundleWatcher');
      await handleRenames(e.files);

      // 2. Notebook Pipelines (V2)
      const { PipelineRepository } = require('./pipelines/pipelineRepository');
      const repo = PipelineRepository.getInstance();
      for (const file of e.files) {
        if (file.oldUri.path.endsWith('.dpnb')) {
          await repo.renameNotebook(file.oldUri, file.newUri);
        }
      }
    })
  );



  // -----------------------------
  // Pipelines Sidebar (NEW)
  // -----------------------------
  const { PipelinesSidebarProvider } = require('./pipelines/pipelinesSidebar');
  const pipelinesSidebarProvider = new PipelinesSidebarProvider();

  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("dp.pipelinesSidebarView", pipelinesSidebarProvider),
    vscode.commands.registerCommand("dp.pipelinesSidebar.refresh", () => pipelinesSidebarProvider.refresh()),
    vscode.commands.registerCommand("dp.notebook.delete", async (item?: { resourceUri?: vscode.Uri; context?: { uri?: vscode.Uri } }) => {
      // item is NotebookItem { context: { uri: ... } } from sidebar, or undefined from toolbar
      let uri = item?.resourceUri;
      if (!uri && item?.context?.uri) uri = item.context.uri;

      // If no item passed (toolbar invocation), get active notebook editor
      if (!uri) {
        const activeNotebook = vscode.window.activeNotebookEditor?.notebook;
        if (activeNotebook && activeNotebook.notebookType === 'dp-notebook') {
          uri = activeNotebook.uri;
        }
      }

      if (!uri) {
        vscode.window.showErrorMessage("No notebook selected to delete.");
        return;
      }

      const confirm = await vscode.window.showWarningMessage(
        `Are you sure you want to delete ${path.basename(uri.fsPath)}?`,
        { modal: true },
        "Delete"
      );

      if (confirm === "Delete") {
        try {
          // 1. Close the tab if it's open
          for (const tabGroup of vscode.window.tabGroups.all) {
            for (const tab of tabGroup.tabs) {
              const tabInput = tab.input as { uri?: vscode.Uri } | undefined;
              if (tabInput?.uri?.toString() === uri.toString()) {
                await vscode.window.tabGroups.close(tab);
              }
            }
          }

          // 2. Delete file
          await vscode.workspace.fs.delete(uri);

          // 2. Clean up index/lineage (Optional but good)
          // We'd ideally call PipelineRepository.deletePipeline(uri)
          // For now, the file watcher in SidebarProvider will auto-refresh the list.
          // Repository might have stale entries until next run/save, but that's acceptable for V1.

          // 3. Delete markdown doc if exists?
          const mdUri = uri.with({ path: uri.path.replace('.dpnb', '.md') });
          try {
            await vscode.workspace.fs.delete(mdUri);
          } catch { } // ignore if missing

        } catch (e: unknown) {
          const msg = e instanceof Error ? e.message : String(e);
          vscode.window.showErrorMessage(`Failed to delete notebook: ${msg}`);
        }
      }
    })
  );

  // Commands: Open Schema ERD
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.erd.openSchema", async (item: ExplorerItem) => {
      const { openSchemaErdCommand } = require('./erd/openSchemaCommand');
      await openSchemaErdCommand(context, item);
    }),
    vscode.commands.registerCommand("dp.schema.generateDescriptionsWithAI", async (item: ExplorerItem) => {
      const { generateDescriptionsWithAI } = require('./schema/descriptionGenerator');
      await generateDescriptionsWithAI(context, item);
    }),
    vscode.commands.registerCommand("dp.schema.generateDescriptionsLocalDuckDB", async () => {
      const { generateDescriptionsWithAI } = require('./schema/descriptionGenerator');
      // Pass a fake item with schemaName='duck-piper-local-data-work' to trigger the specific logic
      // we added in generateDescriptionsWithAI
      await generateDescriptionsWithAI(context, { schemaName: 'duck-piper-local-data-work' });
    })
  );

  // -----------------------------
  // Memory Recall (History - NEW)
  // -----------------------------
  const { MemoryRecallProvider } = require('./panels/memoryRecallView');
  const { openMemoryRecallQuery } = require('./commands/memoryRecallCommands');



  // Register Provider
  const memoryRecallProvider = new MemoryRecallProvider();
  context.subscriptions.push(
    vscode.window.registerTreeDataProvider("dp.memoryRecallView", memoryRecallProvider)
  );

  // Register Commands
  context.subscriptions.push(
    vscode.commands.registerCommand("dp.memoryRecall.openQuery", async (entry) => {
      await openMemoryRecallQuery(context, entry);
    }),
    vscode.commands.registerCommand("dp.memoryRecall.refresh", () => {
      memoryRecallProvider.refresh();
    })
  );

  return {
    registerProvider: (descriptor) => ProviderRegistry.getInstance().registerProvider(descriptor),
    registerAdapter: (dialect, factory) => registerAdapter(dialect, factory),
    registerProviderActionHandler: (dialect, handler) => ProviderRegistry.getInstance().registerProviderActionHandler(dialect, handler),
    getProviders: () => ProviderRegistry.getInstance().getProviders(),
    getConnectionProfiles: () => loadConnectionProfiles(),
    saveConnectionProfile: (profile) => saveConnectionProfile(profile),
    getConnectionSecrets: (id) => getConnectionSecrets(id),
  };
}

export function deactivate() {
  // Cleanup if needed
}

function isSqlDoc(doc: vscode.TextDocument): boolean {
  const id = doc.languageId.toLowerCase();
  return id.includes("sql") || id.includes("pgsql") || id.includes("mysql");
}

function checkForDDL(sql: string): boolean {
  return /\b(CREATE|DROP|ALTER|TRUNCATE)\s+/i.test(sql);
}

function buildTableFqnForPreview(schemaName: string | undefined, tableName: string, dialect: DbDialect): string {
  if (!schemaName) {
    return quoteIdentifier(dialect, tableName);
  }

  // Snowflake can represent schema as DATABASE.SCHEMA in introspection.
  if (dialect === 'snowflake' && schemaName.includes('.')) {
    const parts = schemaName.split('.');
    const database = parts.shift() ?? '';
    const schema = parts.join('.');
    if (database && schema) {
      return `${quoteIdentifier(dialect, database)}.${quoteIdentifier(dialect, schema)}.${quoteIdentifier(dialect, tableName)}`;
    }
  }

  return `${quoteIdentifier(dialect, schemaName)}.${quoteIdentifier(dialect, tableName)}`;
}

function createResultId(): string {
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function normalizeSqlForComparison(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim().toLowerCase();
}

function parseSimpleSelectSource(sql: string): QueryResultSource | null {
  const cleaned = stripSqlComments(sql).trim().replace(/;+\s*$/g, '');
  if (!/^select\b/i.test(cleaned)) {
    return null;
  }

  if (/\bjoin\b/i.test(cleaned)) {
    return null;
  }

  const ident = String.raw`(?:"[^"]+"|` + '`[^`]+`' + String.raw`|\[[^\]]+\]|[a-zA-Z_][\w$]*)`;
  const fromRegex = new RegExp(String.raw`\bfrom\s+(${ident}(?:\s*\.\s*${ident}){0,2})`, 'i');
  const match = cleaned.match(fromRegex);
  if (!match) {
    return null;
  }

  const fromExpr = match[1].trim();
  if (!fromExpr || fromExpr.startsWith('(')) {
    return null;
  }

  const parts = fromExpr
    .split(/\s*\.\s*/)
    .map(unquoteIdentifierPart)
    .filter((part) => part.length > 0);

  if (parts.length === 1) {
    return { table: parts[0] };
  }
  if (parts.length === 2) {
    return { schema: parts[0], table: parts[1] };
  }
  if (parts.length === 3) {
    return { catalog: parts[0], schema: parts[1], table: parts[2] };
  }

  return null;
}

function stripSqlComments(sql: string): string {
  return sql
    .replace(/--.*$/gm, '')
    .replace(/\/\*[\s\S]*?\*\//g, ' ');
}

function unquoteIdentifierPart(part: string): string {
  const trimmed = part.trim();
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
    (trimmed.startsWith('`') && trimmed.endsWith('`'))) {
    return trimmed.slice(1, -1);
  }
  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

function formatSourceLabel(source: QueryResultSource): string {
  const parts = [source.catalog, source.schema, source.table].filter((part) => !!part);
  return parts.join('.');
}

function normalizeRowEdit(edit: ResultsetRowEdit, editableColumns: Set<string>): ResultsetRowEdit | null {
  const changes = (edit.changes || [])
    .filter((change) => editableColumns.has(change.column))
    .filter((change) => change.oldValue !== change.newValue);

  if (changes.length === 0) {
    return null;
  }

  return {
    rowKey: edit.rowKey,
    changes
  };
}

async function executeNonQueryCompat(
  adapter: {
    executeNonQuery?: (profile: ConnectionProfile, secrets: ConnectionSecrets, sql: string) => Promise<{ affectedRows: number | null } | undefined>;
    runQuery?: (profile: ConnectionProfile, secrets: ConnectionSecrets, sql: string, options: { maxRows: number }) => Promise<unknown>;
  },
  profile: ConnectionProfile,
  secrets: ConnectionSecrets,
  sql: string
): Promise<{ affectedRows: number | null }> {
  if (typeof adapter?.executeNonQuery === 'function') {
    const result = await adapter.executeNonQuery(profile, secrets, sql);
    const affectedRows = typeof result?.affectedRows === 'number'
      ? Number(result.affectedRows)
      : null;
    return { affectedRows };
  }

  // Backward compatibility: provider adapters that have not implemented executeNonQuery yet.
  if (typeof adapter?.runQuery === 'function') {
    await adapter.runQuery(profile, secrets, sql, { maxRows: 0 });
    return { affectedRows: null };
  }

  throw new Error('Connection adapter does not support updates. Upgrade the provider extension.');
}

function buildUpdateStatement(params: {
  dialect: DbDialect;
  source: QueryResultSource;
  rowEdit: ResultsetRowEdit;
  primaryKeyColumns: string[];
}): string {
  const { dialect, source, rowEdit, primaryKeyColumns } = params;
  const quote = (name: string) => quoteIdentifier(dialect, name);

  const tablePathParts = [source.catalog, source.schema, source.table]
    .filter((part): part is string => typeof part === 'string' && part.length > 0)
    .map((part) => quote(part));
  const tablePath = tablePathParts.join('.');

  const setClause = rowEdit.changes
    .map((change) => `${quote(change.column)} = ${toSqlLiteral(change.newValue, dialect)}`)
    .join(', ');

  const pkPredicate = primaryKeyColumns
    .map((pk) => {
      const value = rowEdit.rowKey[pk];
      if (value === null || value === undefined) {
        return `${quote(pk)} IS NULL`;
      }
      return `${quote(pk)} = ${toSqlLiteral(value, dialect)}`;
    })
    .join(' AND ');

  const optimisticPredicate = rowEdit.changes
    .map((change) => {
      if (change.oldValue === null || change.oldValue === undefined) {
        return `${quote(change.column)} IS NULL`;
      }
      return `${quote(change.column)} = ${toSqlLiteral(change.oldValue, dialect)}`;
    })
    .join(' AND ');

  return `UPDATE ${tablePath} SET ${setClause} WHERE ${pkPredicate} AND ${optimisticPredicate}`;
}


/**
 * Consolidates initialization of all DuckPiper core systems.
 * Call this during activation (if project is initialized) or after manual initialization.
 */
async function initializeProjectComponents(context: vscode.ExtensionContext) {
  try {
    const { queryIndex } = require('./queryLibrary/queryIndex');
    const { initializePromptFiles } = require('./ai/prompts');
    const { ensureReadmeMd } = require('./core/fsWorkspace');
    const { PipelineRepository } = require('./pipelines/pipelineRepository');
    const { LocalDuckDB } = require('./transform/localDuckDB');
    const { HistoryService } = require('./services/historyService');
    const { getConnection } = require('./connections/connectionStore');
    const { performIntrospection } = require('./connections/connectionCommands');

    // 1. Query Index
    await queryIndex.initialize();

    // 2. Prompt Files
    await initializePromptFiles();

    // 3. Documentation
    await ensureReadmeMd();
    // 4. Pipeline Repository
    await PipelineRepository.getInstance().initialize();

    // 5. Local DuckDB & Introspection
    await LocalDuckDB.getInstance().initialize();

    // Auto-introspect local duckdb
    const localProfile = await getConnection('duck-piper-local-data-work');
    if (localProfile) {
      await performIntrospection(localProfile, true);
    }

    // 6. History Service
    await HistoryService.getInstance().initialize(context);

  } catch (err) {
    Logger.error("Failed to initialize project components", err);
    throw err; // Re-throw to caller to handle/log
  }
}
