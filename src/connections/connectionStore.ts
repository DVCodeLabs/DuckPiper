import * as vscode from "vscode";
import { ConnectionProfile, ConnectionSecrets } from "../core/types";
import { ensureDPDirs, fileExists, readJson, writeJson } from "../core/fsWorkspace";
import { Logger } from '../core/logger';

let secretStorage: vscode.SecretStorage | undefined;

export function initConnectionStore(context: vscode.ExtensionContext) {
  secretStorage = context.secrets;
}

interface ConnectionsFile {
  version: "0.1";
  generatedAt: string;
  connections: ConnectionProfile[];
}

async function getConnectionsUri(createIfMissing = false): Promise<vscode.Uri | undefined> {
  const root = vscode.workspace.workspaceFolders?.[0]?.uri;
  if (!root) return undefined;

  if (createIfMissing) {
    const dpDir = await ensureDPDirs();
    return vscode.Uri.joinPath(dpDir, "system", "connections.json");
  }

  // Read-only path resolution must not create folders.
  return vscode.Uri.joinPath(root, "DP", "system", "connections.json");
}

export async function loadConnectionProfiles(): Promise<ConnectionProfile[]> {
  const uri = await getConnectionsUri(false);
  if (!uri) return [];
  let connections: ConnectionProfile[] = [];

  if (await fileExists(uri)) {
    try {
      const file = await readJson<ConnectionsFile>(uri);
      connections = file.connections || [];

      // Migration: 'type' -> 'dialect' (v0 legacy fix)
      let changed = false;
      connections.forEach((c) => {
        const legacy = c as unknown as Record<string, unknown>;
        if (!legacy.dialect && legacy.type) {
          legacy.dialect = legacy.type;
          delete legacy.type;
          changed = true;
        }
      });

      if (changed) {
        await writeJson(uri, { ...file, connections });
      }

    } catch (e) {
      Logger.error("Failed to load connections.json", e);
    }
  }

  // Ensure default Data Work connection exists — but only when the
  // DP/ project structure already exists (connections.json was found
  // above or the directory was previously created).  This prevents
  // auto-creating DP/ folders in a brand-new project.
  if (await fileExists(uri) || connections.length > 0) {
    const defaultId = 'duck-piper-local-data-work';

    // Migration: Ensure existing duck-piper-local-data-work connection has isLocalDuckPiperCacheDb flag
    const existingDefault = connections.find(c => c.id === defaultId);
    if (existingDefault && !existingDefault.isLocalDuckPiperCacheDb) {
      existingDefault.isLocalDuckPiperCacheDb = true;
      await writeJson(uri, { version: "0.1", generatedAt: new Date().toISOString(), connections });
    }

    if (!existingDefault) {
      // Resolve default path from config
      const config = vscode.workspace.getConfiguration('dp');
      const relPath = config.get<string>('duckdb.path', 'DP/system/piper.duckdb');
      let dbPath = relPath;

      if (vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0) {
        const root = vscode.workspace.workspaceFolders[0].uri.fsPath;
        const path = require('path');
        dbPath = path.join(root, relPath);
      }

      connections.unshift({
        id: defaultId,
        name: 'Duck_Piper',
        dialect: 'duckdb',
        filePath: dbPath,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        isLocalDuckPiperCacheDb: true // Flag as local cache
      });
      // Use createIfMissing=true for the write since we know the project exists
      const writeUri = await getConnectionsUri(true);
      if (writeUri) {
        await writeJson(writeUri, { version: "0.1", generatedAt: new Date().toISOString(), connections });
      }
    }
  }

  return connections;
}

export async function saveConnectionProfile(profile: ConnectionProfile): Promise<void> {
  const uri = await getConnectionsUri(true);
  if (!uri) {
    throw new Error("No workspace folder open.");
  }
  let connections = await loadConnectionProfiles();

  const idx = connections.findIndex(c => c.id === profile.id);
  if (idx >= 0) {
    // Check for name change
    const existing = connections[idx];
    if (existing.name !== profile.name) {
      // Renamed!
      try {
        const { renameSchemaFiles } = require('../schema/schemaStore');
        await renameSchemaFiles(profile.id, existing.name, profile.name);
      } catch (e) {
        Logger.error("Failed to rename schema files:", e);
      }
    }
    connections[idx] = profile;
  } else {
    connections.push(profile);
  }

  const file: ConnectionsFile = {
    version: "0.1",
    generatedAt: new Date().toISOString(),
    connections
  };
  await writeJson(uri, file);
}

export async function deleteConnection(id: string): Promise<void> {
  const uri = await getConnectionsUri(true);
  if (!uri) {
    throw new Error("No workspace folder open.");
  }
  let connections = await loadConnectionProfiles();
  connections = connections.filter(c => c.id !== id);

  const file: ConnectionsFile = {
    version: "0.1",
    generatedAt: new Date().toISOString(),
    connections
  };
  await writeJson(uri, file);

  // also delete secrets
  if (secretStorage) {
    await secretStorage.delete(`dp.secrets.${id}`);
  }
}

const sessionSecrets = new Map<string, string>(); // JSON stringified

export async function getConnectionSecrets(id: string): Promise<ConnectionSecrets> {
  // Determine mode from profile
  const profile = await getConnection(id);
  // Default to 'secretStorage' for legacy compatibility if undefined
  const mode = profile?.credentialStorageMode || 'secretStorage';

  if (mode === 'session') {
    if (sessionSecrets.has(id)) {
      try {
        return JSON.parse(sessionSecrets.get(id)!);
      } catch {
        return {};
      }
    }
    return {};
  } else if (mode === 'secretStorage') {
    if (!secretStorage) { return {}; }
    const json = await secretStorage.get(`dp.secrets.${id}`);
    if (!json) { return {}; }
    try {
      return JSON.parse(json) as ConnectionSecrets;
    } catch {
      return {};
    }
  }
  return {};
}

export async function saveConnectionSecrets(id: string, secrets: ConnectionSecrets, explicitMode?: 'session' | 'secretStorage' | 'browser'): Promise<void> {
  let mode = explicitMode;
  if (!mode) {
    const profile = await getConnection(id);
    mode = profile?.credentialStorageMode || 'secretStorage';
  }

  if (mode === 'session') {
    sessionSecrets.set(id, JSON.stringify(secrets));
    // Optional: Clear from secretStorage if previously there?
    // For safety/cleanup, we could attempts to delete from secretStorage.
    if (secretStorage) {
      await secretStorage.delete(`dp.secrets.${id}`);
    }
  } else if (mode === 'secretStorage') {
    if (!secretStorage) {
      throw new Error("SecretStorage not initialized");
    }
    await secretStorage.store(`dp.secrets.${id}`, JSON.stringify(secrets));
    // Clear from session if exists
    sessionSecrets.delete(id);
  }
}

export async function validateConnectionName(name: string, excludeId?: string): Promise<string | undefined> {
  const profiles = await loadConnectionProfiles();
  const clean = name.trim();
  if (!clean) return "Name cannot be empty";

  // Check against existing names (case-insensitive)
  const conflict = profiles.find(p => p.name.toLowerCase() === clean.toLowerCase() && p.id !== excludeId);
  if (conflict) {
    return `Connection name '${clean}' is already in use.`;
  }
  return undefined;
}
// Helper to get single connection
export async function getConnection(id: string): Promise<ConnectionProfile | undefined> {
  const profiles = await loadConnectionProfiles();
  return profiles.find(p => p.id === id);
}
