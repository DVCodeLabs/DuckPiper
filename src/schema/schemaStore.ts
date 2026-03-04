
import * as vscode from 'vscode';
import {
  SchemaIntrospection,
  CustomRelationshipsFile,
  CustomRelationship,
  SchemaModel,
  RoutineModel,
  RoutineParameterModel,
} from '../core/types';
import { ensureDPDirs, readJson, listFiles, writeJson, fileExists } from '../core/fsWorkspace';
import { SchemaDescriptionsFile } from './descriptionStore';
import { Logger } from '../core/logger';

function normalizeRoutineModel(rawRoutine: Record<string, unknown>, fallbackKind: 'procedure' | 'function'): RoutineModel {
  const kind = rawRoutine?.kind === 'procedure' || rawRoutine?.kind === 'function'
    ? rawRoutine.kind
    : fallbackKind;
  return {
    name: String(rawRoutine?.name ?? ''),
    kind,
    comment: typeof rawRoutine?.comment === 'string' ? rawRoutine.comment : undefined,
    returnType: typeof rawRoutine?.returnType === 'string' ? rawRoutine.returnType : undefined,
    language: typeof rawRoutine?.language === 'string' ? rawRoutine.language : undefined,
    deterministic: typeof rawRoutine?.deterministic === 'boolean' ? rawRoutine.deterministic : undefined,
    schemaQualifiedName: typeof rawRoutine?.schemaQualifiedName === 'string' ? rawRoutine.schemaQualifiedName : undefined,
    signature: typeof rawRoutine?.signature === 'string' ? rawRoutine.signature : undefined,
    parameters: Array.isArray(rawRoutine?.parameters)
      ? (rawRoutine.parameters as Record<string, unknown>[])
        .map((p: Record<string, unknown>) => ({
          name: String(p?.name ?? ''),
          mode: typeof p?.mode === 'string' ? p.mode as RoutineParameterModel['mode'] : undefined,
          type: typeof p?.type === 'string' ? p.type : undefined,
          position: typeof p?.position === 'number' ? p.position : undefined,
        }))
        .filter(p => p.name.length > 0)
      : [],
  };
}

function normalizeSchemaModel(rawSchema: Record<string, unknown>): SchemaModel {
  const tables = Array.isArray(rawSchema?.tables) ? rawSchema.tables : [];
  const views = Array.isArray(rawSchema?.views) ? rawSchema.views : [];
  const procedures = Array.isArray(rawSchema?.procedures)
    ? rawSchema.procedures.map((routine: Record<string, unknown>) => normalizeRoutineModel(routine, 'procedure'))
    : [];
  const functions = Array.isArray(rawSchema?.functions)
    ? rawSchema.functions.map((routine: Record<string, unknown>) => normalizeRoutineModel(routine, 'function'))
    : [];

  return {
    name: String(rawSchema?.name ?? ''),
    tables,
    views,
    procedures,
    functions,
  };
}

function normalizeSchemaIntrospection(rawSchema: Record<string, unknown>): SchemaIntrospection | undefined {
  if (!rawSchema || !rawSchema.connectionId) return undefined;
  const schemas = Array.isArray(rawSchema.schemas)
    ? (rawSchema.schemas as Record<string, unknown>[]).map(normalizeSchemaModel)
    : [];
  return {
    version: rawSchema.version === '0.2' ? '0.2' : '0.1',
    generatedAt: typeof rawSchema.generatedAt === 'string' ? rawSchema.generatedAt : new Date().toISOString(),
    connectionId: String(rawSchema.connectionId),
    connectionName: typeof rawSchema.connectionName === 'string' ? rawSchema.connectionName : undefined,
    dialect: String(rawSchema.dialect ?? '') as SchemaIntrospection['dialect'],
    docPath: typeof rawSchema.docPath === 'string' ? rawSchema.docPath : undefined,
    customRelationshipsPath: typeof rawSchema.customRelationshipsPath === 'string' ? rawSchema.customRelationshipsPath : undefined,
    schemas,
  };
}

export async function loadSchemas(): Promise<SchemaIntrospection[]> {
  const dpDir = await ensureDPDirs();
  const schemaDir = vscode.Uri.joinPath(dpDir, 'schemas');

  // ensure dir exists (list files might fail if not)
  // ensureDPDirs ensures 'schemas' subdir exists? Yes if implemented correctly.
  // Let's assume yes or catch.

  try {
    const files = await listFiles(schemaDir);
    // Exclude description and custom relationships files (they are separate metadata, not schema data)
    const jsonFiles = files.filter(f =>
      f.endsWith('.json') &&
      !f.endsWith('.description.json') &&
      !f.endsWith('.custom.relationships.json')
    );

    const allSchemas: SchemaIntrospection[] = [];
    for (const file of jsonFiles) {
      const uri = vscode.Uri.joinPath(schemaDir, file);
      try {
        const raw = await readJson<Record<string, unknown>>(uri);
        const s = normalizeSchemaIntrospection(raw);
        if (s && s.version) {
          allSchemas.push(s);
        }
      } catch (_e) {
        Logger.warn(`Failed to parse schema file ${file}:`, _e);
      }
    }

    // Deduplicate by connectionId - keep the one with more schemas
    const schemaMap = new Map<string, SchemaIntrospection>();
    for (const s of allSchemas) {
      const existing = schemaMap.get(s.connectionId);
      if (!existing || (s.schemas?.length ?? 0) > (existing.schemas?.length ?? 0)) {
        schemaMap.set(s.connectionId, s);
      }
    }

    return Array.from(schemaMap.values());
  } catch (_e) {
    // likely dir doesn't exist yet
    return [];
  }
}

export async function saveSchema(introspection: SchemaIntrospection) {
  const normalized = normalizeSchemaIntrospection(introspection as unknown as Record<string, unknown>);
  if (!normalized) {
    throw new Error('Invalid schema introspection payload');
  }

  const dpDir = await ensureDPDirs();
  const safeName = normalized.connectionName
    ? normalized.connectionName.replace(/[^a-z0-9_\-\.]/gi, '_')
    : normalized.connectionId;

  const filename = `${safeName}.json`;
  const uri = vscode.Uri.joinPath(dpDir, 'schemas', filename);

  // Auto-create/ensure description file exists
  const descFilename = `${safeName}.description.json`;
  const descUri = vscode.Uri.joinPath(dpDir, 'schemas', descFilename);

  if (!await fileExists(descUri)) {
    // Create default
    const defaultDesc: SchemaDescriptionsFile = {
      __dpHeader: "#DuckPiper created",
      version: "0.1",
      generatedAt: new Date().toISOString(),
      connectionId: normalized.connectionId,
      connectionName: normalized.connectionName,
      dialect: normalized.dialect,
      schemaName: normalized.schemas?.[0]?.name || 'main',
      tables: {},
      columns: {}
    };
    await writeJson(descUri, defaultDesc);
  }

  // Auto-create/ensure custom relationships file exists
  const customRelFilename = `${safeName}.custom.relationships.json`;
  const customRelUri = vscode.Uri.joinPath(dpDir, 'schemas', customRelFilename);

  if (!await fileExists(customRelUri)) {
    // Create default empty relationships file
    const defaultCustomRel: CustomRelationshipsFile = {
      version: "0.1",
      connectionId: normalized.connectionId,
      connectionName: normalized.connectionName,
      relationships: []
    };
    await writeJson(customRelUri, defaultCustomRel);
  }

  // Update introspection with docPath and customRelationshipsPath
  // Reconstruct object to ensure paths are after dialect
  const orderedIntrospection: SchemaIntrospection = {
    version: "0.2",
    generatedAt: normalized.generatedAt,
    connectionId: normalized.connectionId,
    connectionName: normalized.connectionName,
    dialect: normalized.dialect,
    docPath: descUri.fsPath,
    customRelationshipsPath: customRelUri.fsPath,
    schemas: normalized.schemas
  };

  await writeJson(uri, orderedIntrospection);
}

export async function deleteSchema(connectionId: string, connectionName?: string) {
  const dpDir = await ensureDPDirs();
  const schemaDir = vscode.Uri.joinPath(dpDir, 'schemas');

  // Try to delete by name first
  if (connectionName) {
    const safeName = connectionName.replace(/[^a-z0-9_\-\.]/gi, '_');

    // Delete schema file
    const uri = vscode.Uri.joinPath(schemaDir, `${safeName}.json`);
    try {
      await vscode.workspace.fs.delete(uri, { useTrash: false });
    } catch (_e) {
      // Ignore, file might not exist or name changed
    }

    // Delete description file
    const descUri = vscode.Uri.joinPath(schemaDir, `${safeName}.description.json`);
    try {
      await vscode.workspace.fs.delete(descUri, { useTrash: false });
    } catch (_e) {
      // Ignore
    }

    // Delete custom relationships file
    const customRelUri = vscode.Uri.joinPath(schemaDir, `${safeName}.custom.relationships.json`);
    try {
      await vscode.workspace.fs.delete(customRelUri, { useTrash: false });
    } catch (_e) {
      // Ignore
    }

    return;
  }

  // Fallback: try by ID
  const uriId = vscode.Uri.joinPath(schemaDir, `${connectionId}.json`);
  try {
    await vscode.workspace.fs.delete(uriId, { useTrash: false });
  } catch (_e) {
    // Ignore
  }

  // Also try deleting description and custom relationships by ID
  const descUriId = vscode.Uri.joinPath(schemaDir, `${connectionId}.description.json`);
  try {
    await vscode.workspace.fs.delete(descUriId, { useTrash: false });
  } catch (_e) {
    // Ignore
  }

  const customRelUriId = vscode.Uri.joinPath(schemaDir, `${connectionId}.custom.relationships.json`);
  try {
    await vscode.workspace.fs.delete(customRelUriId, { useTrash: false });
  } catch (_e) {
    // Ignore
  }

  // Loop through all and check content? Expensive.
  // For now, this covers the standard file creation patterns.
}

export async function renameSchemaFiles(connectionId: string, oldName: string, newName: string) {
  const dpDir = await ensureDPDirs();
  const schemaDir = vscode.Uri.joinPath(dpDir, 'schemas');

  const safeOld = oldName.replace(/[^a-z0-9_\-\.]/gi, '_');
  const safeNew = newName.replace(/[^a-z0-9_\-\.]/gi, '_');

  if (safeOld === safeNew) return;

  // 1. Rename Schema File
  const oldSchemaUri = vscode.Uri.joinPath(schemaDir, `${safeOld}.json`);
  const newSchemaUri = vscode.Uri.joinPath(schemaDir, `${safeNew}.json`);

  let schemaExists = false;
  try {
    if (await fileExists(oldSchemaUri)) {
      await vscode.workspace.fs.rename(oldSchemaUri, newSchemaUri, { overwrite: true });
      schemaExists = true;
    }
  } catch (_e) {
    Logger.warn("Failed to rename schema file:", _e);
  }

  // 2. Rename Description File
  const oldDescUri = vscode.Uri.joinPath(schemaDir, `${safeOld}.description.json`);
  const newDescUri = vscode.Uri.joinPath(schemaDir, `${safeNew}.description.json`);

  let descExists = false;
  try {
    if (await fileExists(oldDescUri)) {
      await vscode.workspace.fs.rename(oldDescUri, newDescUri, { overwrite: true });
      descExists = true;
    }
  } catch (_e) {
    Logger.warn("Failed to rename description file:", _e);
  }

  // 3. Rename Custom Relationships File
  const oldCustomRelUri = vscode.Uri.joinPath(schemaDir, `${safeOld}.custom.relationships.json`);
  const newCustomRelUri = vscode.Uri.joinPath(schemaDir, `${safeNew}.custom.relationships.json`);

  let customRelExists = false;
  try {
    if (await fileExists(oldCustomRelUri)) {
      await vscode.workspace.fs.rename(oldCustomRelUri, newCustomRelUri, { overwrite: true });
      customRelExists = true;
    }
  } catch (e) {
    Logger.warn("Failed to rename custom relationships file:", e);
  }

  // 4. Update contents if needed
  if (schemaExists) {
    try {
      const schema = await readJson<SchemaIntrospection>(newSchemaUri);
      if (schema) {
        let changed = false;
        if (schema.connectionName !== newName) {
          schema.connectionName = newName;
          changed = true;
        }
        if (descExists && schema.docPath) {
          // Update docPath to new location
          schema.docPath = newDescUri.fsPath;
          changed = true;
        }
        if (customRelExists && schema.customRelationshipsPath) {
          // Update customRelationshipsPath to new location
          schema.customRelationshipsPath = newCustomRelUri.fsPath;
          changed = true;
        }

        if (changed) {
          await writeJson(newSchemaUri, schema);
        }
      }
    } catch (e) {
      Logger.warn("Failed to update renamed schema content:", e);
    }
  }

  if (descExists) {
    try {
      const desc = await readJson<SchemaDescriptionsFile>(newDescUri);
      if (desc && desc.connectionName !== newName) {
        desc.connectionName = newName;
        await writeJson(newDescUri, desc);
      }
    } catch (e) {
      Logger.warn("Failed to update renamed description content:", e);
    }
  }

  if (customRelExists) {
    try {
      const customRel = await readJson<CustomRelationshipsFile>(newCustomRelUri);
      if (customRel && customRel.connectionName !== newName) {
        customRel.connectionName = newName;
        await writeJson(newCustomRelUri, customRel);
      }
    } catch (e) {
      Logger.warn("Failed to update renamed custom relationships content:", e);
    }
  }
}

// Custom Relationships Management
export async function loadCustomRelationships(customRelationshipsPath: string): Promise<CustomRelationship[]> {
  try {
    const uri = vscode.Uri.file(customRelationshipsPath);
    const file = await readJson<CustomRelationshipsFile>(uri);
    return file?.relationships || [];
  } catch (e) {
    Logger.warn('Failed to load custom relationships:', e);
    return [];
  }
}

export async function saveCustomRelationships(
  connectionId: string,
  connectionName: string,
  relationships: CustomRelationship[]
) {
  const dpDir = await ensureDPDirs();
  const safeName = connectionName.replace(/[^a-z0-9_\-\.]/gi, '_');
  const filename = `${safeName}.custom.relationships.json`;
  const uri = vscode.Uri.joinPath(dpDir, 'schemas', filename);

  const file: CustomRelationshipsFile = {
    version: "0.1",
    connectionId,
    connectionName,
    relationships
  };

  await writeJson(uri, file);
}
