import * as vscode from 'vscode';
import { DbAdapter } from './connections/adapters/adapter';
import { ConnectionProfile, ConnectionSecrets, DPProviderActionHandler, DPProviderDescriptor } from './core/types';

export interface DuckPiperExtensionApi {
    registerProvider(descriptor: DPProviderDescriptor): vscode.Disposable;
    registerAdapter(dialect: string, factory: () => DbAdapter): vscode.Disposable;
    registerProviderActionHandler(dialect: string, handler: DPProviderActionHandler): vscode.Disposable;
    getProviders(): DPProviderDescriptor[];

    // Profile access for connector extensions (e.g. SecureQL)
    getConnectionProfiles(): Promise<ConnectionProfile[]>;
    saveConnectionProfile(profile: ConnectionProfile): Promise<void>;
    getConnectionSecrets(id: string): Promise<ConnectionSecrets>;
}
