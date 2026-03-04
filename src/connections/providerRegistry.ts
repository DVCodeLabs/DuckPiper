import { DPProviderActionHandler, DPProviderActionResult, DPProviderDescriptor } from '../core/types';
import { BUILTIN_PROVIDERS } from './builtinProviders';
import { secureqlActionHandler } from './secureqlActionHandler';
import * as vscode from 'vscode';

export class ProviderRegistry {
    private static instance: ProviderRegistry;
    private providers = new Map<string, DPProviderDescriptor>(); // Keyed by dialect
    private actionHandlers = new Map<string, DPProviderActionHandler>(); // Keyed by dialect

    private constructor() {
        // Register built-in providers
        this.registerDefaultProviders();
        // Register built-in action handlers
        this.registerDefaultActionHandlers();
    }

    public static getInstance(): ProviderRegistry {
        if (!ProviderRegistry.instance) {
            ProviderRegistry.instance = new ProviderRegistry();
        }
        return ProviderRegistry.instance;
    }

    private registerDefaultProviders() {
        for (const provider of BUILTIN_PROVIDERS) {
            this.registerProvider(provider);
        }
    }

    private registerDefaultActionHandlers() {
        this.actionHandlers.set('secureql', secureqlActionHandler);
    }

    public registerProvider(descriptor: DPProviderDescriptor): vscode.Disposable {
        const previous = this.providers.get(descriptor.dialect);
        this.providers.set(descriptor.dialect, descriptor);
        return new vscode.Disposable(() => {
            const current = this.providers.get(descriptor.dialect);
            if (current === descriptor) {
                if (previous) {
                    this.providers.set(descriptor.dialect, previous);
                } else {
                    this.providers.delete(descriptor.dialect);
                }
            }
        });
    }

    public getProviders(): DPProviderDescriptor[] {
        return Array.from(this.providers.values()).sort((a, b) => a.displayName.localeCompare(b.displayName));
    }

    public getProvider(dialect: string): DPProviderDescriptor | undefined {
        return this.providers.get(dialect);
    }

    public registerProviderActionHandler(dialect: string, handler: DPProviderActionHandler): vscode.Disposable {
        const previous = this.actionHandlers.get(dialect);
        this.actionHandlers.set(dialect, handler);
        return new vscode.Disposable(() => {
            const current = this.actionHandlers.get(dialect);
            if (current === handler) {
                if (previous) {
                    this.actionHandlers.set(dialect, previous);
                } else {
                    this.actionHandlers.delete(dialect);
                }
            }
        });
    }

    public async runProviderAction(
        dialect: string,
        actionId: string,
        payload: Record<string, unknown>
    ): Promise<DPProviderActionResult | undefined> {
        const handler = this.actionHandlers.get(dialect);
        if (!handler) {
            return undefined;
        }
        const result = await handler(actionId, payload);
        return result ?? undefined;
    }

    /**
     * Scan for extensions contributing 'dp.provider'
     * For now, this is a placeholder for the extension handshake.
     */
    public async scanExtensions() {
    }
}
