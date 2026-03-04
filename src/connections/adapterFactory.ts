
import { DbAdapter } from './adapters/adapter';
import { DuckDBAdapter } from './adapters/duckdb';
import { PostgresAdapter } from './adapters/postgres';
import { DbDialect } from '../core/types';
import { Disposable } from 'vscode';

import { MySQLAdapter } from './adapters/mysql';
import { SecureQLAdapter } from './adapters/secureql';

type AdapterFactory = () => DbAdapter;

const adapterFactories = new Map<string, AdapterFactory>();

/** Callback injected after connectionStore is ready so the adapter can persist profile changes. */
let _saveProfileCallback: ((profile: any) => Promise<void>) | undefined;

export function setSecureQLSaveProfile(cb: (profile: any) => Promise<void>): void {
    _saveProfileCallback = cb;
}

function registerBuiltinAdapters() {
    if (adapterFactories.size > 0) return;
    adapterFactories.set('duckdb', () => new DuckDBAdapter());
    adapterFactories.set('postgres', () => new PostgresAdapter());
    adapterFactories.set('mysql', () => new MySQLAdapter());
    adapterFactories.set('secureql', () => new SecureQLAdapter(_saveProfileCallback));
}

registerBuiltinAdapters();

export function registerAdapter(dialect: string, factory: AdapterFactory): Disposable {
    const previous = adapterFactories.get(dialect);
    adapterFactories.set(dialect, factory);
    return new Disposable(() => {
        const current = adapterFactories.get(dialect);
        if (current === factory) {
            if (previous) {
                adapterFactories.set(dialect, previous);
            } else {
                adapterFactories.delete(dialect);
            }
        }
    });
}

export function hasAdapter(dialect: string): boolean {
    return adapterFactories.has(dialect);
}

export function getAdapter(dialect: DbDialect): DbAdapter {
    const factory = adapterFactories.get(dialect);
    if (!factory) {
        throw new Error(`Unsupported dialect: ${dialect}. Install or enable the provider extension for this dialect.`);
    }
    return factory();
}
