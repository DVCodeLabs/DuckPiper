export class CompletionCache {
    private cache = new Map<string, unknown[]>();

    add(key: string, items: unknown[]) {
        this.cache.set(key, items);
    }

    get(key: string): unknown[] | undefined {
        return this.cache.get(key);
    }
}
