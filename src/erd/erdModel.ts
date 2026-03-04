export interface ErdNode {
    id: string;
    label: string;
}

export interface ErdEdge {
    source: string;
    target: string;
}

export function generateErd(_schema: unknown): { nodes: ErdNode[], edges: ErdEdge[] } {
    return { nodes: [], edges: [] };
}
