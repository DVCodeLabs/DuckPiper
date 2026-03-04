
import { SchemaIntrospection, TableModel, ColumnModel, CustomRelationship } from '../core/types';
import { Node, Edge } from 'reactflow';
import { Logger } from '../core/logger';

export interface ERDData {
    nodes: Node[];
    edges: Edge[];
}

interface ColumnData {
    name: string;
    type: string;
    isPrimaryKey: boolean;
    isForeignKey: boolean;
    relationshipColor?: string; // Color for this relationship
}

// Color palette for relationships
const RELATIONSHIP_COLORS = [
    '#60a5fa', // blue
    '#f59e0b', // orange
    '#10b981', // green
    '#ef4444', // red
    '#8b5cf6', // purple
    '#ec4899', // pink
    '#14b8a6', // teal
    '#f97316', // dark orange
    '#06b6d4', // cyan
    '#84cc16', // lime
    '#a855f7', // violet
    '#fb923c', // light orange
];

export function generateERD(introspection: SchemaIntrospection, customRelationships?: CustomRelationship[]): ERDData {
    const nodes: Node[] = [];
    const edges: Edge[] = [];
    const tablesMap = new Map<string, TableModel>();

    // Layout configuration
    const _NODE_WIDTH = 250;
    const SPACING_X = 300;
    const SPACING_Y = 50;
    let xPos = 0;
    let yPos = 0;
    const COLS_PER_ROW = 4;

    // Helper: detect if column is likely a primary key
    const isPrimaryKey = (col: ColumnModel, tableName: string): boolean => {
        const colName = col.name.toLowerCase();
        const tblName = tableName.toLowerCase();
        // Common patterns: id, <table>_id, <table>Id
        return colName === 'id' || colName === `${tblName}_id` || colName === `${tblName}id`;
    };

    // Helper: detect if column is likely a foreign key
    const isForeignKey = (col: ColumnModel, table: TableModel): boolean => {
        if (table.foreignKeys) {
            return table.foreignKeys.some(fk => fk.column === col.name);
        }
        // Fallback heuristic
        const colName = col.name.toLowerCase();
        return colName.endsWith('_id') && colName !== 'id';
    };

    // Track relationships with colors
    interface RelationshipMeta {
        sourceTableId: string;
        sourceColumnIndex: number;
        targetTableId: string;
        targetColumnIndex: number;
        color: string;
    }
    const relationships: RelationshipMeta[] = [];
    let colorIndex = 0;

    // 1. Create Nodes with detailed column information
    for (const schema of introspection.schemas) {
        const allObjects = [...schema.tables, ...(schema.views || [])];
        for (const table of allObjects) {
            const tableId = `${schema.name}.${table.name}`;
            tablesMap.set(tableId, table);

            const columns: ColumnData[] = table.columns.map(c => ({
                name: c.name,
                type: c.type,
                isPrimaryKey: isPrimaryKey(c, table.name),
                isForeignKey: isForeignKey(c, table)
            }));

            nodes.push({
                id: tableId,
                position: { x: xPos, y: yPos },
                data: {
                    label: table.name,
                    nodeId: tableId,
                    columns: columns
                },
                type: 'tableNode'
            });

            // Grid layout calculation
            const estimatedHeight = 50 + (columns.length * 24);
            xPos += SPACING_X;
            if (nodes.length % COLS_PER_ROW === 0) {
                xPos = 0;
                yPos += Math.max(estimatedHeight, 200) + SPACING_Y;
            }
        }
    }

    // 2. Detect relationships and assign colors
    nodes.forEach(sourceNode => {
        const sourceTable = tablesMap.get(sourceNode.id);
        if (!sourceTable) return;

        // Use explicit foreign keys if available (introspected)
        if (sourceTable.foreignKeys && sourceTable.foreignKeys.length > 0) {
            sourceTable.foreignKeys.forEach(fk => {
                const targetId = `${fk.foreignSchema}.${fk.foreignTable}`;
                const targetTable = tablesMap.get(targetId);

                if (targetTable) {
                    const sourceColIndex = sourceTable.columns.findIndex(c => c.name === fk.column);
                    const targetColIndex = targetTable.columns.findIndex(c => c.name === fk.foreignColumn);

                    if (sourceColIndex !== -1 && targetColIndex !== -1) {
                        // Assign a color to this relationship
                        const color = RELATIONSHIP_COLORS[colorIndex % RELATIONSHIP_COLORS.length];
                        colorIndex++;

                        relationships.push({
                            sourceTableId: sourceNode.id,
                            sourceColumnIndex: sourceColIndex,
                            targetTableId: targetId,
                            targetColumnIndex: targetColIndex,
                            color
                        });

                        edges.push({
                            id: `e-${sourceNode.id}-${fk.name || 'fk'}-${sourceColIndex}-${targetId}-${targetColIndex}`,
                            source: sourceNode.id,
                            sourceHandle: `${sourceNode.id}-col-${sourceColIndex}`,
                            target: targetId,
                            targetHandle: `${targetId}-col-${targetColIndex}`,
                            type: 'smoothstep',
                            animated: false,
                            style: { stroke: color, strokeWidth: 2 }
                        });
                    }
                }
            });
            return;
        }

        // Heuristic fallback
        sourceTable.columns.forEach((col, colIndex) => {
            const colName = col.name.toLowerCase();

            // Check if this is a foreign key pattern
            if (colName.endsWith('_id') && colName !== 'id') {
                const targetBase = colName.substring(0, colName.length - 3); // "order" from "order_id"
                const candidates = [targetBase, `${targetBase}s`, `${targetBase}es`];

                for (const candidate of candidates) {
                    for (const [targetId, targetTable] of tablesMap.entries()) {
                        if (targetTable.name.toLowerCase() === candidate && targetId !== sourceNode.id) {
                            // Find the primary key column in target table
                            const targetPKIndex = targetTable.columns.findIndex(c =>
                                c.name.toLowerCase() === 'id' ||
                                c.name.toLowerCase() === `${targetBase}_id`
                            );

                            if (targetPKIndex !== -1) {
                                // Assign a color to this relationship
                                const color = RELATIONSHIP_COLORS[colorIndex % RELATIONSHIP_COLORS.length];
                                colorIndex++;

                                relationships.push({
                                    sourceTableId: sourceNode.id,
                                    sourceColumnIndex: colIndex,
                                    targetTableId: targetId,
                                    targetColumnIndex: targetPKIndex,
                                    color
                                });

                                edges.push({
                                    id: `e-${sourceNode.id}-${col.name}-${targetId}`,
                                    source: sourceNode.id,
                                    sourceHandle: `${sourceNode.id}-col-${colIndex}`,
                                    target: targetId,
                                    targetHandle: `${targetId}-col-${targetPKIndex}`,
                                    type: 'smoothstep',
                                    animated: false,
                                    style: { stroke: color, strokeWidth: 2 }
                                });
                            }
                        }
                    }
                }
            }
        });
    });

    // 2b. Process custom relationships (user-defined)
    if (customRelationships && customRelationships.length > 0) {
        customRelationships.forEach(rel => {
            const sourceTable = tablesMap.get(rel.source);
            const targetTable = tablesMap.get(rel.target);

            if (!sourceTable || !targetTable) {
                Logger.warn(`Custom relationship references non-existent table: ${rel.source} -> ${rel.target}`);
                return;
            }

            const sourceColIndex = sourceTable.columns.findIndex(c => c.name === rel.sourceColumn);
            const targetColIndex = targetTable.columns.findIndex(c => c.name === rel.targetColumn);

            if (sourceColIndex === -1 || targetColIndex === -1) {
                Logger.warn(`Custom relationship references non-existent column: ${rel.source}.${rel.sourceColumn} -> ${rel.target}.${rel.targetColumn}`);
                return;
            }

            // Check if this relationship already exists (avoid duplicates)
            const isDuplicate = edges.some(e =>
                e.source === rel.source &&
                e.sourceHandle === `${rel.source}-col-${sourceColIndex}` &&
                e.target === rel.target &&
                e.targetHandle === `${rel.target}-col-${targetColIndex}`
            );

            if (!isDuplicate) {
                const color = RELATIONSHIP_COLORS[colorIndex % RELATIONSHIP_COLORS.length];
                colorIndex++;

                relationships.push({
                    sourceTableId: rel.source,
                    sourceColumnIndex: sourceColIndex,
                    targetTableId: rel.target,
                    targetColumnIndex: targetColIndex,
                    color
                });

                edges.push({
                    id: `e-custom-${rel.source}-${rel.sourceColumn}-${rel.target}-${rel.targetColumn}`,
                    source: rel.source,
                    sourceHandle: `${rel.source}-col-${sourceColIndex}`,
                    target: rel.target,
                    targetHandle: `${rel.target}-col-${targetColIndex}`,
                    type: 'smoothstep',
                    animated: false,
                    style: { stroke: color, strokeWidth: 2, strokeDasharray: '5,5' } // Dashed for custom
                });
            }
        });
    }

    // 3. Apply relationship colors to columns in nodes
    nodes.forEach(node => {
        const nodeData = node.data as { columns: ColumnData[] };
        relationships.forEach(rel => {
            // Color the source FK column
            if (node.id === rel.sourceTableId) {
                nodeData.columns[rel.sourceColumnIndex].relationshipColor = rel.color;
            }
            // Color the target PK column
            if (node.id === rel.targetTableId) {
                nodeData.columns[rel.targetColumnIndex].relationshipColor = rel.color;
            }
        });
    });

    return { nodes, edges };
}

export function computeGraphSignature(nodes: Node[], edges: Edge[]): string {
    const nodeIds = nodes.map(n => n.id).sort();
    const edgeSigs = edges.map(e => `${e.source}|${e.sourceHandle}|${e.target}|${e.targetHandle}`).sort();
    return [...nodeIds, '---', ...edgeSigs].join('\n');
}
