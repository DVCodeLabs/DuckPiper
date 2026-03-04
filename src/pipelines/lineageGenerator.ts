import { PipelineLineage } from './types';
import { Node, Edge } from 'reactflow';

export interface LineageGraphData {
    nodes: Node[];
    edges: Edge[];
}

interface LineageColumnDisplay {
    name: string;
    type: string;
    isPrimaryKey: boolean;
    isForeignKey: boolean;
    relationshipColor: string | undefined;
}

export function generateLineageGraph(lineage: PipelineLineage): LineageGraphData {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Color palette for lineage relationships (same as ERD)
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
    let colorIndex = 0;

    // Map table FQN to columns for easy lookup
    const tableColumns = new Map<string, LineageColumnDisplay[]>();
    // Map table FQN to node ID for edge creation
    const tableIdMap = new Map<string, string>();

    // Constants for layout (simple grid for now, or use automated layout lib if available, but spec says Grid like ERD)
    // We can use a simple layer-based layout since it is a DAG (bronze -> silver -> gold)
    const LAYER_X_SPACING = 400;
    const NODE_Y_SPACING = 150;
    const layers = ['bronze', 'silver', 'gold'];
    const layerY: { [key: string]: number } = {
        'bronze': 0, 'silver': 0, 'gold': 0
    };

    // 1. Process Tables -> Nodes
    lineage.tables.forEach(table => {
        // Collect columns for this table
        // We filter the flattened lineage.columns array
        const cols = lineage.columns
            .filter(c => c.tableId === table.id)
            .map(c => ({
                name: c.name,
                type: c.type || 'unknown',
                isPrimaryKey: false, // Lineage doesn't track these explicitly yet
                isForeignKey: false,
                relationshipColor: undefined as string | undefined
            }));

        // Check if we need to add a Wildcard (*) pseudo-column
        // Scan edges to see if any edge targets or sources this table's wildcard
        const hasWildcardEdge = lineage.columnEdges.some(e =>
            e.from.endsWith('::' + table.fqn + '.*') ||
            e.to.endsWith('::' + table.fqn + '.*') ||
            e.from.endsWith(table.id + '.*') || // fallback ID format check
            e.to.endsWith(table.id + '.*')
        );

        if (hasWildcardEdge) {
            cols.push({
                name: '*',
                type: 'aggregate',
                isPrimaryKey: false,
                isForeignKey: false,
                relationshipColor: undefined
            });
        }

        tableColumns.set(table.fqn, cols);
        tableIdMap.set(table.fqn, table.id); // Or use fqn as ID? ERD uses "schema.table" as ID. 
        // Spec says: node.id = table.fqn (example: silver.stg_customer)
        // lineage.tables[0].id is currently "pipelineId::fqn" (from tracker).
        // Spec says: "node.id = table.fqn". Let's stick to FQN for the visual node ID to match ERD style.
        const nodeId = table.fqn;

        // Simple Layout Calculation
        const layerIndex = layers.indexOf(table.schema) >= 0 ? layers.indexOf(table.schema) : 0;
        const xPos = layerIndex * LAYER_X_SPACING;
        const yPos = layerY[table.schema] || 0;
        layerY[table.schema] = yPos + (50 + cols.length * 30) + NODE_Y_SPACING;

        nodes.push({
            id: nodeId,
            type: 'tableNode',
            position: { x: xPos, y: yPos },
            data: {
                label: table.fqn,
                nodeId: nodeId,
                columns: cols,
                layer: table.schema,
                status: table.status,
                deletedAt: table.deletedAt,
                deletedBy: table.deletedBy
            }
        });
    });

    // 2. Process Edges and Assign Colors
    lineage.columnEdges.forEach(ce => {
        // Parse IDs: pipelineId::schema.table.col
        // format: <pipelineId>::<fqn>.<col>

        // Helper to parse ID
        const parseId = (id: string) => {
            // Remove pipeline prefix if present
            const parts = id.split('::');
            const core = parts.length > 1 ? parts[1] : parts[0];

            // Core is "schema.table.col" 
            // BUT beware of "schema.table.*"

            const lastDot = core.lastIndexOf('.');
            if (lastDot === -1) return null;

            const tableFqn = core.substring(0, lastDot);
            const colName = core.substring(lastDot + 1);
            return { tableFqn, colName };
        };

        const source = parseId(ce.from);
        const target = parseId(ce.to);

        if (!source || !target) return;

        // Find column indices
        const sourceCols = tableColumns.get(source.tableFqn);
        const targetCols = tableColumns.get(target.tableFqn);

        if (!sourceCols || !targetCols) return;

        const sourceIndex = sourceCols.findIndex(c => c.name === source.colName);
        const targetIndex = targetCols.findIndex(c => c.name === target.colName);

        if (sourceIndex === -1 || targetIndex === -1) return;

        // Assign a unique color to this relationship
        const color = RELATIONSHIP_COLORS[colorIndex % RELATIONSHIP_COLORS.length];
        colorIndex++;

        // Apply color to both source and target columns
        sourceCols[sourceIndex].relationshipColor = color;
        targetCols[targetIndex].relationshipColor = color;

        // construct IDs
        const sourceNodeId = source.tableFqn;
        const targetNodeId = target.tableFqn;

        const edgeId = `e-${sourceNodeId}-${source.colName}-${targetNodeId}-${target.colName}`;

        const fullLabel = ce.transform && ce.transform !== '*' && ce.transform !== source.colName ? ce.transform : undefined;
        const shortLabel = fullLabel ? truncate(fullLabel, 20) : undefined;

        edges.push({
            id: edgeId,
            source: sourceNodeId,
            target: targetNodeId,
            sourceHandle: `${sourceNodeId}-col-${sourceIndex}`,
            targetHandle: `${targetNodeId}-col-${targetIndex}`,
            type: 'bezier',
            animated: false, // Enable animation by default for lineage
            label: shortLabel,
            labelStyle: {
                fontSize: 10,
                fill: color,
                fontFamily: 'var(--vscode-font-family)'
            },
            labelBgPadding: [4, 6],
            labelBgBorderRadius: 4,
            labelBgStyle: {
                fill: 'var(--vscode-editor-background)',
                stroke: 'var(--vscode-widget-border)',
                strokeWidth: 1,
                opacity: 0.9
            },
            style: { stroke: color, strokeWidth: 2 },
            data: {
                transformFull: ce.transform,
                labelFull: fullLabel,
                labelShort: shortLabel,
                confidence: ce.confidence,
                cellId: ce.cellId,
                reason: ce.reason,
                color: color // Store color for trace animation
            }
        });
    });

    return { nodes, edges };
}

function truncate(str: string, n: number) {
    return (str.length > n) ? str.substr(0, n - 1) + '...' : str;
}
