import * as vscode from 'vscode';
import { ResultsViewProvider } from '../results/resultsView';
import { getAIProvider } from './aiService';
import { ErrorHandler, ErrorSeverity, formatAIError, formatGeneralError } from '../core/errorHandler';

export async function generateChart(context: vscode.ExtensionContext, docUriArg?: string | vscode.Uri) {
    let docUri: string;

    if (docUriArg) {
        docUri = docUriArg.toString();
    } else {
        const editor = vscode.window.activeTextEditor;
        if (!editor) {
            await ErrorHandler.handle(
                new Error(formatGeneralError(
                    'Chart generation',
                    'No active SQL editor found',
                    'Open a SQL file with query results'
                )),
                { severity: ErrorSeverity.Warning, context: 'Generate Chart' }
            );
            return;
        }
        docUri = editor.document.uri.toString();
    }

    const provider = ResultsViewProvider.current;
    const result = provider?.getLastResult(vscode.Uri.parse(docUri));

    if (!provider || !result) {
        await ErrorHandler.handle(
            new Error(formatGeneralError(
                'Chart generation',
                'No query results found',
                'Run a query first to generate charts from results'
            )),
            { severity: ErrorSeverity.Warning, context: 'Generate Chart' }
        );
        return;
    }

    // Create a lightweight context for AI
    const contextData = {
        columns: result.columns.map(c => ({ name: c.name, type: c.type || 'unknown' })),
        sampleRows: result.rows.slice(0, 5)
    };

    vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: "Generating chart configuration...",
        cancellable: false
    }, async () => {
        try {
            const ai = await getAIProvider(context);
            const prompt = createChartPrompt(contextData);
            const response = await ai.generateCompletion(prompt);

            const config = parseJson(response);

            // Send to webview
            provider.postMessage(vscode.Uri.parse(docUri), 'chartConfig', config);

        } catch (e: unknown) {
            await ErrorHandler.handle(e, {
                severity: ErrorSeverity.Error,
                userMessage: formatAIError(
                    'Chart generation',
                    'AI',
                    ErrorHandler.extractErrorMessage(e),
                    'Check AI provider settings and query results'
                ),
                context: 'Generate Chart'
            });
        }
    });
}

function createChartPrompt(data: { columns: { name: string; type: string }[]; sampleRows: unknown[] }): string {
    return `You are a data visualization expert. Given the following SQL query result structure and sample data, recommend a Chart.js configuration.

Data Structure:
${JSON.stringify(data, null, 2)}

Requirements:
1. Return a JSON object defining how to map the data to a chart.
2. The format must be:
{
    "type": "bar" | "line" | "pie" | "doughnut" | "scatter" | "radar",
    "title": "Chart Title",
    "labelColumn": "name_of_column_for_x_axis_or_labels",
    "datasetColumns": ["name_of_column_1", "name_of_column_2"]
}
3. Select the most appropriate chart type for the data.
    - Time series -> line/bar
    - Categorical comparison -> bar
    - Part-to-whole -> pie/doughnut
4. Return ONLY valid JSON. No markdown formatting.
`;
}

function parseJson(text: string): unknown {
    try {
        // cleanup markdown code blocks if present
        const clean = text.replace(/```json/g, '').replace(/```/g, '').trim();
        return JSON.parse(clean);
    } catch (_e) {
        throw new Error(formatAIError(
            'Chart configuration parsing',
            'AI',
            'Invalid JSON response',
            'Try regenerating the chart'
        ));
    }
}
