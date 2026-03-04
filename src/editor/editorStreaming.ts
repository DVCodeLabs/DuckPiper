import * as vscode from 'vscode';

/**
 * Simulates a streaming edit by progressively replacing the content in the editor.
 * This provides visual feedback to the user as if the text is being typed.
 */
export async function streamEdit(
    editor: vscode.TextEditor,
    finalText: string
): Promise<void> {
    const doc = editor.document;
    const lastLine = doc.lineCount > 0 ? doc.lineAt(doc.lineCount - 1).range.end : new vscode.Position(0, 0);
    const fullRange = new vscode.Range(new vscode.Position(0, 0), lastLine);

    // If the text is small, just replace it instantly
    if (finalText.length < 500) {
        await editor.edit(editBuilder => {
            editBuilder.replace(fullRange, finalText);
        });
        return;
    }

    // Chunking strategy for larger texts to simulate streaming
    const _updateFrequencyMs = 50;
    const _chunkSize = 200; // Characters per chunk

    const totalLength = finalText.length;
    let _currentLength = 0;
    const step = Math.ceil(totalLength / 10); // 10 steps

    for (let i = 0; i < totalLength; i += step) {
        const end = Math.min(i + step, totalLength);
        const chunk = finalText.slice(0, end);

        await editor.edit(editBuilder => {
            const currentLast = editor.document.lineAt(editor.document.lineCount - 1).range.end;
            const currentFull = new vscode.Range(new vscode.Position(0, 0), currentLast);
            editBuilder.replace(currentFull, chunk);
        });

        // Small delay to let the UI update
        await new Promise(r => setTimeout(r, 20));
    }
}
