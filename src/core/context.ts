import * as vscode from "vscode";

export async function setHasActiveConnection(value: boolean): Promise<void> {
  await vscode.commands.executeCommand("setContext", "dp.hasActiveConnection", value);
}

export async function setHasActiveSchema(value: boolean): Promise<void> {
  await vscode.commands.executeCommand("setContext", "dp.hasActiveSchema", value);
}

export async function setHasSimilarQueries(value: boolean): Promise<void> {
  await vscode.commands.executeCommand("setContext", "dp.hasSimilarQueries", value);
}

export async function setCompareSourceSet(value: boolean): Promise<void> {
  await vscode.commands.executeCommand("setContext", "dp.compareSourceSet", value);
}

export async function setCompareSourceKind(value: string | undefined): Promise<void> {
  await vscode.commands.executeCommand("setContext", "dp.compareSourceKind", value ?? "");
}
