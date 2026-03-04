import * as vscode from 'vscode';
import { Logger } from '../core/logger';
import { formatAIError } from '../core/errorHandler';

export interface AIProvider {
    generateCompletion(prompt: string): Promise<string>;
    streamCompletion?(prompt: string, onChunk: (chunk: string) => void): Promise<void>;
}

export class MockAIProvider implements AIProvider {
    async generateCompletion(_prompt: string): Promise<string> {
        await new Promise(resolve => setTimeout(resolve, 500));
        return `# What this query answers

This is a mock response. Configure an AI provider to generate real content.

# Inputs

- Example input

# Business logic

- Example logic

# Output

- Example output

# Caveats

- Example caveat

# Performance notes

- Example performance note
`;
    }
}

type ProviderName = "vscode" | "openai" | "anthropic" | "azureOpenAI" | "ollama" | "openaiCompatible";

interface ProviderConfig {
    provider: ProviderName;
    model: string;
    endpoint?: string;
    apiKey?: string;
    source: "agent" | "settings";
}

function normalizeBaseUrl(endpoint: string, defaultBase: string): string {
    const base = endpoint && endpoint.trim().length > 0 ? endpoint.trim() : defaultBase;
    return base.replace(/\/+$/, "");
}

async function getAgentPanelConfig(): Promise<ProviderConfig | null> {
    const commands = await vscode.commands.getCommands(true);
    const candidates = [
        "dp.agent.getSelectedProvider",
        "dp.agent.getProviderConfig",
        "dp.agentPanel.getSelectedProvider"
    ];
    const cmd = candidates.find(c => commands.includes(c));
    if (!cmd) return null;

    try {
        const cfg = await vscode.commands.executeCommand(cmd) as Record<string, unknown> | undefined;
        if (!cfg?.provider || !cfg?.model) return null;
        return {
            provider: cfg.provider as ProviderName,
            model: cfg.model as string,
            endpoint: (cfg.endpoint || cfg.baseUrl) as string | undefined,
            apiKey: cfg.apiKey as string | undefined,
            source: "agent"
        } as ProviderConfig;
    } catch {
        return null;
    }
}

export async function openAiProviderSettings(): Promise<void> {
    const commands = await vscode.commands.getCommands(true);
    const candidates = [
        "dp.agent.open",
        "dp.agentPanel.open",
        "dp.agent.show"
    ];
    const cmd = candidates.find(c => commands.includes(c));
    if (cmd) {
        await vscode.commands.executeCommand(cmd);
        return;
    }
    await vscode.commands.executeCommand("workbench.action.openSettings", "dp.ai");
}

async function getSettingsConfig(context: vscode.ExtensionContext): Promise<ProviderConfig | null> {
    const config = vscode.workspace.getConfiguration('dp');
    const provider = config.get<string>('ai.provider', 'vscode');
    if (!provider || provider === "none") return null;

    const model = config.get<string>('ai.model', '');
    const endpoint = config.get<string>('ai.endpoint', '');
    const apiKey = await context.secrets.get('dp.ai.apiKey');

    return {
        provider: provider as ProviderName,
        model,
        endpoint,
        apiKey: apiKey || undefined,
        source: "settings"
    };
}

function requiresApiKey(provider: ProviderName): boolean {
    return provider === "openai" || provider === "anthropic" || provider === "azureOpenAI";
}

function resolveConfig(candidate: ProviderConfig | null): ProviderConfig | null {
    if (!candidate) return null;
    if (!candidate.provider) return null;
    if (candidate.provider !== "vscode" && candidate.provider !== "ollama" && !candidate.model) return null;
    if (requiresApiKey(candidate.provider) && !candidate.apiKey) return null;
    if ((candidate.provider === "openaiCompatible" || candidate.provider === "azureOpenAI") && !candidate.endpoint) return null;
    return candidate;
}

async function resolveProviderConfig(context: vscode.ExtensionContext): Promise<ProviderConfig | null> {
    const agent = resolveConfig(await getAgentPanelConfig());
    if (agent) return agent;
    return resolveConfig(await getSettingsConfig(context));
}

/** Response shape from OpenAI-compatible chat/completions endpoints */
interface OpenAIChatResponse {
    choices?: { message?: { content?: string } }[];
}

/** Response shape from Anthropic /v1/messages endpoint */
interface AnthropicResponse {
    content?: { text?: string }[];
}

/** Response shape from Ollama /api/generate endpoint */
interface OllamaGenerateResponse {
    response?: string;
}

/** Response shape from Ollama /api/tags endpoint */
interface OllamaTagsResponse {
    models?: { name: string }[];
}

/** A content part from VS Code LM or similar response */
interface LMContentPart {
    text?: string;
    value?: string;
    content?: string;
}

class HttpAIProvider implements AIProvider {
    constructor(private cfg: ProviderConfig) { }

    async generateCompletion(prompt: string): Promise<string> {
        switch (this.cfg.provider) {
            case "openai":
                return this.callOpenAI(prompt);
            case "openaiCompatible":
                return this.callOpenAICompatible(prompt);
            case "azureOpenAI":
                return this.callAzureOpenAI(prompt);
            case "anthropic":
                return this.callAnthropic(prompt);
            case "ollama":
                return this.callOllama(prompt);
            default:
                throw new Error(formatAIError(
                    'AI request',
                    this.cfg.provider,
                    'Provider not supported',
                    'Check AI provider settings'
                ));
        }
    }

    private async callOpenAI(prompt: string): Promise<string> {
        const base = normalizeBaseUrl(this.cfg.endpoint || "", "https://api.openai.com/v1");
        const res = await fetch(`${base}/chat/completions`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Authorization": `Bearer ${this.cfg.apiKey}`
            },
            body: JSON.stringify({
                model: this.cfg.model,
                messages: [{ role: "user", content: prompt }],
                temperature: 0.2
            })
        });

        if (!res.ok) {
            const errText = await res.text();
            let reason = `HTTP ${res.status}`;
            let suggestion = 'Check API key and try again';

            if (res.status === 401) {
                reason = 'Authentication failed';
                suggestion = 'Check your OpenAI API key in settings';
            } else if (res.status === 429) {
                reason = 'Rate limit exceeded';
                suggestion = 'Wait a moment and try again';
            } else if (res.status >= 500) {
                reason = 'OpenAI service error';
                suggestion = 'Try again later';
            } else if (errText) {
                reason = errText.substring(0, 100);
            }

            throw new Error(formatAIError('AI request', 'OpenAI', reason, suggestion));
        }
        const json = await res.json() as OpenAIChatResponse;
        return json.choices?.[0]?.message?.content ?? "";
    }

    private async callOpenAICompatible(prompt: string): Promise<string> {
        const base = normalizeBaseUrl(this.cfg.endpoint || "", "");
        const headers: Record<string, string> = {
            "Content-Type": "application/json"
        };
        if (this.cfg.apiKey) {
            headers["Authorization"] = `Bearer ${this.cfg.apiKey}`;
        }
        const res = await fetch(`${base}/chat/completions`, {
            method: "POST",
            headers,
            body: JSON.stringify({
                model: this.cfg.model,
                messages: [{ role: "user", content: prompt }],
                temperature: 0.2
            })
        });

        if (!res.ok) {
            const errText = await res.text();
            let reason = `HTTP ${res.status}`;
            let suggestion = 'Check endpoint and API key settings';

            if (res.status === 401) {
                reason = 'Authentication failed';
            } else if (res.status === 429) {
                reason = 'Rate limit exceeded';
                suggestion = 'Wait a moment and try again';
            } else if (res.status >= 500) {
                reason = 'Server error';
                suggestion = 'Check endpoint URL and try again later';
            } else if (errText) {
                reason = errText.substring(0, 100);
            }

            throw new Error(formatAIError('AI request', 'OpenAI-compatible', reason, suggestion));
        }
        const json = await res.json() as OpenAIChatResponse;
        return json.choices?.[0]?.message?.content ?? "";
    }

    private async callAzureOpenAI(prompt: string): Promise<string> {
        const base = normalizeBaseUrl(this.cfg.endpoint || "", "");
        const url = `${base}/openai/deployments/${encodeURIComponent(this.cfg.model)}/chat/completions?api-version=2024-02-15-preview`;
        const res = await fetch(url, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "api-key": this.cfg.apiKey || ""
            },
            body: JSON.stringify({
                messages: [{ role: "user", content: prompt }],
                temperature: 0.2
            })
        });

        if (!res.ok) {
            const errText = await res.text();
            let reason = `HTTP ${res.status}`;
            let suggestion = 'Check Azure endpoint and API key';

            if (res.status === 401 || res.status === 403) {
                reason = 'Authentication failed';
            } else if (res.status === 429) {
                reason = 'Rate limit exceeded';
                suggestion = 'Wait a moment and try again';
            } else if (res.status >= 500) {
                reason = 'Azure service error';
                suggestion = 'Try again later';
            } else if (errText) {
                reason = errText.substring(0, 100);
            }

            throw new Error(formatAIError('AI request', 'Azure OpenAI', reason, suggestion));
        }
        const json = await res.json() as OpenAIChatResponse;
        return json.choices?.[0]?.message?.content ?? "";
    }

    private async callAnthropic(prompt: string): Promise<string> {
        const base = normalizeBaseUrl(this.cfg.endpoint || "", "https://api.anthropic.com");
        const res = await fetch(`${base}/v1/messages`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "x-api-key": this.cfg.apiKey || "",
                "anthropic-version": "2023-06-01"
            },
            body: JSON.stringify({
                model: this.cfg.model,
                max_tokens: 1200,
                messages: [{ role: "user", content: prompt }]
            })
        });

        if (!res.ok) {
            const errText = await res.text();
            let reason = `HTTP ${res.status}`;
            let suggestion = 'Check API key and try again';

            if (res.status === 401) {
                reason = 'Authentication failed';
                suggestion = 'Check your Anthropic API key in settings';
            } else if (res.status === 429) {
                reason = 'Rate limit exceeded';
                suggestion = 'Wait a moment and try again';
            } else if (res.status >= 500) {
                reason = 'Anthropic service error';
                suggestion = 'Try again later';
            } else if (errText) {
                reason = errText.substring(0, 100);
            }

            throw new Error(formatAIError('AI request', 'Anthropic', reason, suggestion));
        }
        const json = await res.json() as AnthropicResponse;
        const content = json.content?.[0]?.text ?? "";
        return content;
    }

    private async callOllama(prompt: string): Promise<string> {
        const base = normalizeBaseUrl(this.cfg.endpoint || "", "http://localhost:11434");
        const res = await fetch(`${base}/api/generate`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                model: this.cfg.model,
                prompt,
                stream: false
            })
        });

        if (!res.ok) {
            const errText = await res.text();
            let reason = `HTTP ${res.status}`;
            let suggestion = 'Check that Ollama is running and the model is available';

            if (res.status === 404) {
                reason = 'Model not found';
                suggestion = 'Pull the model with "ollama pull" first';
            } else if (res.status >= 500) {
                reason = 'Ollama service error';
                suggestion = 'Check Ollama logs';
            } else if (errText) {
                reason = errText.substring(0, 100);
            }

            throw new Error(formatAIError('AI request', 'Ollama', reason, suggestion));
        }
        const json = await res.json() as OllamaGenerateResponse;
        return json.response ?? "";
    }
}

class VscodeLmProvider implements AIProvider {
    constructor(private cfg: ProviderConfig) { }

    async generateCompletion(prompt: string): Promise<string> {
        const lm = (vscode as Record<string, unknown>).lm as { selectChatModels?: (selector: Record<string, unknown>) => Promise<unknown[]> } | undefined;
        if (!lm || !lm.selectChatModels) {
            throw new Error(formatAIError(
                'AI request',
                'VS Code LM',
                'Language Model API not available',
                'Update to VS Code version that supports Language Models'
            ));
        }

        const selector: Record<string, string> = {};
        if (this.cfg.model && this.cfg.model.trim().length > 0) {
            selector.family = this.cfg.model;
        }

        let models = await lm.selectChatModels(selector);

        // If specific model not found, fall back to any
        if ((!models || models.length === 0) && selector.family) {

            models = await lm.selectChatModels({});
        }

        if (!models || models.length === 0) {
            throw new Error(formatAIError(
                'AI request',
                'VS Code LM',
                'No chat models available',
                'Install a VS Code chat extension like GitHub Copilot'
            ));
        }

        // Prefer one that matches our config if we fell back
        let model = models[0] as Record<string, unknown>;
        if (selector.family) {
            const best = models.find((m) => {
                const md = m as Record<string, unknown>;
                return md.family === selector.family || md.id === selector.family;
            });
            if (best) model = best as Record<string, unknown>;
        }



        const msgFactory = (vscode as Record<string, unknown>).LanguageModelChatMessage as Record<string, (...args: unknown[]) => unknown> | undefined;
        const messages = msgFactory?.User
            ? [msgFactory.User(prompt)]
            : [{ role: "user", content: prompt }];

        const sendRequest = model.sendRequest as (messages: unknown[], options: Record<string, unknown>, token: vscode.CancellationToken) => Promise<{ text: AsyncIterable<unknown> }>;
        const response = await sendRequest(messages, {}, new vscode.CancellationTokenSource().token);

        // The response is an async iterable. Each chunk should have a 'value' property for text.
        let result = "";
        for await (const part of response.text) {
            // VS Code LM API: part is a LanguageModelTextPart with .value
            if (typeof part === "string") {
                result += part;
            } else if (part && typeof part === "object") {
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                const p = part as any;
                if (typeof p.value === "string") {
                    result += p.value;
                } else {
                    // Fallback: try to extract text in various ways
                    const text = p.text ?? p.content ?? "";
                    if (typeof text === "string") {
                        result += text;
                    }
                }
            }
        }


        return result;
    }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- probes unknown LM response shapes at runtime
async function _extractLmTextNonStreaming(response: any): Promise<string> {
    if (!response) return "";
    if (typeof response === "string") return response;
    if (typeof response.text === "string") return response.text;
    if (response.text?.stream && Symbol.asyncIterator in response.text.stream) return "";
    if (typeof response.text === "function") {
        try {
            const value = await response.text();
            if (typeof value === "string") return value;
        } catch (e) {
            // Extraction method failed, trying next approach
            Logger.debug(`Text extraction via response.text() failed: ${e}`);
        }
    }
    if (typeof response.response?.text === "function") {
        try {
            const value = await response.response.text();
            if (typeof value === "string") return value;
        } catch (e) {
            // Extraction method failed, trying next approach
            Logger.debug(`Text extraction via response.response.text() failed: ${e}`);
        }
    }
    if (typeof response.response?.text === "string") return response.response.text;
    if (response.content && Array.isArray(response.content)) {
        return response.content.map((c: LMContentPart) => c.text ?? c.value ?? "").join("");
    }
    if (response.response?.content && Array.isArray(response.response.content)) {
        return response.response.content.map((c: LMContentPart) => c.text ?? c.value ?? "").join("");
    }
    return "";
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any -- probes unknown LM response shapes at runtime
async function _extractLmText(response: any): Promise<string> {
    if (!response) return "";
    if (typeof response === "string") return response;
    if (typeof response.text === "string") return response.text;
    if (typeof response.text === "function") {
        try {
            const value = await response.text();
            if (typeof value === "string") return value;
            if (value && Symbol.asyncIterator in value) return await consumeAsyncText(value);
        } catch (e) {
            // LM text extraction method failed, trying next approach
            Logger.debug(`LM text extraction via response.text() failed: ${e}`);
        }
    }
    if (typeof response.response?.text === "function") {
        try {
            const value = await response.response.text();
            if (typeof value === "string") return value;
            if (value && Symbol.asyncIterator in value) return await consumeAsyncText(value);
        } catch (e) {
            // LM text extraction method failed, trying next approach
            Logger.debug(`LM text extraction via response.response.text() failed: ${e}`);
        }
    }
    if (response.text && Symbol.asyncIterator in response.text) {
        return await consumeAsyncText(response.text);
    }
    if (response.stream && Symbol.asyncIterator in response.stream) {
        return await consumeAsyncText(response.stream);
    }
    if (response.text?.stream && Symbol.asyncIterator in response.text.stream) {
        return await consumeAsyncText(response.text.stream);
    }
    if (response.response?.stream && Symbol.asyncIterator in response.response.stream) {
        return await consumeAsyncText(response.response.stream);
    }
    if (typeof response.text?.value === "string") return response.text.value;
    if (typeof response.text?.text === "string") return response.text.text;
    if (typeof response.response?.text === "string") return response.response.text;
    if (typeof response.response?.text === "string") return response.response.text;
    if (response.content && Array.isArray(response.content)) {
        return response.content.map((c: LMContentPart) => c.text ?? c.value ?? "").join("");
    }
    if (response.response?.content && Array.isArray(response.response.content)) {
        return response.response.content.map((c: LMContentPart) => c.text ?? c.value ?? "").join("");
    }
    if (Symbol.asyncIterator in response) {
        return await consumeAsyncText(response as AsyncIterable<unknown>);
    }
    try {
        return JSON.stringify(response);
    } catch {
        return String(response);
    }
}

async function consumeAsyncText(stream: AsyncIterable<unknown>): Promise<string> {
    let acc = "";
    for await (const chunk of stream) {
        if (typeof chunk === "string") {
            acc += chunk;
            continue;
        }

        // Handle VS Code LanguageModelChatResponseChunk (it often uses 'part' for new API)
        const c = chunk as Record<string, unknown>;
        if (c.part) {
            if (typeof c.part === "string") acc += c.part;
            else {
                const p = c.part as Record<string, unknown>;
                if (p.value) acc += p.value;
                else if (p.text) acc += p.text;
            }
            continue;
        }

        const textField = c.text as Record<string, unknown> | string | undefined;
        if (typeof textField === "object" && textField !== null && typeof textField.value === "string") {
            acc += textField.value;
        } else if (typeof textField === "object" && textField !== null && typeof textField.text === "string") {
            acc += textField.text;
        } else if (typeof textField === "string") {
            acc += textField;
        } else if (typeof c.value === "string") {
            acc += c.value;
        } else if (typeof c.content === "string") {
            acc += c.content;
        } else if (Array.isArray(c.content)) {
            acc += c.content.map((cp: LMContentPart) => cp.text ?? cp.value ?? "").join("");
        } else if (Array.isArray(c.parts)) {
            acc += c.parts.map((p: LMContentPart) => p.text ?? p.value ?? "").join("");
        }
    }
    return acc;
}

async function pickOllamaModel(endpoint?: string): Promise<string | undefined> {
    try {
        const base = normalizeBaseUrl(endpoint || "", "http://localhost:11434");
        const res = await fetch(`${base}/api/tags`);
        if (!res.ok) return undefined;
        const json = await res.json() as OllamaTagsResponse;
        const models = (json.models || []).map((m) => m.name).filter(Boolean);
        if (models.length === 0) return undefined;
        const picked = await vscode.window.showQuickPick(models, { title: "Select Ollama model" });
        return picked || undefined;
    } catch {
        return undefined;
    }
}

export async function getConfiguredAIProvider(
    context: vscode.ExtensionContext,
    options?: { requireConfigured?: boolean }
): Promise<AIProvider | null> {
    const cfg = await resolveProviderConfig(context);
    if (!cfg) {
        if (options?.requireConfigured) return null;
        return new MockAIProvider();
    }
    if (cfg.provider === "vscode") {
        return new VscodeLmProvider(cfg);
    }
    if (cfg.provider === "ollama" && !cfg.model) {
        const picked = await pickOllamaModel(cfg.endpoint);
        if (picked) {
            const config = vscode.workspace.getConfiguration('dp');
            await config.update("ai.model", picked, vscode.ConfigurationTarget.Global);
            cfg.model = picked;
        }
    }
    return new HttpAIProvider(cfg);
}

export async function selectAIModel(): Promise<void> {
    const lm = (vscode as Record<string, unknown>).lm as { selectChatModels?: (selector: Record<string, unknown>) => Promise<unknown[]> } | undefined;
    if (!lm || !lm.selectChatModels) {
        vscode.window.showErrorMessage("VS Code Language Model API not available.");
        return;
    }

    try {
        const models = await lm.selectChatModels({});
        if (!models || models.length === 0) {
            vscode.window.showWarningMessage("No VS Code chat models found.");
            return;
        }

        // Gather unique families
        const items = models.map((m) => {
            const md = m as Record<string, string>;
            return {
                label: `$(hubot) ${md.family}`,
                description: md.name ?? md.id,
                family: md.family
            };
        });

        // Deduplicate by family to avoid showing same thing multiple times if multiple instances exist
        const uniqueItems = [];
        const seen = new Set();
        for (const item of items) {
            if (!seen.has(item.family)) {
                seen.add(item.family);
                uniqueItems.push(item);
            }
        }

        const picked = await vscode.window.showQuickPick(uniqueItems, {
            placeHolder: "Select an AI model family",
            title: "Select VS Code AI Model"
        });

        if (picked) {
            const config = vscode.workspace.getConfiguration("dp");
            await config.update("ai.model", picked.family, vscode.ConfigurationTarget.Global);
            vscode.window.showInformationMessage(`AI Model set to: ${picked.family}`);
        }

    } catch (e: unknown) {
        const message = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Failed to select model: ${message}`);
    }
}

export async function getAIProvider(
    context?: vscode.ExtensionContext
): Promise<AIProvider> {
    if (!context) return new MockAIProvider();
    return (await getConfiguredAIProvider(context)) ?? new MockAIProvider();
}
