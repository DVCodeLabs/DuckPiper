# AI Provider Setup

AI features in Duck Piper are optional. The extension works without AI providers.

## Supported Providers

- VS Code LM API (`vscode`)
- OpenAI (`openai`)
- Anthropic (`anthropic`)
- Azure OpenAI (`azureOpenAI`)
- Ollama (`ollama`)
- OpenAI-compatible endpoint (`openaiCompatible`)

## Provider Selection

Set:

- `dp.ai.provider`
- `dp.ai.model`
- `dp.ai.endpoint` (when required)

Use `DP: Select AI Model` to choose from available model options.

## Common Setup Patterns

## `vscode`
- Best for users already using a VS Code agent/chat model ecosystem.
- No external endpoint required in common cases.

## `ollama`
- Use local models for offline/private flows.
- Set endpoint if not default (`http://localhost:11434`).

## Hosted APIs
- OpenAI/Anthropic/Azure OpenAI require network and credentials.
- Prefer secure secret storage, not plain-text committed settings.

## AI Features

- Query Markdown documentation generation
- Notebook Markdown documentation generation
- Inline SQL comments generation
- Optional chart mapping suggestions

## Prompt Templates

Duck Piper stores templates in:

- `DP/system/prompts/markdownDoc.txt`
- `DP/system/prompts/notebookMarkdownDoc.txt`
- `DP/system/prompts/inlineComments.txt`
- `DP/system/prompts/describeSchema.txt`

You can customize these to enforce team writing conventions.

