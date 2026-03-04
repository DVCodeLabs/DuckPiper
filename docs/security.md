# Security and Secrets Guide

This guide describes how to operate Duck Piper safely in local-first and team environments.

## Default Security Posture

- Duck Piper is offline-first by default.
- Core workflows can run without cloud services.
- Local artifacts are persisted in your workspace under `DP/`.

## Credential Handling

- Do not commit credentials, API keys, or passwords.
- Prefer VS Code secret storage and environment-based secret injection.
- Review settings before sharing workspace files.

## AI Provider Safety

- Use `dp.ai.provider = none` for fully offline/no-AI environments.
- For hosted providers, treat endpoint and credentials as sensitive.
- For local models (for example, Ollama), verify local endpoint controls.

## Artifact Review Before Commit

Review these paths before pushing:

- `DP/schemas/`
- `DP/system/pipelines/`
- `DP/system/erd/`
- `DP/system/prompts/`
- Query/notebook Markdown docs

These files are intended to be reviewable artifacts, but should still be checked for sensitive data.

## Vulnerability Reporting

For security issues, follow private disclosure in [`../SECURITY.md`](../SECURITY.md).

