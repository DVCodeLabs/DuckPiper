# Security Policy

## Supported Versions

Security fixes are applied to the latest release line.

| Version | Supported |
| --- | --- |
| 0.0.x | :white_check_mark: |
| < 0.0.0 | :x: |

## Reporting a Vulnerability

Do not open public GitHub issues for security reports.

Please report vulnerabilities privately using one of these channels:

1. GitHub Security Advisories (preferred): use the repository's "Report a vulnerability" flow.
2. If advisory flow is unavailable, open a private maintainer contact request in GitHub Discussions and ask for a secure handoff channel.

Please include:

- Affected version(s)
- Reproduction steps
- Impact assessment
- Suggested mitigation (if known)

We will acknowledge reports as quickly as possible and provide updates during triage, fix, and disclosure.

## Credential and Secret Handling

Duck Piper stores data in the workspace and VS Code secret storage patterns used by the extension.

- Treat workspace files under `DP/` as project artifacts and review before commit.
- Do not commit credentials, API keys, or database passwords.
- Use VS Code secret storage features when available.
- For AI providers, prefer environment or secure secret storage over plain-text settings.

## Safe Defaults

- Duck Piper is offline-first by default.
- External connections and hosted AI providers are optional.
- Users control whether data leaves the local environment.

