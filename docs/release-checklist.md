# Release Pre-Publish Checklist

This checklist is for maintainers preparing a VS Code Marketplace release.

## 1) Version and Changelog

- [ ] Bump `package.json` version
- [ ] Add release notes entry to `CHANGELOG.md`
- [ ] Verify release notes match shipped behavior

## 2) Marketplace Metadata

- [ ] `package.json` includes `publisher`
- [ ] `package.json` includes `icon`
- [ ] `displayName` and `description` are accurate
- [ ] Categories and keywords are set for discoverability
- [ ] Repository/bugs/homepage links are valid

## 3) Listing Content

- [ ] `README.md` is user-facing and up to date
- [ ] Copy is aligned with `docs/marketplace-listing.md`
- [ ] Release notes draft prepared for Marketplace entry

## 4) Assets

- [ ] `media/icon.png` is final quality (`128x128`)
- [ ] Marketplace screenshots are real product captures (not placeholders)
- [ ] Banner image is final quality
- [ ] Feature images/GIFs reflect current UI
- [ ] Paths referenced in README are valid

## 5) Quality Gates

- [ ] `npm ci`
- [ ] `npm run lint`
- [ ] `npm test`
- [ ] `npm run package`
- [ ] Smoke-test extension in VS Code Extension Host

## 6) Security and Community Docs

- [ ] `SECURITY.md` exists and private reporting flow is current
- [ ] `CODE_OF_CONDUCT.md` exists in repo root
- [ ] `CONTRIBUTING.md` has no placeholder URLs/emails
- [ ] Issue/PR templates exist in `.github/`

## 7) Publish Prep (`vsce`)

- [ ] Install tooling:
  - `npm i -g @vscode/vsce`
- [ ] Authenticate publisher:
  - `vsce login <publisher-id>`
- [ ] Package locally:
  - `vsce package`
- [ ] Inspect generated `.vsix`:
  - install in local VS Code and run smoke checks

## 8) Publish

- [ ] Publish:
  - `vsce publish`
  - or `vsce publish <major|minor|patch>`
- [ ] Confirm Marketplace listing updates (description, screenshots, version)
- [ ] Tag release in GitHub (`vX.Y.Z`)
- [ ] Announce release with changelog links

## 9) Post-Publish Verification

- [ ] Fresh install test from Marketplace
- [ ] Upgrade test from previous version
- [ ] Core flows verified:
  - SQL run/results panel
  - connection introspection
  - notebook transform + lineage
  - ERD generation
- [ ] Open issues for regressions discovered during verification

## 10) Current Project-Specific Notes

- [ ] Replace placeholder marketplace assets under `media/marketplace/` before final public launch
- [ ] Keep offline-first + optional-AI messaging consistent across README/docs/listing

