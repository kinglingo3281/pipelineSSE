# Security Policy

## Portfolio snapshot

This repository is a sanitized public portfolio snapshot of selected work from a larger private production system. It intentionally excludes production credentials, private endpoints, user information, proprietary strategies, restricted integrations, complete infrastructure configuration, and original private Git history.

## Reporting a concern

Please report a suspected security issue privately to **kinglingo3281@gmail.com**. Do not open a public issue containing credentials, private data, exploit details, or production endpoints.

## Configuration boundaries

- Browser-visible variables must contain only values safe for public clients.
- Credentials, signing material, database secrets, wallet secrets, and restricted API keys belong in protected server-side environment variables or secret stores.
- Example environment files contain placeholders only.
- No real production user records, account identifiers, wallet addresses, balances, or private URLs should be committed.

## Supported scope

These public repositories are portfolio artifacts rather than supported production distributions. Security reports are still welcome, especially when they identify accidentally exposed information or unsafe public defaults.
