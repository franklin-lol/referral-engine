# Security Policy

## Reporting a vulnerability

Do **not** open a public GitHub issue for security vulnerabilities.

Report privately via GitHub's [Security Advisories](https://github.com/franklin-lol/referral-engine/security/advisories/new)
or by emailing the maintainer directly (contact in profile).

Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

Response within 72 hours.

## Scope

Issues in scope:
- Incorrect financial calculations (duplicate accruals, wrong amounts)
- Cycle detection bypass
- Idempotency key collision
- SQL injection via adapter layer

Out of scope:
- Issues in your own adapter implementation
- Rate configuration mistakes
