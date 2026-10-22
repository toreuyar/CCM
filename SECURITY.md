# Security

## Reporting a vulnerability

Use **Security → Report a vulnerability** in this GitHub repository when private
vulnerability reporting is enabled. If that option is unavailable, request a
private reporting channel from the maintainer through GitHub without posting
exploit details or credentials in a public issue.

Include affected source/version, environment, reproduction steps, impact and a
minimal sanitized example. Never attach real API keys, private keys, raw production
traffic or full runtime databases. No response-time or supported-release guarantee
has been established; reports should identify the exact revision tested.

## Deployment boundaries

- Keep CCM on loopback unless a reviewed HTTPS deployment is configured. Public
  dashboard assets do not imply public access to authenticated APIs.
- The bootstrap key administers workforces/keys; provider credentials authorize
  upstream inference; CCM client keys authorize client operations. Keep them distinct.
- Tool grants default to deny. Enabling shell/MCP execution is a separate authority
  from a specialist's written capability description.
- Linux sandboxing requires supported kernel features. macOS native children are
  unconfined by default. WSL2 uses its Linux kernel's actual capabilities, not a
  native Windows sandbox.
- Conversation/trace databases and traffic logs contain sensitive user content.
  Header masking is not payload redaction, and automatic traffic rotation is absent.
- Persistence is asynchronous; abrupt termination can lose unpublished work.
  Recovery does not guarantee rollback of external effects or replay safety.

See [configuration](docs/CONFIGURATION.md), [tool boundaries](docs/TOOLS_AND_MCP.md)
and [deployment](docs/DEPLOYMENT.md). Git ignores common secret/runtime files and
GitHub Actions runs Gitleaks, but these measures do not replace reviewing staged content.
