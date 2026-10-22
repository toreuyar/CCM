# Bearer authentication and workforce scope

Bearer-key authentication and workforce isolation protect `/v1`, `/api` and
`/mcp`. The static dashboard remains public; its data APIs require a key.

## Request authentication

Every request whose path is `/v1`, `/v1/...`, `/api`, `/api/...`, or `/mcp` must include:

```http
Authorization: Bearer <key>
```

Missing, malformed, unknown, or revoked credentials receive `401 Unauthorized`.
An authenticated key lacking the route permission receives `403 Forbidden`. If the
service starts without bootstrap configuration, `/v1`, `/api`, and `/mcp` instead return
`503 Service Unavailable` for every request, even if a valid stored key is supplied.
Key tokens are high-entropy generated values; only a SHA-256 digest is stored. The raw
token is returned once by key creation and is not included in key-list responses.

Static dashboard HTML and assets (`/` and `/assets/...`) are not protected by this
middleware. Their API calls under `/api` are protected. MCP initialization and
subsequent requests are protected at `/mcp`; external APIs must be enabled by
`CCM_BOOTSTRAP_ADMIN_KEY`.

## Browser bearer-key flow

Opening `/` serves the dashboard and its access-key sign-in form. The browser keeps
the supplied key in memory while connecting, then saves it in tab-scoped
`sessionStorage` under `ccm-api-token` for reloads in that tab. Dashboard API
requests attach it as `Authorization: Bearer <key>`; it is not placed in a URL or
sent as a query parameter. Logging out clears the saved key. During an established
dashboard session, a rejected/revoked key (`401`) clears it and returns to sign-in;
a permission failure (`403`) reports access denied for that request. Initial sign-in
can also be rejected if the key lacks permission for the dashboard's initial reads.
When the tab's browser session ends, its `sessionStorage` entry is removed.

This browser flow does not elevate key permissions: the dashboard can display only
data and actions already allowed to that key. Use HTTPS when sending bearer keys
outside a trusted local development connection.

## Bootstrap and administration

Set `CCM_BOOTSTRAP_ADMIN_KEY` in the service environment to seed the bootstrap
administrator credential and enable external `/v1`, `/api`, and `/mcp` access. If it is absent
or empty, those routes fail closed with `503`, including for previously issued keys.
Setting it on startup replaces the stored bootstrap-key digest, so changing the value
rotates that credential. Unsetting it blocks access but does **not** revoke credentials
stored in the database; if the environment variable is configured again later, those
keys remain usable unless explicitly revoked.

The seeded bootstrap key has only `bootstrap_admin`; it cannot use or read a roster
unless separately provisioned with those permissions. The bootstrap key itself cannot
be revoked through the API. A bootstrap administrator may explicitly grant
`bootstrap_admin` to another created key.

All `/api/admin` routes require `bootstrap_admin`:

| Method and path | Operation |
|---|---|
| `GET /api/admin/workforces` | List workforces and active-key counts |
| `POST /api/admin/workforces` | Create a workforce |
| `GET /api/admin/keys` | List key metadata and permissions; tokens are omitted |
| `POST /api/admin/keys` | Create a key; returns its raw token once |
| `DELETE /api/admin/keys/{id}` | Revoke a key; the record is retained |

Workforce creation accepts `name` and optional `visibility` (`private` by default;
otherwise `shared`). Key creation accepts `label`, optional `workforce_id`, optional
`workforce_name`, and optional `permissions`. If `workforce_id` is omitted, a new
private workforce is created; its name is `workforce_name` or, by default, the key
label. Supplying an existing `workforce_id` attaches the key to that workforce and
marks it shared. Names and labels must be nonblank and at most 120 bytes.

Every permission defaults to `false` when omitted:

```json
{
  "label": "automation",
  "permissions": {
    "roster_use": true,
    "roster_read": true,
    "roster_write": false,
    "improvement_contribute": false,
    "bootstrap_admin": false
  }
}
```

There is no key update endpoint for permissions, labels, or workforce assignment.
To change these properties, revoke the key and create a replacement with the desired
permissions and workforce. The key-list endpoint includes revoked records with their
`revoked_ms`; it never returns their former tokens.

## Permissions

The middleware applies the following route-level checks:

| Permission | Route access |
|---|---|
| `roster_use` | All `/v1` routes and `POST /api/chat` |
| `roster_read` | `GET`/`HEAD` on non-admin `/api` routes other than `/api/chat` |
| `roster_write` | Other non-admin `/api` methods, plus roster-changing runtime operations |
| `improvement_contribute` | Required to submit improvement evidence and for source-owned HTTP suggestion declassification |
| `bootstrap_admin` | All `/api/admin` workforce/key management routes |

`/mcp` requires an authenticated key at the transport layer; each advertised MCP
tool is checked again against its operation-specific permission. In particular,
the one-time `tool_approvals.*` actions, including cancellation, require
`roster_use` and are limited to the current invoker's own approval. They do not
require `roster_write` and do not alter embedded-tool policy. The direct
`/api/tool-approvals/{id}` GET/DELETE and approve/deny/result routes use the same
current-invoker rule; unlike general async-operation cancellation, approval
cancellation is not roster mutation.

The permissions are independent. In particular, contributing improvement evidence
does not grant shared-roster write authority. A key with contribution permission but
without roster-write permission can contribute private evidence, but its job is not
automatically applied to shared definitions. Roster-write permission alone does not
authorize evidence contribution.

`GET /api/improvements` permits `roster_read` or `improvement_contribute` to read the
caller's own suggestions. Cross-key pending suggestions are included only when the
caller has both `roster_read` and `roster_write`. Thus `roster_read` alone does not
expose another key's proposal, a contributor without `roster_read` retains only its
own private view, and `roster_write` without `roster_read` cannot list shared
proposals. Explicit application separately requires `roster_write`.

For a generated `revise` suggestion, the source key sees its own private proposed
replacement. Other keys cannot see that replacement until the source explicitly
declassifies it; before then, an eligible cross-key listing shows only sanitized
metadata indicating declassification is required. The implemented HTTP action
`POST /api/improvements/{id}/declassify` requires `improvement_contribute` and source
ownership. It records the source action and does not apply the suggestion or mutate a
definition. MCP source-key scope, model-identity, heartbeat, and run-level final-
publication checks across the complete request chain have been accepted in independent
review. Declassification releases visibility only; it does not apply the proposal.

## Workforce and key data boundaries

Keys are the private identity boundary; workforces are the shared roster boundary.

- **Shared within one workforce:** active agent/workflow definitions, definition
  versions, workforce usage aggregates, shared workforce-change notifications, and
  sanitized pending improvement proposals visible only to keys with both
  `roster_read` and `roster_write`. Generated replacement text is shared only after
  the source key explicitly declassifies it.
- **Private to one key within that workforce:** runs and traces, conversations,
  continuations, user preferences, model snapshots, and improvement/contribution
  evidence and jobs. Raw suggestion verdicts/evidence and source identifiers remain
  private; applying a shared proposal creates an audit owned by the applying key.
- **Aggregated:** workforce usage counts combine activity across keys sharing that
  workforce. They do not expose another key's run or conversation contents.

Keys in different workforces cannot read or mutate one another's roster. Keys in the
same workforce share roster definitions and usage, but do not share request history,
continuation ownership, or preferences.

Agent consolidation internally preserves the old executor snapshot in affected legacy
paused runs across the workforce so those runs remain resumable. The snapshots stay
within each run's owner scope; the applying key is not given another key's private run
state or evidence.

## Existing data and migration

Initialization adds the workforce/key administration tables and nullable ownership
columns to the existing schema. It is additive and restart-safe: existing rows are
left unscoped (`NULL` ownership), not assigned to a workforce or key. Scoped external
queries require an exact workforce/key match, so unscoped legacy rows remain preserved
but are not exposed to scoped external keys. There is no automatic legacy-data
backfill or ownership inference.

Review schema compatibility and keep a stopped-state backup before upgrading an
existing deployment. Test fixtures do not establish compatibility for every database.

## Known compatibility and operational limits

- Key permission/label/workforce updates and workforce deletion are not implemented.
- Removing `CCM_BOOTSTRAP_ADMIN_KEY` closes the external routes but does not revoke
  already-issued keys; use the admin revoke operation for explicit revocation.
- MCP sessions expire after eight idle hours and are bound to both key and workforce.
  A session cannot be reused with another key, and a revoked key cannot keep using
  its existing session.
