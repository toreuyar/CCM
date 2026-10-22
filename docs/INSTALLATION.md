# Installation and first request

Use the [README platform instructions](../README.md#build-and-install) for Linux,
Windows through WSL2, and macOS. A current stable Rust toolchain, platform C
compiler/linker, Git and trusted CA certificates are required. SQLite is bundled;
there is no external database service or JavaScript asset build.

## Installation layout

Build with `cargo build --release --locked`, then install `target/release/ccm`
in a directory on `PATH`. Keep a separate writable configuration/data directory
when installing outside the checkout:

```sh
mkdir -p "$HOME/.config/ccm"
chmod 700 "$HOME/.config/ccm"
cp ccm.example.json "$HOME/.config/ccm/ccm.json"
cp .env.example "$HOME/.config/ccm/.env"
chmod 600 "$HOME/.config/ccm/.env"
cd "$HOME/.config/ccm"
```

Edit `.env` with a randomly generated bootstrap key and the provider credentials
referenced by `ccm.json`. `openssl rand -hex 32` produces a suitable random
bootstrap key. Edit the provider model to an ID available to your account.
Then run `ccm` from this directory. The example does not reference external prompt
files; defaults are embedded. If you add prompt paths, make them relative to your
JSON file or absolute.

Default files created in the working directory:

| File | Purpose |
|---|---|
| `portfolio.db` | Definitions, keys, conversations, traces and execution state |
| `portfolio.db.ccm-lock` | Single-writer lock |
| `portfolio.db.ccm-next` | Temporary persistence snapshot when publishing |
| `ccm-traffic.jsonl` | Full traffic journal; no automatic rotation |

Keep the directory writable and private. A copied binary still needs these
runtime resources; installation does not create a service or provision keys.

## Create a client key

The bootstrap credential has administration rights only. Create a separate
client credential for chat and dashboard access. Start the server in one terminal;
in a second terminal, enter its configuration directory and export your trusted
local `.env` settings:

```sh
set -a
. ./.env
set +a
```

The following commands create a private workforce with a client key, save the
raw token to a private file, and print no token. Run them once per intended client;
repeating them creates another private workforce. Use a new destination filename
if `ccm-client.api-key` already exists. The temporary header keeps the credential
out of curl's process arguments.

```sh
umask 077
set -o pipefail
HEADER=$(mktemp)
printf 'Authorization: Bearer %s\n' "$CCM_BOOTSTRAP_ADMIN_KEY" > "$HEADER"
curl --fail-with-body --silent --show-error \
  --header @"$HEADER" --header 'Content-Type: application/json' \
  --data '{"label":"local-client","permissions":{"roster_use":true,"roster_read":true,"roster_write":true,"improvement_contribute":false,"bootstrap_admin":false}}' \
  "http://127.0.0.1:${CCM_PORT:-8080}/api/admin/keys" \
  | jq --exit-status --raw-output '.token // error("No client token returned")' > ccm-client.api-key
rm -f "$HEADER"
chmod 600 ccm-client.api-key
```

Use the saved token in the dashboard sign-in form or your API client's API-key
field. Do not paste it into an issue or commit it. The example disables contribution
to automatic improvement; enable `improvement_contribute` deliberately if wanted.
With `roster_write` and contribution enabled, eligible single-key private
workforces can automatically apply definition improvements. See
[permissions](BEARER_AUTH.md) and [improvement](AUTO_IMPROVEMENT.md).

## Test a chat request

This example reads the client key without putting it in process arguments.
The public alias is `ccm-default` in `ccm.example.json`; use `ccm-local` for the
local-provider example, or your configured alias.

```sh
umask 077
HEADER=$(mktemp)
printf 'Authorization: Bearer %s\n' "$(cat ccm-client.api-key)" > "$HEADER"
curl --fail-with-body --silent --show-error \
  --header @"$HEADER" --header 'Content-Type: application/json' \
  --data '{"model":"ccm-default","stream":false,"messages":[{"role":"user","content":"Hello!"}]}' \
  "http://127.0.0.1:${CCM_PORT:-8080}/v1/chat/completions"
rm -f "$HEADER"
```

Model calls may incur provider charges and take multiple orchestration rounds.
This endpoint is an OpenAI-compatible subset, not a promise of every OpenAI API.
For a non-inference check, request `/v1/models` with the same client key.

## Upgrades and uninstall

Stop/drain the service as described in [deployment](DEPLOYMENT.md), back up its
data/configuration after shutdown, rebuild using the desired source snapshot and
replace the executable. Database compatibility across arbitrary versions is not
guaranteed; do not run an older executable on newer state without a compatible
backup. Verify readiness and client access after restarting.

To uninstall, stop CCM, disable its service if configured, and remove the installed
executable. Retain or deliberately delete the separate configuration/data directory
according to your needs; it may contain credentials and conversations.

## Troubleshooting

| Symptom | Check |
|---|---|
| Compiler/linker not found | Install `build-essential` (Linux/WSL2) or complete `xcode-select --install` (macOS); reopen the terminal after installing Rust. |
| Rust edition/dependency version error | Run `rustup update stable` and use `cargo +stable build --release --locked`. |
| Configuration file missing | Run from the config directory or set `CCM_CONFIG` to the correct JSON path. Explicit missing paths fail startup. |
| API returns 503 | Set a nonempty `CCM_BOOTSTRAP_ADMIN_KEY` before startup; also check whether the service is draining. |
| API returns 401 | Use the raw, unrevoked CCM client key, not a provider key. |
| API returns 403 | Bootstrap keys do not grant chat/roster access; create a client key with the needed permissions. |
| Provider fails or unknown model | Check provider endpoint, credential environment variable and available model ID; public aliases and inference model IDs differ. |
| Port already in use | Stop the conflicting process or set `CCM_PORT`; never run two writers against one database. |
| Shell unavailable | It is disabled by default and needs a workforce grant. Linux confinement also requires supported Landlock/seccomp; WSL2 kernels vary. |
| macOS native-tool isolation | macOS has no sandbox backend. Native children are unconfined by default; explicit required sandboxing is rejected. |
| Windows cannot reach WSL2 server | First test with `curl` inside Ubuntu; verify WSL2 localhost forwarding and firewall/VPN policy. Do not expose the listener broadly just to bypass a configuration problem. |

WSL2 is the documented Windows route; no native Windows executable is provided.
Native macOS runtime validation and WSL2-specific runtime validation remain
separate from Linux x86_64 test results.
