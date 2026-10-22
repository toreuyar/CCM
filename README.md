# CCM — Composite Cognitive Model

CCM composes language models, specialist agents and workflows behind an
OpenAI-compatible chat-completions interface. **Foreman** coordinates work and
composes responses; reusable specialists and bounded workflow graphs execute it.

The Rust server includes a web dashboard, bearer-key/workforce isolation,
Streamable HTTP MCP, client-tool continuations, background operations and
definition-level improvement. This is orchestration, not model-weight training
or a claim of superior answer quality.

## Platform support

| Platform | Installation route | Verification and limitations |
|---|---|---|
| Linux | Native Rust build | Linux x86_64 is locally tested. Native sandbox requires Landlock ABI 6+ and seccomp on supported x86_64/aarch64 targets. |
| Windows | **WSL2 with Ubuntu** | Use the Linux instructions inside WSL2. Native Windows builds are not supported. WSL2/kernel sandbox availability must be checked locally. |
| macOS | Native Rust build | Build instructions and a GitHub Actions job are provided; native macOS execution has not been verified in the Linux development environment. Shell/MCP child sandboxing is unavailable; enabled native children run unconfined by default. |

## Build and install

Install Git, a current stable [Rust toolchain](https://rustup.rs/) with Cargo,
and the platform C compiler/linker. SQLite is bundled and compiled during the build.
Node.js is needed only for frontend tests, not to build or run the server.

### Linux

Ubuntu/Debian:

```sh
sudo apt-get update
sudo apt-get install -y build-essential pkg-config git curl ca-certificates openssl jq
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
```

Fedora: install `gcc gcc-c++ make pkgconf-pkg-config git curl openssl jq`
with `dnf`, then install Rust as above.

### Windows (WSL2)

In **administrator PowerShell**, install WSL2:

```powershell
wsl --install -d Ubuntu
wsl --set-default-version 2
wsl --list --verbose
```

Restart Windows if prompted, open Ubuntu, create its Linux user, and follow the
Linux instructions. Confirm Ubuntu is version 2 in `wsl --list --verbose`; an
existing version-1 distribution can be converted with `wsl --set-version Ubuntu 2`.
Keep the checkout in the Linux filesystem, such as `~/src/ccm`, rather than
`/mnt/c`. Run all following commands in Ubuntu. Windows browsers can normally
reach the dashboard at `http://localhost:8080` through WSL localhost forwarding.
See [installation troubleshooting](docs/INSTALLATION.md#troubleshooting).

### macOS

Install Apple's Command Line Tools and complete their installer:

```sh
xcode-select --install
```

Then install Rust:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
```

The setup examples also use `curl`, `openssl` and `jq`. Check they are
available; install jq through your usual macOS package manager
(for example, `brew install jq`). No Homebrew OpenSSL linkage is required by
CCM: its HTTP client uses Rustls.

### All platforms: compile and install the executable

Copy the HTTPS or SSH clone URL from this GitHub repository's **Code** menu and
substitute it below:

```sh
git clone <your-GitHub-clone-URL> ccm
cd ccm
rustup update stable
cargo +stable build --release --locked
mkdir -p "$HOME/.local/bin"
install -m 755 target/release/ccm "$HOME/.local/bin/ccm"
export PATH="$HOME/.local/bin:$PATH"
```

Add that `PATH` setting to your shell profile if necessary. Keep configuration
and data in a writable directory; installing the executable does not install
configuration or create an OS service. The dashboard and default prompts are
embedded in the binary.

## Configure and run

From the checkout, create your **local, Git-ignored** configuration:

```sh
umask 077
cp ccm.example.json ccm.json
cp .env.example .env
openssl rand -hex 32
```

Edit `.env`: paste the generated value into `CCM_BOOTSTRAP_ADMIN_KEY` and set
`OLLAMA_CLOUD_API_KEY` to your own provider key. These are **different credentials**.
Never use a provider key as a CCM client key. Select a model available to your
provider account by editing `models.ccm-default.model` in `ccm.json`; the included
model is an example, not an availability guarantee.

For a local Ollama server instead, use:

```sh
cp examples/ccm.local-provider.json ccm.json
```

Install/start Ollama separately and pull the model named in that file. On WSL2,
run it inside the distribution or explicitly configure a reachable host endpoint.
Its default
endpoint is `http://127.0.0.1:11434/v1`; no provider API key is configured. Set the
bootstrap credential in `.env` in either case.

Start CCM **from the directory containing `.env` and `ccm.json`**:

```sh
ccm
```

In another terminal:

```sh
curl --fail http://127.0.0.1:8080/health/ready
```

Open `http://127.0.0.1:8080`. Before using the dashboard or chat API,
[create a client key](docs/INSTALLATION.md#create-a-client-key). The bootstrap
key administers accounts but does not itself have chat/roster permissions.
Without `CCM_BOOTSTRAP_ADMIN_KEY`, protected endpoints return **503**.

The server reads process environment first and uses `.env` to fill unset
variables. JSON changes require a restart. See the complete
[configuration reference](docs/CONFIGURATION.md) for precedence, provider/model
overrides, tool permissions, sandbox behavior, timeouts, memory, improvement,
shutdown, telemetry and examples.

## Documentation

- [Installation, client setup and troubleshooting](docs/INSTALLATION.md)
- [Complete configuration reference](docs/CONFIGURATION.md)
- [Documentation index](docs/README.md)
- [Authentication and permissions](docs/BEARER_AUTH.md)
- [Models and providers](docs/MODEL_CONFIGURATION.md)
- [MCP server](docs/MCP_SERVER.md) · [Tools, MCP clients and plugins](docs/TOOLS_AND_MCP.md)
- [Operations and recovery](docs/OPERATIONS.md) · [Service deployment](docs/DEPLOYMENT.md)
- [Architecture paper](docs/paper/CCM.md) — its dated evidence cutoff is distinct
  from the current implementation and verification results.

## Development and verification

```sh
cargo fmt --check
cargo build --locked
cargo test --locked
node --test web/state.test.mjs
```

Use Node.js 22+ for frontend tests. See [verification](docs/TEST_RESULTS.md)
and [contributing](CONTRIBUTING.md). GitHub Actions runs Linux/macOS checks and
secret scanning on hosted runners.

## Data and security

CCM persists conversations, tool inputs/results and execution traces locally.
The traffic journal can contain full prompt/response bodies, including secrets
you put inside them; authentication-header masking is not general redaction.
Use trusted storage, configure retention/rotation externally, and never publish
runtime data. Default listening is loopback-only. Use HTTPS at a reverse proxy
before exposing bearer-authenticated APIs over a network.

See [security guidance](SECURITY.md). The software is under the
[MIT license](LICENSE), © 2026 **TÖRE ÇAĞRI UYAR**.
