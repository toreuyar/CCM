# Verification

Verification is performed against a clean export of the public source tree, with
no developer `.env`, `ccm.json`, internal notes, workstation configuration or live
application database. Synthetic providers and disposable state are used for tests.

## Reproduce

```sh
cargo fmt --check
cargo build --locked
cargo test --locked
cargo build --release --locked
node --check web/app.js
node --check web/state.js
node --test web/state.test.mjs
gitleaks git . --redact --log-opts="--all"
```

Use a current stable Rust toolchain, a platform C compiler/linker and Node.js 22+.
Build the normal executable before tests: native-helper scenarios re-execute it.
Configuration examples are parsed by the regression suite. Local API smoke tests
should verify readiness, key provisioning, permissions and model listing without
requiring paid inference.

## Publication acceptance

On Linux x86_64, the clean public export passed formatting, the debug build,
374 Rust unit/scenario tests and one forced-shutdown integration test. Frontend
syntax checks and all 13 frontend tests passed. Gitleaks 8.30.1 reported zero
findings in the public snapshot; a separate comparison against seven locally
available credentials found no matches. Relative Markdown links resolve within
the public snapshot. The optimized release build passed. The documented `curl`
setup passed a disposable-service smoke check: dotenv loading, bootstrap/client
key separation, client provisioning, the configured public model alias, 401/403
authentication boundaries and clean shutdown. No provider inference was invoked.

## Limits

- Linux x86_64 is the local validation platform. Linux aarch64, native macOS and
  WSL2-specific behavior are not established by these Linux results.
- Native Windows is not supported; Windows installation uses WSL2.
- CI configuration is not evidence that a GitHub Actions workflow or macOS job
  has executed. Check the results for the actual published commit.
- Tests use controlled fixtures; they do not benchmark intelligence, task success,
  provider quality, cost or arbitrary third-party model compatibility.
- Required native sandbox behavior depends on architecture and kernel features;
  configured mode alone is not runtime proof of confinement.
- Figure structural validation and XML parsing are separate from browser/perceptual
  acceptance; see [figure verification](paper/FIGURE_VERIFICATION.md).
- Secret scans detect known patterns and compared credentials; zero findings are
  not an absolute guarantee about every possible unknown secret.
