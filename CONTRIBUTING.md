# Contributing to CCM

Use GitHub issues for reproducible bugs and proposals, and pull requests for
changes. For sensitive reports, follow [SECURITY.md](SECURITY.md).

Discuss substantial behavior or architecture changes before implementation. State
the problem, intended contract, compatibility implications and how it can be
verified. Distinguish proposed behavior from implemented and tested behavior.

## Local checks

Install current stable Rust, the platform C toolchain and Node.js 22+. Use Linux
or Windows through WSL2; macOS has the documented sandbox/runtime limits.

```sh
cargo fmt --check
cargo build --locked
cargo test --locked
node --check web/app.js
node --check web/state.js
node --test web/state.test.mjs
git diff --check
```

Tests use synthetic providers and temporary state; they must not require a personal
provider account, live credentials or a developer's local `ccm.json`. Build the
regular executable before tests because native-helper scenarios re-execute it.
Keep `Cargo.lock` committed for reproducible application dependency resolution.

Update configuration/API documentation alongside contract changes. Add focused
regressions for behavior changes, especially continuation ownership, permissions,
recovery and persistence. Record platform limits rather than treating Linux tests
as macOS/Windows runtime proof. The source supports aarch64 Linux paths, but the
recorded local acceptance environment is x86_64.

Never commit `.env`, client keys, private notes, workstation configuration, runtime
databases or traffic logs. Run a secret scan before sharing a branch:

```sh
gitleaks git . --redact --log-opts="--all"
```

GitHub Actions checks Linux, macOS and secrets on hosted runners. Workflow
configuration is not proof of a successful run; inspect the Actions results for
the relevant commit before claiming remote platform acceptance.

CCM is licensed under [MIT](LICENSE). Contributions must be compatible with that
license; retain applicable third-party notices. Do not add personal contact details
or make release/support commitments on behalf of the maintainer.
