# Deploying CCM as a service

Build/install and provision credentials using [installation](INSTALLATION.md).
The binary embeds its dashboard and default prompts; it still needs a writable,
private working directory for configuration, database snapshots and traffic.

## Common service settings

- Run as a dedicated non-root user with access only to the intended data/workspace.
- Set an explicit working directory containing `.env` and `ccm.json`, or provide
  environment settings through your service manager. Relative database/traffic paths
  use the working directory; JSON prompt/tool paths use the JSON directory.
- Keep credentials outside the repository and service command arguments.
- Retain loopback binding behind an HTTPS reverse proxy when serving remote clients.
  Configure streaming and idle timeouts for long model calls; do not buffer SSE.
- Allow at least the configured shutdown budget before any service-manager kill.
  `shutdown.supervisor_stopwait_secs` validates the application budget but does not
  configure the service manager itself.
- Arrange log/journal rotation, storage limits and backups separately. Runtime data
  can contain user prompts, tool results and credentials included in message bodies.

## Linux systemd example

Adapt paths and create the `ccm` service user/data directory before installing this
unit. This is a template, not an installed service or an asserted host setup.

```ini
[Unit]
Description=CCM Composite Cognitive Model
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ccm
Group=ccm
WorkingDirectory=/var/lib/ccm
ExecStart=/usr/local/bin/ccm
Restart=on-failure
RestartSec=5
TimeoutStopSec=300
KillSignal=SIGTERM
UMask=0077

[Install]
WantedBy=multi-user.target
```

Put the executable at the configured `ExecStart` path and the protected `.env` /
`ccm.json` files in `/var/lib/ccm`, owned by the service user. Save the unit as
`/etc/systemd/system/ccm.service`, then run:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now ccm
sudo systemctl status ccm
curl --fail http://127.0.0.1:8080/health/ready
```

Do not enable a second service instance against an already-running CCM database.
Host hardening settings can affect native-tool execution; verify the intended
tool/sandbox behavior after changing them.

## macOS and WSL2

On macOS, start in the foreground initially. For launchd, configure the absolute
binary path, `WorkingDirectory`, protected environment and graceful termination
behavior for your user/service account. A launchd service is not supplied or
validated by this repository. Native tools are unconfined by default on macOS.

On WSL2, use the Linux procedure if systemd is enabled in the distribution;
otherwise run CCM in a supervised Linux shell/process manager. Windows shutdown,
WSL shutdown and host sleep may interrupt work; do not assume Linux service
restart settings cover host/VM termination.

## Readiness and planned restart

1. Identify the current process and service manager; check `/health/live` and
   `/health/ready`. Read authenticated `/api/admin/service/status`.
2. Require healthy persistence and a quiescent starting point before an elective
   deployment. Do not drain merely to discover whether ongoing work can stop.
3. Send authenticated `POST /api/admin/service/drain` once; expected status is 202.
   If the response is ambiguous, read status before deciding what to do next.
4. Poll status under an explicit finite deadline (for example, 120 seconds).
   Require `planned_restart.safe_to_stop=true` and no blockers; inspect async
   quiescence, pending work, persistence errors and matching persisted/commit
   generations. Drain is irreversible until restart.
5. Stop through the correct manager, wait for process exit and released database
   locks, then back up data and replace the verified executable.
6. Start once. Verify the actual running executable, readiness, authenticated client
   access and healthy persistence. Logs and old recorded PIDs do not identify the
   current process reliably.

If the deadline expires, do not automatically force-kill active work. The process
may remain alive but non-accepting; report that state accurately. A successful
restart does not prove uncertain external effects were rolled back.

## API access

Use a CCM client key for chat/roster operations and a separate bootstrap/admin key
for administration. Raw issued client tokens are returned only once; store them
securely. Rotation requires revoking/reissuing client keys, while changing the
bootstrap environment value takes effect on startup. See [authentication](BEARER_AUTH.md).
