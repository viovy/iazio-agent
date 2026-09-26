# iazio-agent

User-level supervisor that heartbeats a local tool inventory and starts `iazio-harness`.

```text
go test ./...
go build -o iazio-agent ./cmd/iazio-agent
```

Authorize and token URLs are operator configuration (`IAZIO_AUTH_URL`, `IAZIO_TOKEN_URL`) stored in `~/.iazio/agent.json`. The public client id is `iazio-agent-cli`.

`iazio-agent service install` installs a user service that starts at login:

| OS | Mechanism |
| --- | --- |
| macOS | `~/Library/LaunchAgents/io.iazio.iazio-agent.plist` (`RunAtLoad`, `KeepAlive`) in `gui/<uid>`. Listed under Login Items → Allow in the Background. |
| Debian 13 and WSL Debian 13 | `~/.config/systemd/user/iazio-agent.service` (`WantedBy=default.target`) plus `loginctl enable-linger`. WSL without systemd fails and tells you to set `systemd=true` in `/etc/wsl.conf`. |
| Windows | Task Scheduler task `iazio-agent` with an ONLOGON trigger for the current user. |

Logs on macOS go to `~/.iazio/logs/agent.log`.

Public modules: `iazio`, `iazio-api`, `iazio-web`, `iazio-harvester`, `iazio-mcp`, `iazio-harness`, `iazio-harness-api`.
