# iazio-agent

User-level supervisor that heartbeats a local tool inventory and starts `iazio-harness`.

```text
go test ./...
go build -o iazio-agent ./cmd/iazio-agent
```

Authorize and token URLs are operator configuration (`IAZIO_AUTH_URL`, `IAZIO_TOKEN_URL`) stored in `~/.iazio/agent.json`. The public client id is `iazio-agent-cli`.

Public modules: `iazio`, `iazio-api`, `iazio-web`, `iazio-harvester`, `iazio-mcp`, `iazio-harness`, `iazio-harness-api`.
