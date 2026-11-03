package inventory

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuthStatusProber_EnrichToolsAndCaching(t *testing.T) {
	now := time.Date(2026, 11, 3, 15, 0, 0, 0, time.UTC)
	var runCount atomic.Int32

	prober := NewAuthStatusProber(30*time.Second, func(ctx context.Context) (map[string]bool, error) {
		return map[string]bool{
			"iazio-harvester": true,
			"iazio-mcp":       true,
		}, nil
	})
	prober.Now = func() time.Time { return now }
	prober.Runner = func(ctx context.Context, binPath string, args ...string) ([]byte, error) {
		runCount.Add(1)
		if len(args) != 3 || args[0] != "auth" || args[1] != "status" || args[2] != "--json" {
			t.Fatalf("unexpected args: %v", args)
		}
		if binPath == "/usr/local/bin/iazio-harvester" {
			return []byte(`{
				"config_path": "/home/debian/.iazio/harvester.json",
				"api_base_url": "https://tian.go.ro/iazio-harvester-api",
				"client_id": "iazio-harvester-cli",
				"refresh_token_present": true,
				"access_token_present": true,
				"token_status": "available"
			}`), nil
		}
		return []byte("unrecognized subcommand"), errors.New("exit status 1")
	}

	input := []Tool{
		{
			Name:      "iazio-harvester",
			Path:      "/usr/local/bin/iazio-harvester",
			Version:   "0.2.0",
			Status:    "OK",
			Installed: true,
		},
		{
			Name:      "iazio-mcp",
			Path:      "/usr/local/bin/iazio-mcp",
			Version:   "0.1.0",
			Status:    "OK",
			Installed: true,
		},
		{
			Name:      "autopilot",
			Path:      "/usr/local/bin/autopilot",
			Version:   "1.0.0",
			Status:    "OK",
			Installed: true,
		},
	}

	enriched := prober.EnrichTools(context.Background(), input)
	if len(enriched) != 3 {
		t.Fatalf("expected 3 tools, got %d", len(enriched))
	}

	// 1. iazio-harvester: enabled + valid JSON
	if !enriched[0].AuthStatusEnabled {
		t.Errorf("expected iazio-harvester AuthStatusEnabled=true")
	}
	if enriched[0].AuthStatus["token_status"] != "available" {
		t.Errorf("expected token_status=available, got %+v", enriched[0].AuthStatus)
	}
	if enriched[0].AuthCheckedAt != "2026-11-03T15:00:00Z" {
		t.Errorf("unexpected AuthCheckedAt: %s", enriched[0].AuthCheckedAt)
	}
	if enriched[0].AuthStatusError != "" {
		t.Errorf("unexpected AuthStatusError: %s", enriched[0].AuthStatusError)
	}

	// 2. iazio-mcp: enabled + error
	if !enriched[1].AuthStatusEnabled {
		t.Errorf("expected iazio-mcp AuthStatusEnabled=true")
	}
	if enriched[1].AuthStatusError == "" {
		t.Errorf("expected AuthStatusError on iazio-mcp when runner fails without valid JSON")
	}

	// 3. autopilot: not enabled
	if enriched[2].AuthStatusEnabled {
		t.Errorf("expected autopilot AuthStatusEnabled=false")
	}
	if len(enriched[2].AuthStatus) != 0 {
		t.Errorf("expected nil AuthStatus on autopilot")
	}

	if got := runCount.Load(); got != 2 {
		t.Fatalf("expected 2 command executions on initial pass, got %d", got)
	}

	// Second call within 30s TTL must use cache and NOT re-execute commands
	now = now.Add(10 * time.Second)
	_ = prober.EnrichTools(context.Background(), input)
	if got := runCount.Load(); got != 2 {
		t.Fatalf("expected cached result without new command executions, got %d", got)
	}

	// Third call after 31s TTL expiry must re-probe
	now = now.Add(25 * time.Second)
	_ = prober.EnrichTools(context.Background(), input)
	if got := runCount.Load(); got != 4 {
		t.Fatalf("expected re-probe after TTL expiry (total 4 executions), got %d", got)
	}
}

func TestAuthStatusProber_ProbeToolWithLeadingSlogLine(t *testing.T) {
	prober := NewAuthStatusProber(30*time.Second, nil)
	prober.Runner = func(ctx context.Context, binPath string, args ...string) ([]byte, error) {
		return []byte("{\"time\":\"2026-11-03T21:40:46Z\",\"level\":\"INFO\",\"msg\":\"swept orphaned shadow copies from prior runs\",\"count\":1}\n{\n  \"config_path\": \"/home/romeo/.iazio/harvester.json\",\n  \"api_base_url\": \"https://tian.go.ro/iazio-harness-api\",\n  \"refresh_token_present\": true,\n  \"token_status\": \"available\"\n}"), nil
	}

	entry := prober.ProbeTool(context.Background(), "/home/romeo/.local/bin/iazio-harvester")
	if entry.AuthStatusError != "" {
		t.Fatalf("expected empty AuthStatusError when leading slog JSON precedes report, got %q", entry.AuthStatusError)
	}
	if entry.AuthStatus["config_path"] != "/home/romeo/.iazio/harvester.json" {
		t.Fatalf("expected config_path from final JSON object, got %+v", entry.AuthStatus)
	}
}

