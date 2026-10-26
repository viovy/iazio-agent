package supervisor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type mockDriftFetcher struct {
	manifest *FleetManifest
	err      error
}

func (m *mockDriftFetcher) FetchManifest(ctx context.Context) (*FleetManifest, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.manifest, nil
}

func TestDrift_NormalizeVersion(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"0.0.492", "0.0.492"},
		{"v0.0.492", "0.0.492"},
		{"V0.0.492\n", "0.0.492"},
		{" 1.2.3 ", "1.2.3"},
		{"v1.2.3-rc1", "1.2.3-rc1"},
	}

	for _, c := range cases {
		got := NormalizeVersion(c.in)
		if got != c.want {
			t.Errorf("NormalizeVersion(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestDrift_EvaluateDrift_CleanAndMismatch(t *testing.T) {
	manifest := &FleetManifest{
		Tools: []ManifestTool{
			{Name: "autopilot", BinaryName: "autopilot", TargetVersion: "0.0.492"},
			{Name: "iazio-agent", BinaryName: "iazio-agent", TargetVersion: "0.0.492"},
		},
	}

	installed := map[string]string{
		"/bin/autopilot":   "v0.0.492",
		"/bin/iazio-agent": "0.0.492",
	}
	inspector := func(path string) string {
		return installed[path]
	}

	// 1. Both up to date -> no drift
	drift, err := EvaluateDrift(manifest, inspector, "/bin", nil, "linux")
	if err != nil || drift {
		t.Fatalf("expected no drift, got drift=%v, err=%v", drift, err)
	}

	// 2. Strict semver equality: 0.0.49 must NOT match 0.0.492 (F4)
	installed["/bin/autopilot"] = "0.0.49"
	drift, err = EvaluateDrift(manifest, inspector, "/bin", nil, "linux")
	if err != nil || !drift {
		t.Fatalf("expected drift on version mismatch 0.0.49 vs 0.0.492, got drift=%v, err=%v", drift, err)
	}

	// 3. Filtered check on iazio-agent only (which is still 0.0.492) -> no drift
	drift, err = EvaluateDrift(manifest, inspector, "/bin", []string{"iazio-agent"}, "linux")
	if err != nil || drift {
		t.Fatalf("expected no drift when checking only iazio-agent, got drift=%v, err=%v", drift, err)
	}

	// 4. Missing binary -> drift
	delete(installed, "/bin/autopilot")
	drift, err = EvaluateDrift(manifest, inspector, "/bin", []string{"autopilot"}, "linux")
	if err != nil || !drift {
		t.Fatalf("expected drift for missing binary, got drift=%v, err=%v", drift, err)
	}

	// 5. "dev" version equality trap: even if both installed and target say "dev", must drift
	installed["/bin/autopilot"] = "dev"
	manifest.Tools[0].TargetVersion = "dev"
	drift, err = EvaluateDrift(manifest, inspector, "/bin", []string{"autopilot"}, "linux")
	if err != nil || !drift {
		t.Fatalf("expected drift for dev version equality trap, got drift=%v, err=%v", drift, err)
	}
}

func TestDrift_EvaluateDrift_LatestResolution(t *testing.T) {
	// Tool has TargetVersion="latest", but LatestAvailableVersion="0.0.492" (F3)
	manifest := &FleetManifest{
		Tools: []ManifestTool{
			{
				Name:                   "iazio-mcp",
				BinaryName:             "iazio-mcp",
				TargetVersion:          "latest",
				LatestAvailableVersion: "0.0.492",
				Tier:                   "baseline",
			},
		},
	}

	installed := map[string]string{
		"/bin/iazio-mcp": "0.0.482",
	}
	inspector := func(path string) string {
		return installed[path]
	}

	drift, err := EvaluateDrift(manifest, inspector, "/bin", nil, "linux")
	if err != nil || !drift {
		t.Fatalf("expected drift when latest resolution 0.0.492 exceeds 0.0.482, got drift=%v, err=%v", drift, err)
	}

	installed["/bin/iazio-mcp"] = "0.0.492"
	drift, err = EvaluateDrift(manifest, inspector, "/bin", nil, "linux")
	if err != nil || drift {
		t.Fatalf("expected no drift when installed version matches latest resolution, got drift=%v, err=%v", drift, err)
	}
}

func TestDrift_CheckDrift_FailOpenOnError(t *testing.T) {
	// Error on fetch must return non-nil error so eagerupdater fails-open and proceeds (F3)
	fetcher := &mockDriftFetcher{
		err: errors.New("network timeout 504"),
	}
	drift, err := CheckDrift(context.Background(), fetcher, func(p string) string { return "1.0.0" }, "/bin", nil, "linux")
	if err == nil {
		t.Fatalf("expected non-nil error on fetch failure (F3), got nil")
	}
	if drift {
		t.Fatalf("expected drift=false on error return")
	}
}

func TestDrift_HTTPDriftFetcher_E2E(t *testing.T) {
	manifestPayload := `{
		"tools": [
			{"name": "autopilot", "binary_name": "autopilot", "target_version": "0.0.492"}
		]
	}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/fleet/manifest" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(manifestPayload))
	}))
	defer srv.Close()

	fetcher := &HTTPDriftFetcher{
		Client:  srv.Client(),
		API:     srv.URL,
		Profile: "generic",
		OS:      "linux",
		Arch:    "amd64",
	}

	m, err := fetcher.FetchManifest(context.Background())
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if len(m.Tools) != 1 || m.Tools[0].TargetVersion != "0.0.492" {
		t.Fatalf("unexpected fetched manifest: %+v", m)
	}
}

func TestHTTPDriftFetcher_TTLCache(t *testing.T) {
	// L3: Verify that HTTPDriftFetcher caches manifest across TTL and does not cache errors
	var callCount int
	var failNext bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if failNext {
			http.Error(w, "server error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tools":[{"name":"cached-tool","target_version":"1.0.0"}]}`))
	}))
	defer srv.Close()

	fetcher := &HTTPDriftFetcher{
		API:     srv.URL,
		Client:  srv.Client(),
		Profile: "generic",
	}

	// 1. First fetch: network request succeeds
	m1, err := fetcher.FetchManifest(context.Background())
	if err != nil || m1 == nil || callCount != 1 {
		t.Fatalf("expected initial fetch to hit server, count=%d err=%v", callCount, err)
	}

	// 2. Second fetch within TTL: served from memory cache without network hit
	m2, err := fetcher.FetchManifest(context.Background())
	if err != nil || m2 == nil || callCount != 1 {
		t.Fatalf("expected second fetch to hit cache, count=%d err=%v", callCount, err)
	}

	// 3. Expiration: fast-forward expiresAt
	fetcher.mu.Lock()
	fetcher.expiresAt = time.Now().Add(-1 * time.Second)
	fetcher.mu.Unlock()

	// 4. Third fetch after expiration: fetches fresh from server
	m3, err := fetcher.FetchManifest(context.Background())
	if err != nil || m3 == nil || callCount != 2 {
		t.Fatalf("expected third fetch after TTL to hit server, count=%d err=%v", callCount, err)
	}

	// 5. Error path: errors are not cached
	fetcher.mu.Lock()
	fetcher.expiresAt = time.Now().Add(-1 * time.Second)
	fetcher.mu.Unlock()
	failNext = true

	_, errFail := fetcher.FetchManifest(context.Background())
	if errFail == nil {
		t.Fatalf("expected error on failure")
	}

	// Next immediate call when server recovers should succeed (not serving error)
	failNext = false
	m4, errRecover := fetcher.FetchManifest(context.Background())
	if errRecover != nil || m4 == nil {
		t.Fatalf("expected recovery after server error, err=%v", errRecover)
	}
}
