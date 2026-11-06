package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestStatusRedactsToken(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.json")
	body := []byte(`{"refresh_token":"secret-value","auth_url":"https://auth.example/a","token_url":"https://auth.example/t"}`)
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := ReadStatus(path)
	if err != nil {
		t.Fatal(err)
	}
	text := FormatStatus(st)
	if !st.HasRefreshToken || contains(text, "secret-value") {
		t.Fatal(text)
	}
}

func TestLoginNoninteractive(t *testing.T) {
	if err := LoginAllowed(true, false); err == nil {
		t.Fatal("expected closed")
	}
	if err := LoginAllowed(false, false); err != nil {
		t.Fatal(err)
	}
}

func TestNoninteractiveIgnoresGUI(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{name: "off", want: false},
		{name: "flag", env: map[string]string{"IAZIO_AGENT_NONINTERACTIVE": "1"}, want: true},
		{name: "invocation", env: map[string]string{"INVOCATION_ID": "abc"}, want: true},
		{name: "gui", env: map[string]string{"XPC_SERVICE_NAME": "gui"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Noninteractive(func(k string) string { return tt.env[k] }); got != tt.want {
				t.Fatalf("got %v", got)
			}
		})
	}
}

func TestLoginSkipsBrowserWhenNoninteractive(t *testing.T) {
	opened := false
	err := Login(t.Context(), LoginOptions{
		Getenv: func(k string) string {
			if k == "IAZIO_AGENT_NONINTERACTIVE" {
				return "1"
			}
			if k == "XPC_SERVICE_NAME" {
				return "gui"
			}
			return ""
		},
		OpenBrowser: func(string) error {
			opened = true
			return nil
		},
		HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("network")
			return nil, errors.New("network")
		})},
	})
	if !errors.Is(err, ErrNoninteractive) || opened {
		t.Fatal(err)
	}
}

func TestLoginForegroundUsesClientID(t *testing.T) {
	var sawClient bool
	mux := http.NewServeMux()
	mux.HandleFunc("/device", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("client_id") == ClientID {
			sawClient = true
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code":               "dev-code",
			"verification_uri_complete": "https://example.test/device",
			"interval":                  0,
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("client_id") != ClientID {
			t.Errorf("client id %s", r.Form.Get("client_id"))
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"refresh_token": "secret-value"})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.json")
	if err := os.WriteFile(filepath.Join(dir, "harvester.json"), []byte(`{"refresh_token":"from-other-tool"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	opened := false
	err := Login(t.Context(), LoginOptions{
		Getenv: func(k string) string {
			switch k {
			case "IAZIO_AGENT_CONFIG":
				return cfgPath
			case "IAZIO_DEVICE_AUTH_URL":
				return srv.URL + "/device"
			case "IAZIO_TOKEN_URL":
				return srv.URL + "/token"
			case "XPC_SERVICE_NAME":
				return "gui"
			default:
				return ""
			}
		},
		OpenBrowser: func(raw string) error {
			opened = true
			if raw != "https://example.test/device" {
				t.Fatal(raw)
			}
			return nil
		},
	})
	if err != nil || !opened || !sawClient {
		t.Fatal(err)
	}
	st, err := ReadStatus(cfgPath)
	if err != nil || !st.HasRefreshToken {
		t.Fatal(err)
	}
	text := FormatStatus(st)
	if contains(text, "secret-value") || contains(text, "from-other-tool") {
		t.Fatal(text)
	}
}

func TestEndpointRequired(t *testing.T) {
	t.Setenv("IAZIO_AUTH_URL", "")
	t.Setenv("IAZIO_TOKEN_URL", "")
	if _, _, err := Endpoint(Config{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestConfigPathAndStatus(t *testing.T) {
	tmpDir := t.TempDir()
	p := ConfigPathFrom(tmpDir, func(string) string { return "" })
	if !strings.Contains(p, "agent.json") {
		t.Fatalf("unexpected ConfigPathFrom: %s", p)
	}

	pEnv := ConfigPathFrom("", func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return "/custom/path/agent.json"
		}
		return ""
	})
	if pEnv != "/custom/path/agent.json" {
		t.Fatalf("unexpected pEnv: %s", pEnv)
	}

	// Corrupted status JSON
	badFile := filepath.Join(tmpDir, "bad.json")
	_ = os.WriteFile(badFile, []byte("{invalid-json"), 0600)
	if _, err := ReadStatus(badFile); err == nil {
		t.Fatal("expected error on corrupted JSON")
	}

	// ReadStatus on directory (read error that is not NotExist)
	if _, err := ReadStatus(tmpDir); err == nil {
		t.Fatal("expected error on reading directory as status file")
	}

	// Default ConfigPath() call
	_ = ConfigPath()
}

func TestDeviceAuthURL(t *testing.T) {
	cfg := Config{DeviceURL: "https://auth.example/dev"}
	u := DeviceAuthURL(cfg, func(k string) string {
		if k == "IAZIO_DEVICE_AUTH_URL" {
			return "https://override.example/dev"
		}
		return ""
	})
	if u != "https://override.example/dev" {
		t.Fatalf("expected override URL, got %s", u)
	}

	uDefault := DeviceAuthURL(cfg, nil)
	if uDefault != "https://auth.example/dev" {
		t.Fatalf("expected cfg DeviceURL, got %s", uDefault)
	}
}

func TestOpenBrowserValidation(t *testing.T) {
	// Invalid URL
	if err := OpenBrowser("ht tp://invalid url"); err == nil {
		t.Fatal("expected error on malformed URL")
	}

	// Unsupported scheme
	if err := OpenBrowser("ftp://example.com"); err == nil {
		t.Fatal("expected error on unsupported scheme")
	}
}

func TestLoginErrors(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "agent.json")

	// Missing endpoints
	err := Login(t.Context(), LoginOptions{
		Getenv: func(k string) string {
			if k == "IAZIO_AGENT_CONFIG" {
				return cfgPath
			}
			return ""
		},
	})
	if err == nil || !strings.Contains(err.Error(), "auth endpoints are not configured") {
		t.Fatalf("expected not configured error, got: %v", err)
	}

	// Server error 500 on device start
	srvErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server error", http.StatusInternalServerError)
	}))
	defer srvErr.Close()

	err = Login(t.Context(), LoginOptions{
		Getenv: func(k string) string {
			switch k {
			case "IAZIO_AGENT_CONFIG":
				return cfgPath
			case "IAZIO_DEVICE_AUTH_URL":
				return srvErr.URL
			case "IAZIO_TOKEN_URL":
				return srvErr.URL
			default:
				return ""
			}
		},
		HTTP: srvErr.Client(),
	})
	if err == nil {
		t.Fatal("expected error on 500 response")
	}

	// Denied device flow
	mux := http.NewServeMux()
	mux.HandleFunc("/device", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"device_code":               "dev-code",
			"verification_uri_complete": "https://example.test/device",
			"interval":                  0,
		})
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "access_denied"})
	})
	srvDenied := httptest.NewServer(mux)
	defer srvDenied.Close()

	err = Login(t.Context(), LoginOptions{
		Getenv: func(k string) string {
			switch k {
			case "IAZIO_AGENT_CONFIG":
				return cfgPath
			case "IAZIO_DEVICE_AUTH_URL":
				return srvDenied.URL + "/device"
			case "IAZIO_TOKEN_URL":
				return srvDenied.URL + "/token"
			default:
				return ""
			}
		},
		OpenBrowser: func(string) error { return nil },
		HTTP:        srvDenied.Client(),
	})
	if err == nil || !strings.Contains(err.Error(), "denied") {
		t.Fatalf("expected denied error, got: %v", err)
	}
}

func TestStartDeviceAndPollTokenEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. startDevice missing device code
	srvNoCode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"device_code": ""})
	}))
	defer srvNoCode.Close()

	_, err := startDevice(ctx, srvNoCode.Client(), srvNoCode.URL)
	if err == nil || !strings.Contains(err.Error(), "no device code") {
		t.Fatalf("expected no device code error, got %v", err)
	}

	// 2. pollToken slow_down (returns pending=true)
	srvSlow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "slow_down"})
	}))
	defer srvSlow.Close()

	tok, pending, err := pollToken(ctx, srvSlow.Client(), srvSlow.URL, "dev-code")
	if err != nil || tok != "" || !pending {
		t.Fatalf("expected pending=true on slow_down: tok=%q pending=%v err=%v", tok, pending, err)
	}

	// 3. load error on corrupted file
	tmpDir := t.TempDir()
	badFile := filepath.Join(tmpDir, "corrupted.json")
	_ = os.WriteFile(badFile, []byte("not-json"), 0644)
	if _, err := load(badFile); err == nil {
		t.Fatal("expected error on load corrupted file")
	}

	// 4. save error on invalid path
	if err := save("/dev/null/forbidden/file.json", Config{}); err == nil {
		t.Fatal("expected error on save invalid path")
	}
}

func TestResolveEndpointsFromAPIBaseURL(t *testing.T) {
	cfg := Config{APIBaseURL: "https://tian.go.ro/iazio-harness-api"}
	authURL, tokenURL, deviceURL := ResolveEndpoints(cfg, "", nil)
	if authURL != "https://tian.go.ro/hydra/oauth2/auth" {
		t.Fatalf("unexpected authURL: %s", authURL)
	}
	if tokenURL != "https://tian.go.ro/hydra/oauth2/token" {
		t.Fatalf("unexpected tokenURL: %s", tokenURL)
	}
	if deviceURL != "https://tian.go.ro/hydra/oauth2/fallbacks/device/code" {
		t.Fatalf("unexpected deviceURL: %s", deviceURL)
	}
}

func TestLoginPKCELoopbackFlow(t *testing.T) {
	var sawPKCE bool
	mux := http.NewServeMux()
	mux.HandleFunc("/hydra/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("grant_type") == "authorization_code" &&
			r.Form.Get("client_id") == ClientID &&
			r.Form.Get("code") == "pkce-code-123" &&
			r.Form.Get("code_verifier") != "" {
			sawPKCE = true
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "acc-pkce-token",
			"refresh_token": "ref-pkce-token",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.json")
	if err := os.WriteFile(cfgPath, []byte(`{"api_base_url":"`+srv.URL+`/iazio-harness-api"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	err := Login(t.Context(), LoginOptions{
		LoopbackAddr: "127.0.0.1:0",
		Getenv: func(k string) string {
			if k == "IAZIO_AGENT_CONFIG" {
				return cfgPath
			}
			return ""
		},
		HTTP: srv.Client(),
		OpenBrowser: func(raw string) error {
			// Parse the redirect_uri and state from the authorize URL and simulate browser callback
			req, err := http.NewRequest(http.MethodGet, raw, nil)
			if err != nil {
				return err
			}
			redirectURI := req.URL.Query().Get("redirect_uri")
			state := req.URL.Query().Get("state")
			scope := req.URL.Query().Get("scope")
			if !strings.Contains(scope, "harness:write") {
				t.Errorf("expected harness:write in scope, got %q", scope)
			}
			go func() {
				cbResp, cbErr := http.Get(redirectURI + "?code=pkce-code-123&state=" + state)
				if cbErr == nil {
					_ = cbResp.Body.Close()
				}
			}()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("PKCE Login failed: %v", err)
	}
	if !sawPKCE {
		t.Fatal("expected PKCE authorization_code exchange at token endpoint")
	}

	st, err := ReadStatus(cfgPath)
	if err != nil || !st.HasRefreshToken || !st.HasAccessToken {
		t.Fatalf("unexpected status after PKCE login: %+v, err=%v", st, err)
	}
	if !strings.HasSuffix(st.APIBaseURL, "/iazio-harness-api") {
		t.Fatalf("expected api_base_url preserved, got %q", st.APIBaseURL)
	}
}

func TestBearerRefreshAndClientCredentials(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/hydra/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		switch r.Form.Get("grant_type") {
		case "refresh_token":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token":  "refreshed-access-tok",
				"refresh_token": "rotated-refresh-tok",
				"token_type":    "Bearer",
				"expires_in":    1800,
			})
		case "client_credentials":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "m2m-access-tok",
				"token_type":   "Bearer",
				"expires_in":   1800,
			})
		default:
			http.Error(w, "unsupported grant", http.StatusBadRequest)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.json")
	if err := save(cfgPath, Config{
		APIBaseURL:   srv.URL + "/iazio-harness-api",
		RefreshToken: "initial-refresh-tok",
	}); err != nil {
		t.Fatal(err)
	}

	getenv := func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return cfgPath
		}
		return ""
	}
	tok, err := BearerWithClient(t.Context(), srv.Client(), dir, getenv)
	if err != nil || tok != "refreshed-access-tok" {
		t.Fatalf("expected refreshed-access-tok, got %q (err=%v)", tok, err)
	}
	// Second call should reuse cached unexpired access token without network
	tok2, err := BearerWithClient(t.Context(), &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("unexpected network call on cached access token")
		return nil, errors.New("unexpected")
	})}, dir, getenv)
	if err != nil || tok2 != "refreshed-access-tok" {
		t.Fatalf("expected cached refreshed-access-tok, got %q (err=%v)", tok2, err)
	}

	// Client credentials grant path
	ccPath := filepath.Join(dir, "cc-agent.json")
	if err := save(ccPath, Config{
		APIBaseURL:   srv.URL + "/iazio-harness-api",
		ClientID:     "iazio-agent-cli",
		ClientSecret: "secret-123",
	}); err != nil {
		t.Fatal(err)
	}
	ccTok, err := BearerWithClient(t.Context(), srv.Client(), dir, func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return ccPath
		}
		return ""
	})
	if err != nil || ccTok != "m2m-access-tok" {
		t.Fatalf("expected m2m-access-tok, got %q (err=%v)", ccTok, err)
	}

	// Static env token override
	staticTok, err := Bearer(t.Context(), func(k string) string {
		if k == "IAZIO_AUTH_TOKEN" {
			return "env-static-token"
		}
		return ""
	})
	if err != nil || staticTok != "env-static-token" {
		t.Fatalf("expected env-static-token, got %q (err=%v)", staticTok, err)
	}

	// Missing token endpoint fallback when AccessToken is set without Expiry
	noTokPath := filepath.Join(dir, "notok-agent.json")
	if err := save(noTokPath, Config{AccessToken: "fallback-at"}); err != nil {
		t.Fatal(err)
	}
	fbTok, err := BearerWithClient(t.Context(), srv.Client(), dir, func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return noTokPath
		}
		return ""
	})
	if err != nil || fbTok != "fallback-at" {
		t.Fatalf("expected fallback-at, got %q (err=%v)", fbTok, err)
	}

	// Missing refresh token error
	noRefreshPath := filepath.Join(dir, "norefresh-agent.json")
	if err := save(noRefreshPath, Config{APIBaseURL: srv.URL + "/iazio-harness-api"}); err != nil {
		t.Fatal(err)
	}
	if _, err := BearerWithClient(t.Context(), srv.Client(), dir, func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return noRefreshPath
		}
		return ""
	}); err == nil {
		t.Fatal("expected error when no refresh token")
	}
}

func TestSharedConfigAndLoopbackErrorBranches(t *testing.T) {
	dir := t.TempDir()
	// 1. Shared config.json with hydra endpoint
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"endpoints":{"hydra":"https://hydra.shared.example.com/"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a, tok, dev := ResolveEndpoints(Config{}, dir, nil)
	if a != "https://hydra.shared.example.com/oauth2/auth" || tok != "https://hydra.shared.example.com/oauth2/token" || dev != "https://hydra.shared.example.com/oauth2/fallbacks/device/code" {
		t.Fatalf("unexpected shared hydra endpoints: %q %q %q", a, tok, dev)
	}

	// 2. Shared config.json with harness_api endpoint
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(`{"endpoints":{"harness_api":"https://api.shared.example.com/iazio-harness-api"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a, tok, dev = ResolveEndpoints(Config{}, dir, func(k string) string {
		if k == "OAUTH_AUTHORIZATION_SERVER_URL" {
			return "https://issuer.example.com/"
		}
		return ""
	})
	if a != "https://issuer.example.com/oauth2/auth" || tok != "https://issuer.example.com/oauth2/token" || dev != "https://issuer.example.com/oauth2/fallbacks/device/code" {
		t.Fatalf("unexpected issuer endpoints: %q %q %q", a, tok, dev)
	}
	a, _, _ = ResolveEndpoints(Config{}, dir, nil)
	if a != "https://api.shared.example.com/hydra/oauth2/auth" {
		t.Fatalf("expected harness_api derived hydra url, got %q", a)
	}

	// 3. Endpoint() and DeviceAuthURL() from APIBaseURL
	t.Setenv("IAZIO_AUTH_URL", "")
	t.Setenv("IAZIO_TOKEN_URL", "")
	epA, epT, err := Endpoint(Config{APIBaseURL: "https://tian.go.ro/iazio-harness-api"})
	if err != nil || epA != "https://tian.go.ro/hydra/oauth2/auth" || epT != "https://tian.go.ro/hydra/oauth2/token" {
		t.Fatalf("Endpoint derived failed: %q %q %v", epA, epT, err)
	}
	if d := DeviceAuthURL(Config{APIBaseURL: "https://tian.go.ro/iazio-harness-api"}, nil); d != "https://tian.go.ro/hydra/oauth2/fallbacks/device/code" {
		t.Fatalf("DeviceAuthURL derived failed: %q", d)
	}

	// 4. Loopback callback error cases (state mismatch, oauth error, missing code)
	for _, tc := range []struct {
		name  string
		query string
	}{
		{name: "state_mismatch", query: "state=wrong"},
		{name: "oauth_error", query: "error=access_denied&error_description=denied"},
		{name: "missing_code", query: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfgFile := filepath.Join(t.TempDir(), "agent.json")
			err := Login(t.Context(), LoginOptions{
				Home:         t.TempDir(),
				LoopbackAddr: "127.0.0.1:0",
				Getenv: func(k string) string {
					switch k {
					case "IAZIO_AGENT_CONFIG":
						return cfgFile
					case "IAZIO_AUTH_URL":
						return "https://tian.go.ro/hydra/oauth2/auth"
					case "IAZIO_TOKEN_URL":
						return "https://tian.go.ro/hydra/oauth2/token"
					}
					return ""
				},
				OpenBrowser: func(rawAuthURL string) error {
					u, _ := url.Parse(rawAuthURL)
					redir := u.Query().Get("redirect_uri")
					st := u.Query().Get("state")
					q := tc.query
					if q == "" {
						q = "state=" + url.QueryEscape(st)
					} else if strings.HasPrefix(q, "error=") {
						q = "state=" + url.QueryEscape(st) + "&" + q
					}
					go func() {
						resp, err := http.Get(redir + "?" + q)
						if err == nil {
							_ = resp.Body.Close()
						}
					}()
					return nil
				},
			})
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestBearerWithClient_ConcurrentRefreshSingleFlightAndAtomicSave(t *testing.T) {
	var refreshCalls atomic.Int32
	var usedRefreshMu sync.Mutex
	usedRefreshTokens := make(map[string]bool)

	mux := http.NewServeMux()
	mux.HandleFunc("/hydra/oauth2/token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("grant_type") != "refresh_token" {
			http.Error(w, "expected refresh_token grant", http.StatusBadRequest)
			return
		}
		rt := r.Form.Get("refresh_token")
		usedRefreshMu.Lock()
		alreadyUsed := usedRefreshTokens[rt]
		usedRefreshTokens[rt] = true
		usedRefreshMu.Unlock()
		if alreadyUsed {
			// Simulate Ory Hydra one-time refresh token rotation rejection on concurrent reuse
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_grant"})
			return
		}
		callNum := refreshCalls.Add(1)
		time.Sleep(15 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "concurrent-access-tok-1",
			"refresh_token": "rotated-refresh-tok-1",
			"token_type":    "Bearer",
			"expires_in":    1800,
			"call_num":      callNum,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "agent.json")
	// Write initial config with an expired access token and a custom field that save() must preserve
	initialJSON := `{"api_base_url":"` + srv.URL + `/iazio-harness-api","access_token":"expired-tok","refresh_token":"initial-rt-0","expiry":"2020-01-01T00:00:00Z","custom_field":"preserved-val"}`
	if err := os.WriteFile(cfgPath, []byte(initialJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	getenv := func(k string) string {
		if k == "IAZIO_AGENT_CONFIG" {
			return cfgPath
		}
		return ""
	}

	const callers = 20
	var wg sync.WaitGroup
	errCh := make(chan error, callers*2)
	startBarrier := make(chan struct{})

	for i := 0; i < callers; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-startBarrier
			tok, err := BearerWithClient(t.Context(), srv.Client(), dir, getenv)
			if err != nil {
				errCh <- err
				return
			}
			if tok != "concurrent-access-tok-1" {
				errCh <- errors.New("unexpected access token: " + tok)
			}
		}()
		go func() {
			defer wg.Done()
			<-startBarrier
			st, err := ReadStatus(cfgPath)
			if err != nil {
				errCh <- err
				return
			}
			if !st.HasRefreshToken {
				errCh <- errors.New("expected HasRefreshToken=true during concurrent read")
			}
		}()
	}

	close(startBarrier)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent refresh/read error: %v", err)
	}

	if got := refreshCalls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 refresh_token HTTP call across %d concurrent callers, got %d", callers, got)
	}

	// Verify custom_field was preserved and no .agent.json.tmp.* files remain
	raw, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read final agent.json: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("unmarshal final agent.json: %v", err)
	}
	if parsed["custom_field"] != "preserved-val" {
		t.Fatalf("expected custom_field preserved-val, got %v", parsed["custom_field"])
	}
	if parsed["refresh_token"] != "rotated-refresh-tok-1" {
		t.Fatalf("expected rotated-refresh-tok-1, got %v", parsed["refresh_token"])
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".agent.json.tmp.") {
			t.Fatalf("leftover temp file in config dir: %s", entry.Name())
		}
	}
}
