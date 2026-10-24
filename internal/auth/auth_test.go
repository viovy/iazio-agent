package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
