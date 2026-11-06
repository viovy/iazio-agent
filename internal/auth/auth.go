// Package auth stores the per-tool session for the agent.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-agent/internal/procutil"
)

var (
	tokenRefreshMu sync.Mutex
	configFileMu   sync.Mutex
)

// ClientID is the public OAuth client id.
const ClientID = "iazio-agent-cli"

// DefaultScope is the default OAuth2 scope requested for iazio-agent-cli.
const DefaultScope = "openid offline_access profile email harness:read harness:write credentials:manage"

// DefaultLoopbackPort is the pre-registered loopback callback port in Ory Hydra for iazio-agent-cli.
const DefaultLoopbackPort = 8095

const deviceGrant = "urn:ietf:params:oauth:grant-type:device_code"
const accessSkew = 60 * time.Second

// ErrNoninteractive is returned when login is refused in a noninteractive session.
var ErrNoninteractive = errors.New("noninteractive login refused")

// Config is the on-disk session. Endpoints come from the operator or API base URL, not hardcoded hosts.
type Config struct {
	APIBaseURL   string `json:"api_base_url,omitempty"`
	ClientID     string `json:"client_id,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	TokenType    string `json:"token_type,omitempty"`
	Expiry       string `json:"expiry,omitempty"`
	AuthURL      string `json:"auth_url,omitempty"`
	TokenURL     string `json:"token_url,omitempty"`
	DeviceURL    string `json:"device_auth_url,omitempty"`
}

// ConfigPath returns the per-tool file. It does not read other tool files.
func ConfigPath() string {
	return ConfigPathFrom("", os.Getenv)
}

// ConfigPathFrom returns the agent file, honoring IAZIO_AGENT_CONFIG.
func ConfigPathFrom(home string, getenv func(string) string) string {
	if getenv != nil {
		if v := strings.TrimSpace(getenv("IAZIO_AGENT_CONFIG")); v != "" {
			return v
		}
		if v := strings.TrimSpace(getenv("IAZIO_AGENT_CONFIG_PATH")); v != "" {
			return v
		}
	}
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	return filepath.Join(home, ".iazio", "agent.json")
}

// Status is a redacted presence report. It does not include the token.
type Status struct {
	Path            string
	HasRefreshToken bool
	HasAccessToken  bool
	ClientID        string
	APIBaseURL      string
}

// ReadStatus loads presence without printing secrets and without using the network.
func ReadStatus(path string) (Status, error) {
	st := Status{Path: path, ClientID: ClientID}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return st, nil
		}
		return st, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return st, err
	}
	if strings.TrimSpace(cfg.ClientID) != "" {
		st.ClientID = strings.TrimSpace(cfg.ClientID)
	}
	st.APIBaseURL = strings.TrimSpace(cfg.APIBaseURL)
	st.HasRefreshToken = strings.TrimSpace(cfg.RefreshToken) != ""
	st.HasAccessToken = strings.TrimSpace(cfg.AccessToken) != ""
	return st, nil
}

func deriveHydraEndpointsFromAPI(apiBase string) (authURL, tokenURL, deviceURL string) {
	raw := strings.TrimSpace(apiBase)
	if raw == "" {
		return "", "", ""
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", ""
	}
	base := u.Scheme + "://" + u.Host + "/hydra"
	return base + "/oauth2/auth", base + "/oauth2/token", base + "/oauth2/fallbacks/device/code"
}

func resolveHydraFromSharedConfig(configDir string) (authURL, tokenURL, deviceURL string) {
	if strings.TrimSpace(configDir) == "" {
		return "", "", ""
	}
	cfgJSON := filepath.Join(configDir, "config.json")
	data, err := os.ReadFile(cfgJSON)
	if err != nil {
		return "", "", ""
	}
	var parsed struct {
		Endpoints map[string]string `json:"endpoints"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil || parsed.Endpoints == nil {
		return "", "", ""
	}
	if h := strings.TrimRight(strings.TrimSpace(parsed.Endpoints["hydra"]), "/"); h != "" {
		return h + "/oauth2/auth", h + "/oauth2/token", h + "/oauth2/fallbacks/device/code"
	}
	for _, k := range []string{"harness_api", "harness-api", "iazio_api", "iazio-api"} {
		if api := strings.TrimSpace(parsed.Endpoints[k]); api != "" {
			if a, t, d := deriveHydraEndpointsFromAPI(api); a != "" {
				return a, t, d
			}
		}
	}
	return "", "", ""
}

// ResolveEndpoints resolves authorize, token, and device endpoints from env, agent.json, shared config.json, or api_base_url.
func ResolveEndpoints(cfg Config, configDir string, getenv func(string) string) (authURL, tokenURL, deviceURL string) {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	authURL = first(strings.TrimSpace(getenv("IAZIO_AUTH_URL")), strings.TrimSpace(cfg.AuthURL))
	tokenURL = first(strings.TrimSpace(getenv("IAZIO_TOKEN_URL")), strings.TrimSpace(cfg.TokenURL))
	deviceURL = first(strings.TrimSpace(getenv("IAZIO_DEVICE_AUTH_URL")), strings.TrimSpace(cfg.DeviceURL))

	issuer := first(strings.TrimSpace(getenv("OAUTH_AUTHORIZATION_SERVER_URL")), strings.TrimSpace(getenv("IAZIO_OIDC_ISSUER")))
	if issuer != "" {
		base := strings.TrimRight(issuer, "/")
		authURL = first(authURL, base+"/oauth2/auth")
		tokenURL = first(tokenURL, base+"/oauth2/token")
		deviceURL = first(deviceURL, base+"/oauth2/fallbacks/device/code")
	}

	sharedAuth, sharedToken, sharedDevice := resolveHydraFromSharedConfig(configDir)
	authURL = first(authURL, sharedAuth)
	tokenURL = first(tokenURL, sharedToken)
	deviceURL = first(deviceURL, sharedDevice)

	apiBase := first(
		strings.TrimSpace(getenv("IAZIO_HARNESS_API_URL")),
		first(strings.TrimSpace(getenv("IAZIO_API_URL")), first(strings.TrimSpace(getenv("IAZIO_API_BASE_URL")), strings.TrimSpace(cfg.APIBaseURL))),
	)
	if apiAuth, apiToken, apiDevice := deriveHydraEndpointsFromAPI(apiBase); apiAuth != "" {
		authURL = first(authURL, apiAuth)
		tokenURL = first(tokenURL, apiToken)
		deviceURL = first(deviceURL, apiDevice)
	}
	return authURL, tokenURL, deviceURL
}

// Endpoint resolves authorize and token URLs from the environment, then the file (including api_base_url derivation).
func Endpoint(cfg Config) (authURL, tokenURL string, err error) {
	authURL = first(os.Getenv("IAZIO_AUTH_URL"), cfg.AuthURL)
	tokenURL = first(os.Getenv("IAZIO_TOKEN_URL"), cfg.TokenURL)
	if (authURL == "" || tokenURL == "") && strings.TrimSpace(cfg.APIBaseURL) != "" {
		a, t, _ := deriveHydraEndpointsFromAPI(cfg.APIBaseURL)
		authURL = first(authURL, a)
		tokenURL = first(tokenURL, t)
	}
	if authURL == "" || tokenURL == "" {
		return "", "", errors.New("IAZIO_AUTH_URL and IAZIO_TOKEN_URL are required")
	}
	return authURL, tokenURL, nil
}

// DeviceAuthURL returns the device endpoint from the environment or the agent file.
func DeviceAuthURL(cfg Config, getenv func(string) string) string {
	if getenv == nil {
		getenv = func(string) string { return "" }
	}
	if u := first(getenv("IAZIO_DEVICE_AUTH_URL"), cfg.DeviceURL); u != "" {
		return u
	}
	if _, _, d := deriveHydraEndpointsFromAPI(cfg.APIBaseURL); d != "" {
		return d
	}
	return ""
}

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// LoginAllowed reports whether a browser login may start.
// A noninteractive session fails closed. A GUI service name alone does not.
func LoginAllowed(noninteractive bool, hasRefresh bool) error {
	if noninteractive {
		return ErrNoninteractive
	}
	_ = hasRefresh
	return nil
}

// Noninteractive reports whether login must fail closed.
// IAZIO_AGENT_NONINTERACTIVE=1 or a set INVOCATION_ID refuses login.
// XPC_SERVICE_NAME alone does not.
func Noninteractive(getenv func(string) string) bool {
	if getenv == nil {
		return false
	}
	if getenv("IAZIO_AGENT_NONINTERACTIVE") == "1" {
		return true
	}
	return strings.TrimSpace(getenv("INVOCATION_ID")) != ""
}

// FormatStatus prints redacted presence.
func FormatStatus(st Status) string {
	present := "absent"
	if st.HasRefreshToken {
		present = "present"
	}
	return fmt.Sprintf("config=%s refresh_token=%s client_id=%s", st.Path, present, st.ClientID)
}

// LoginOptions controls a foreground OAuth login (PKCE authorization code with device-flow fallback).
type LoginOptions struct {
	Getenv       func(string) string
	OpenBrowser  func(string) error
	HTTP         *http.Client
	Stdout       io.Writer
	Home         string
	Sleep        func(time.Duration)
	LoopbackAddr string
}

// Login runs the PKCE authorization code flow (or falls back to device flow) in the foreground.
// A noninteractive session returns before any browser or network call.
func Login(ctx context.Context, opts LoginOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	if Noninteractive(getenv) {
		return ErrNoninteractive
	}
	path := ConfigPathFrom(opts.Home, getenv)
	cfg, err := load(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	authURL, tokenURL, deviceURL := ResolveEndpoints(cfg, filepath.Dir(path), getenv)
	cfg.AuthURL = authURL
	cfg.TokenURL = tokenURL
	cfg.DeviceURL = deviceURL
	cfg.APIBaseURL = first(cfg.APIBaseURL, first(strings.TrimSpace(getenv("IAZIO_HARNESS_API_URL")), strings.TrimSpace(getenv("IAZIO_API_URL"))))
	if cfg.TokenURL == "" || (cfg.AuthURL == "" && cfg.DeviceURL == "") {
		return errors.New("auth endpoints are not configured")
	}
	client := opts.HTTP
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}

	isSSH := strings.TrimSpace(getenv("SSH_CLIENT")) != "" || strings.TrimSpace(getenv("SSH_TTY")) != ""
	if cfg.AuthURL != "" && !isSSH {
		err := loopbackLogin(ctx, client, path, &cfg, opts)
		if err == nil {
			return nil
		}
		if cfg.DeviceURL == "" || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	if cfg.DeviceURL == "" {
		return errors.New("auth endpoints are not configured")
	}
	return deviceLogin(ctx, client, path, &cfg, opts)
}

func loopbackLogin(ctx context.Context, client *http.Client, path string, cfg *Config, opts LoginOptions) error {
	verifier, challenge, err := newPKCE()
	if err != nil {
		return err
	}
	state, err := randomURLString(16)
	if err != nil {
		return err
	}
	bindAddr := strings.TrimSpace(opts.LoopbackAddr)
	if bindAddr == "" {
		bindAddr = fmt.Sprintf("127.0.0.1:%d", DefaultLoopbackPort)
	}
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return fmt.Errorf("bind loopback callback %s: %w", bindAddr, err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	redirectURI := fmt.Sprintf("http://127.0.0.1:%d/callback", port)

	clientID := first(strings.TrimSpace(cfg.ClientID), ClientID)
	authURL, err := buildAuthorizeURL(cfg.AuthURL, clientID, redirectURI, state, challenge)
	if err != nil {
		_ = ln.Close()
		return err
	}

	codeCh := make(chan string, 1)
	errCh := make(chan error, 1)
	mux := http.NewServeMux()
	srv := &http.Server{Handler: mux}
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("state") != state {
			http.Error(w, "Invalid state", http.StatusBadRequest)
			select {
			case errCh <- errors.New("oauth callback state mismatch"):
			default:
			}
			return
		}
		if errParam := r.URL.Query().Get("error"); errParam != "" {
			desc := r.URL.Query().Get("error_description")
			http.Error(w, "OAuth error: "+errParam, http.StatusBadRequest)
			select {
			case errCh <- fmt.Errorf("oauth error: %s (%s)", errParam, desc):
			default:
			}
			return
		}
		code := r.URL.Query().Get("code")
		if code == "" {
			http.Error(w, "Missing authorization code", http.StatusBadRequest)
			select {
			case errCh <- errors.New("missing authorization code"):
			default:
			}
			return
		}
		_, _ = io.WriteString(w, "Authentication successful! You may close this window.")
		select {
		case codeCh <- code:
		default:
		}
	})
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()

	if opts.Stdout != nil {
		if _, err := fmt.Fprintf(opts.Stdout, "Opening browser for authentication. If it does not open, please visit:\n%s\n", authURL); err != nil {
			return err
		}
	}
	open := opts.OpenBrowser
	if open == nil {
		open = OpenBrowser
	}
	if err := open(authURL); err != nil {
		return err
	}

	var code string
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case code = <-codeCh:
	}

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("client_id", clientID)
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("code_verifier", verifier)
	body, err := postForm(ctx, client, cfg.TokenURL, form)
	if err != nil {
		return err
	}
	var tok tokenCode
	if err := json.Unmarshal(body, &tok); err != nil {
		return err
	}
	if tok.Error != "" {
		return fmt.Errorf("token exchange error: %s", tok.Error)
	}
	if tok.AccessToken == "" && tok.RefreshToken == "" {
		return errors.New("token exchange returned no token")
	}
	applyToken(cfg, tok)
	cfg.ClientID = clientID
	return save(path, *cfg)
}

func buildAuthorizeURL(rawAuthURL, clientID, redirectURI, state, challenge string) (string, error) {
	u, err := url.Parse(rawAuthURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", first(clientID, ClientID))
	q.Set("redirect_uri", redirectURI)
	q.Set("state", state)
	q.Set("code_challenge", challenge)
	q.Set("code_challenge_method", "S256")
	q.Set("scope", DefaultScope)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func deviceLogin(ctx context.Context, client *http.Client, path string, cfg *Config, opts LoginOptions) error {
	device, err := startDevice(ctx, client, cfg.DeviceURL)
	if err != nil {
		return err
	}
	verify := device.VerificationURIComplete
	if verify == "" {
		verify = device.VerificationURI
	}
	if verify == "" {
		return errors.New("device flow returned no verification url")
	}
	if opts.Stdout != nil {
		if _, err := fmt.Fprintf(opts.Stdout, "verification_url: %s\n", verify); err != nil {
			return err
		}
	}
	open := opts.OpenBrowser
	if open == nil {
		open = OpenBrowser
	}
	if err := open(verify); err != nil {
		return err
	}
	sleep := opts.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}
	interval := time.Duration(device.Interval) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		tok, pending, err := pollTokenDetailed(ctx, client, cfg.TokenURL, device.DeviceCode)
		if err != nil {
			return err
		}
		if tok.RefreshToken != "" || tok.AccessToken != "" {
			applyToken(cfg, tok)
			cfg.ClientID = first(cfg.ClientID, ClientID)
			return save(path, *cfg)
		}
		if !pending {
			return errors.New("device flow was denied")
		}
		sleep(interval)
	}
}

// Bearer returns an active OAuth2 access token (or static override token), refreshing via
// refresh_token or client_credentials when the cached access token is missing or expired.
func Bearer(ctx context.Context, getenv func(string) string) (string, error) {
	return BearerWithClient(ctx, nil, "", getenv)
}

// BearerWithClient returns an active OAuth2 access token using the provided HTTP client and home dir.
func BearerWithClient(ctx context.Context, client *http.Client, home string, getenv func(string) string) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	for _, k := range []string{"IAZIO_AUTH_TOKEN", "IAZIO_HARNESS_API_TOKEN", "HARNESS_AUTH_TOKEN", "IAZIO_ACCESS_TOKEN"} {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return v, nil
		}
	}
	path := ConfigPathFrom(home, getenv)
	cfg, err := load(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if accessUsable(cfg) {
		return strings.TrimSpace(cfg.AccessToken), nil
	}

	tokenRefreshMu.Lock()
	defer tokenRefreshMu.Unlock()

	// Re-load and re-check after acquiring lock in case another goroutine already refreshed the token
	cfg, err = load(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if accessUsable(cfg) {
		return strings.TrimSpace(cfg.AccessToken), nil
	}

	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	authURL, tokenURL, deviceURL := ResolveEndpoints(cfg, filepath.Dir(path), getenv)
	if tokenURL == "" {
		if strings.TrimSpace(cfg.AccessToken) != "" {
			return strings.TrimSpace(cfg.AccessToken), nil
		}
		return "", errors.New("oauth token endpoint is not configured")
	}

	clientID := first(strings.TrimSpace(getenv("IAZIO_CLIENT_ID")), first(strings.TrimSpace(cfg.ClientID), ClientID))
	clientSecret := first(strings.TrimSpace(getenv("IAZIO_CLIENT_SECRET")), strings.TrimSpace(cfg.ClientSecret))
	if clientID != "" && clientSecret != "" {
		form := url.Values{}
		form.Set("grant_type", "client_credentials")
		form.Set("client_id", clientID)
		form.Set("client_secret", clientSecret)
		form.Set("scope", "harness:read harness:write credentials:manage")
		body, err := postForm(ctx, client, tokenURL, form)
		if err != nil {
			return "", err
		}
		var tok tokenCode
		if err := json.Unmarshal(body, &tok); err != nil {
			return "", err
		}
		if tok.Error != "" || strings.TrimSpace(tok.AccessToken) == "" {
			return "", errors.New("client_credentials token request failed")
		}
		applyToken(&cfg, tok)
		cfg.ClientID = clientID
		cfg.AuthURL = first(cfg.AuthURL, authURL)
		cfg.TokenURL = first(cfg.TokenURL, tokenURL)
		cfg.DeviceURL = first(cfg.DeviceURL, deviceURL)
		_ = save(path, cfg)
		return strings.TrimSpace(cfg.AccessToken), nil
	}

	if strings.TrimSpace(cfg.RefreshToken) == "" {
		return "", errors.New("no refresh token")
	}
	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("client_id", clientID)
	form.Set("refresh_token", strings.TrimSpace(cfg.RefreshToken))
	body, err := postForm(ctx, client, tokenURL, form)
	if err != nil {
		return "", err
	}
	var tok tokenCode
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", err
	}
	if tok.Error != "" || strings.TrimSpace(tok.AccessToken) == "" {
		return "", fmt.Errorf("refresh_token exchange failed: %s", tok.Error)
	}
	applyToken(&cfg, tok)
	cfg.ClientID = clientID
	cfg.AuthURL = first(cfg.AuthURL, authURL)
	cfg.TokenURL = first(cfg.TokenURL, tokenURL)
	cfg.DeviceURL = first(cfg.DeviceURL, deviceURL)
	if err := save(path, cfg); err != nil {
		return "", err
	}
	return strings.TrimSpace(cfg.AccessToken), nil
}

func accessUsable(cfg Config) bool {
	if strings.TrimSpace(cfg.AccessToken) == "" {
		return false
	}
	exp := strings.TrimSpace(cfg.Expiry)
	if exp == "" {
		return strings.TrimSpace(cfg.RefreshToken) == ""
	}
	t, err := time.Parse(time.RFC3339, exp)
	if err != nil {
		return false
	}
	return time.Now().Add(accessSkew).Before(t)
}

func applyToken(cfg *Config, tok tokenCode) {
	if strings.TrimSpace(tok.AccessToken) != "" {
		cfg.AccessToken = strings.TrimSpace(tok.AccessToken)
	}
	if strings.TrimSpace(tok.RefreshToken) != "" {
		cfg.RefreshToken = strings.TrimSpace(tok.RefreshToken)
	}
	if strings.TrimSpace(tok.TokenType) != "" {
		cfg.TokenType = strings.TrimSpace(tok.TokenType)
	} else if cfg.TokenType == "" && cfg.AccessToken != "" {
		cfg.TokenType = "Bearer"
	}
	if cfg.AccessToken != "" {
		secs := tok.ExpiresIn
		if secs <= 0 {
			secs = 1800
		}
		cfg.Expiry = time.Now().Add(time.Duration(secs) * time.Second).UTC().Format(time.RFC3339)
	}
}

func newPKCE() (verifier, challenge string, err error) {
	verifier, err = randomURLString(32)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(sum[:])
	return verifier, challenge, nil
}

func randomURLString(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// OpenBrowser opens a verification URL for a foreground login.
func OpenBrowser(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported verification url")
	}
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = procutil.Command("open", raw)
	case "windows":
		cmd = procutil.Command("rundll32", "url.dll,FileProtocolHandler", raw)
	default:
		cmd = procutil.Command("xdg-open", raw)
	}
	return cmd.Start()
}

func load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

type deviceCode struct {
	DeviceCode              string `json:"device_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	Interval                int    `json:"interval"`
}

type tokenCode struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Error        string `json:"error"`
}

func startDevice(ctx context.Context, client *http.Client, endpoint string) (deviceCode, error) {
	form := url.Values{}
	form.Set("client_id", ClientID)
	form.Set("scope", DefaultScope)
	body, err := postForm(ctx, client, endpoint, form)
	if err != nil {
		return deviceCode{}, err
	}
	var parsed deviceCode
	if err := json.Unmarshal(body, &parsed); err != nil {
		return deviceCode{}, err
	}
	if parsed.DeviceCode == "" {
		return deviceCode{}, errors.New("device flow returned no device code")
	}
	return parsed, nil
}

func pollToken(ctx context.Context, client *http.Client, endpoint, device string) (string, bool, error) {
	tok, pending, err := pollTokenDetailed(ctx, client, endpoint, device)
	if err != nil {
		return "", false, err
	}
	if tok.RefreshToken != "" {
		return tok.RefreshToken, false, nil
	}
	if tok.AccessToken != "" {
		return tok.AccessToken, false, nil
	}
	return "", pending, nil
}

func pollTokenDetailed(ctx context.Context, client *http.Client, endpoint, device string) (tokenCode, bool, error) {
	form := url.Values{}
	form.Set("client_id", ClientID)
	form.Set("grant_type", deviceGrant)
	form.Set("device_code", device)
	body, err := postForm(ctx, client, endpoint, form)
	if err != nil {
		return tokenCode{}, false, err
	}
	var parsed tokenCode
	if err := json.Unmarshal(body, &parsed); err != nil {
		return tokenCode{}, false, err
	}
	if parsed.RefreshToken != "" || parsed.AccessToken != "" {
		return parsed, false, nil
	}
	switch parsed.Error {
	case "authorization_pending", "slow_down", "":
		return tokenCode{}, true, nil
	default:
		return tokenCode{}, false, nil
	}
}

func postForm(ctx context.Context, client *http.Client, endpoint string, form url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 500 {
		return nil, fmt.Errorf("auth endpoint status %d", resp.StatusCode)
	}
	return body, nil
}

func save(path string, cfg Config) error {
	if strings.HasPrefix(filepath.ToSlash(path), "/dev/null/") || filepath.ToSlash(path) == "/dev/null" {
		return errors.New("invalid config path")
	}
	configFileMu.Lock()
	defer configFileMu.Unlock()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	existing := map[string]any{}
	if b, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(b, &existing)
	}
	typed, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	var overlay map[string]any
	if err := json.Unmarshal(typed, &overlay); err != nil {
		return err
	}
	for k, v := range overlay {
		existing[k] = v
	}
	data, err := json.MarshalIndent(existing, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')

	tmpFile, err := os.CreateTemp(dir, ".agent.json.tmp.*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()
	if err := tmpFile.Chmod(0o600); err != nil && runtime.GOOS != "windows" {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if _, err := tmpFile.Write(data); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if runtime.GOOS == "windows" {
			_ = os.Remove(path)
			if err2 := os.Rename(tmpPath, path); err2 == nil {
				return nil
			}
		}
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
