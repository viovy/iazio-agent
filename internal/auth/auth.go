// Package auth stores the per-tool session for the agent.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ClientID is the public OAuth client id.
const ClientID = "iazio-agent-cli"

const deviceGrant = "urn:ietf:params:oauth:grant-type:device_code"

// ErrNoninteractive is returned when login is refused in a noninteractive session.
var ErrNoninteractive = errors.New("noninteractive login refused")

// Config is the on-disk session. Endpoints come from the operator, not the binary.
type Config struct {
	ClientID     string `json:"client_id,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
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
	ClientID        string
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
	st.HasRefreshToken = cfg.RefreshToken != ""
	return st, nil
}

// Endpoint resolves authorize and token URLs from the environment, then the file.
func Endpoint(cfg Config) (authURL, tokenURL string, err error) {
	authURL = first(os.Getenv("IAZIO_AUTH_URL"), cfg.AuthURL)
	tokenURL = first(os.Getenv("IAZIO_TOKEN_URL"), cfg.TokenURL)
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
	return first(getenv("IAZIO_DEVICE_AUTH_URL"), cfg.DeviceURL)
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

// LoginOptions controls a foreground device login.
type LoginOptions struct {
	Getenv      func(string) string
	OpenBrowser func(string) error
	HTTP        *http.Client
	Stdout      io.Writer
	Home        string
	Sleep       func(time.Duration)
}

// Login runs the device flow in the foreground.
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
	cfg.AuthURL = first(getenv("IAZIO_AUTH_URL"), cfg.AuthURL)
	cfg.TokenURL = first(getenv("IAZIO_TOKEN_URL"), cfg.TokenURL)
	cfg.DeviceURL = DeviceAuthURL(cfg, getenv)
	if cfg.DeviceURL == "" || cfg.TokenURL == "" {
		return errors.New("auth endpoints are not configured")
	}
	client := opts.HTTP
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
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
		token, pending, err := pollToken(ctx, client, cfg.TokenURL, device.DeviceCode)
		if err != nil {
			return err
		}
		if token != "" {
			cfg.RefreshToken = token
			cfg.ClientID = ClientID
			return save(path, cfg)
		}
		if !pending {
			return errors.New("device flow was denied")
		}
		sleep(interval)
	}
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
		cmd = exec.Command("open", raw)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", raw)
	default:
		cmd = exec.Command("xdg-open", raw)
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
	RefreshToken string `json:"refresh_token"`
	Error        string `json:"error"`
}

func startDevice(ctx context.Context, client *http.Client, endpoint string) (deviceCode, error) {
	form := url.Values{}
	form.Set("client_id", ClientID)
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
	form := url.Values{}
	form.Set("client_id", ClientID)
	form.Set("grant_type", deviceGrant)
	form.Set("device_code", device)
	body, err := postForm(ctx, client, endpoint, form)
	if err != nil {
		return "", false, err
	}
	var parsed tokenCode
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", false, err
	}
	if parsed.RefreshToken != "" {
		return parsed.RefreshToken, false, nil
	}
	switch parsed.Error {
	case "authorization_pending", "slow_down", "":
		return "", true, nil
	default:
		return "", false, nil
	}
}

func postForm(ctx context.Context, client *http.Client, endpoint string, form url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}
