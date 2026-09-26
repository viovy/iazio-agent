// Package auth stores the per-tool Hydra session for the agent.
package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ClientID is the public OAuth client id.
const ClientID = "iazio-agent-cli"

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
	if v := os.Getenv("IAZIO_AGENT_CONFIG"); v != "" {
		return v
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".iazio", "agent.json")
}

// Status is a redacted presence report. It does not include the token.
type Status struct {
	Path            string
	HasRefreshToken bool
	ClientID        string
}

// ReadStatus loads presence without printing secrets.
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

func first(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// LoginAllowed reports whether a browser login may start.
// A noninteractive service flag fails closed. A GUI service name alone does not.
func LoginAllowed(noninteractive bool, hasRefresh bool) error {
	if noninteractive && !hasRefresh {
		return errors.New("noninteractive login requires an existing refresh token")
	}
	return nil
}

// FormatStatus prints redacted presence.
func FormatStatus(st Status) string {
	present := "absent"
	if st.HasRefreshToken {
		present = "present"
	}
	return fmt.Sprintf("config=%s refresh_token=%s client_id=%s", st.Path, present, st.ClientID)
}
