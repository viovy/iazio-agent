package auth

import (
	"os"
	"path/filepath"
	"testing"
)

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
	if !st.HasRefreshToken {
		t.Fatal("expected refresh")
	}
	if contains(text, "secret-value") {
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

func TestEndpointRequired(t *testing.T) {
	t.Setenv("IAZIO_AUTH_URL", "")
	t.Setenv("IAZIO_TOKEN_URL", "")
	if _, _, err := Endpoint(Config{}); err == nil {
		t.Fatal("expected error")
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || (len(s) > 0 && (indexOf(s, sub) >= 0)))
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
