// Package controlplane is the outbound client the supervisor uses to dial the harness API.
package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Client posts heartbeats and finish checks. It does not send IDE credentials.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// Tool is one binary on a heartbeat.
type Tool struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	Version string `json:"version"`
	Status  string `json:"status"`
}

// Heartbeat marks the host online and stores the tool inventory.
func (c Client) Heartbeat(ctx context.Context, hostID string, tools []Tool, fetchFailed bool) error {
	body := map[string]any{"fetch_failed": fetchFailed, "tools": tools}
	return c.post(ctx, "/v1/hosts/"+hostID+"/heartbeat", body)
}

// Register records the host kind.
func (c Client) Register(ctx context.Context, hostID, kind string) error {
	return c.post(ctx, "/v1/hosts/register", map[string]string{"id": hostID, "kind": kind})
}

// PostChunk stores one stripped output chunk for a job.
func (c Client) PostChunk(ctx context.Context, jobID, stream, text string) error {
	return c.post(ctx, "/v1/jobs/"+jobID+"/chunks", map[string]string{
		"type": "OUTPUT_CHUNK", "stream": stream, "text": text,
	})
}

func (c Client) post(ctx context.Context, path string, body any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	httpClient := c.HTTP
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("control plane %s: %s", path, resp.Status)
	}
	return nil
}
