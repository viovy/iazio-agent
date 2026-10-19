// Package client provides HTTP clients for communication between iazio-agent and iazio-harness-api.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/viovy/iazio-agent/internal/harness"
)

const (
	defaultMaxRetries     = 3
	defaultInitialBackoff = 50 * time.Millisecond
	defaultTimeout        = 30 * time.Second
)

// RepoSyncRequest is sent in Phase 1 to register/handshake submodule git_tree_sha values.
type RepoSyncRequest struct {
	HostID string                 `json:"host_id"`
	Repos  []harness.RepoManifest `json:"repos"`
}

// OutOfSyncRepo identifies a repository requiring delta chunk synchronization.
type OutOfSyncRepo struct {
	RepoName       string `json:"repo_name"`
	IndexedTreeSHA string `json:"indexed_tree_sha"`
	TargetTreeSHA  string `json:"target_tree_sha"`
}

// RepoSyncResponse contains the subset of repositories requiring chunk export.
type RepoSyncResponse struct {
	OutOfSync []OutOfSyncRepo `json:"out_of_sync"`
}

// CodebaseSyncRequest uploads a batch of chunks and deleted files for an out-of-sync repository.
type CodebaseSyncRequest struct {
	RepoName     string              `json:"repo_name"`
	BaseTreeSHA  string              `json:"base_tree_sha"`
	GitTreeSHA   string              `json:"git_tree_sha"`
	IsFinalBatch bool                `json:"is_final_batch"`
	DeletedFiles []string            `json:"deleted_files,omitempty"`
	Chunks       []harness.CodeChunk `json:"chunks"`
}

// CodebaseSyncResponse reports the result of a chunk sync batch upload.
type CodebaseSyncResponse struct {
	Status         string `json:"status,omitempty"`
	IndexedChunks  int    `json:"indexed_chunks,omitempty"`
	IndexedTreeSHA string `json:"indexed_tree_sha,omitempty"`
}

// HarnessSyncClient defines communication with iazio-harness-api for codebase sync.
type HarnessSyncClient interface {
	SyncRepos(ctx context.Context, req RepoSyncRequest) (*RepoSyncResponse, error)
	SyncCodebase(ctx context.Context, req CodebaseSyncRequest) (*CodebaseSyncResponse, error)
}

// HTTPHarnessSyncClient is an HTTP implementation of HarnessSyncClient with retry resilience.
type HTTPHarnessSyncClient struct {
	BaseURL        string
	AuthToken      string
	HTTPClient     *http.Client
	MaxRetries     int
	InitialBackoff time.Duration
}

// NewHTTPHarnessSyncClient constructs a new HTTPHarnessSyncClient.
func NewHTTPHarnessSyncClient(baseURL, authToken string, client *http.Client) *HTTPHarnessSyncClient {
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}
	baseURL = strings.TrimRight(baseURL, "/")
	return &HTTPHarnessSyncClient{
		BaseURL:        baseURL,
		AuthToken:      authToken,
		HTTPClient:     client,
		MaxRetries:     defaultMaxRetries,
		InitialBackoff: defaultInitialBackoff,
	}
}

// SyncRepos calls POST /api/v1/agent/repos/sync to perform the Phase 1 handshake.
func (c *HTTPHarnessSyncClient) SyncRepos(ctx context.Context, req RepoSyncRequest) (*RepoSyncResponse, error) {
	url := c.BaseURL + "/api/v1/agent/repos/sync"
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal repos sync request: %w", err)
	}

	respBody, err := c.doRequestWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("repos sync failed: %w", err)
	}

	var res RepoSyncResponse
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &res); err != nil {
			return nil, fmt.Errorf("decode repos sync response: %w", err)
		}
	}
	return &res, nil
}

// SyncCodebase calls POST /api/v1/agent/codebase/sync to upload a batch of code chunks and deletions.
func (c *HTTPHarnessSyncClient) SyncCodebase(ctx context.Context, req CodebaseSyncRequest) (*CodebaseSyncResponse, error) {
	url := c.BaseURL + "/api/v1/agent/codebase/sync"
	bodyBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal codebase sync request: %w", err)
	}

	respBody, err := c.doRequestWithRetry(ctx, http.MethodPost, url, bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("codebase sync failed: %w", err)
	}

	var res CodebaseSyncResponse
	if len(respBody) > 0 {
		if err := json.Unmarshal(respBody, &res); err != nil {
			return nil, fmt.Errorf("decode codebase sync response: %w", err)
		}
	}
	return &res, nil
}

func (c *HTTPHarnessSyncClient) doRequestWithRetry(ctx context.Context, method, url string, body []byte) ([]byte, error) {
	maxRetries := c.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}
	backoff := c.InitialBackoff
	if backoff <= 0 {
		backoff = defaultInitialBackoff
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				backoff *= 2
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("create http request: %w", err)
		}
		httpReq.Header.Set("Content-Type", "application/json")
		if c.AuthToken != "" {
			httpReq.Header.Set("Authorization", "Bearer "+c.AuthToken)
		}

		resp, err := c.HTTPClient.Do(httpReq)
		if err != nil {
			lastErr = err
			if isTransientNetError(err) && attempt < maxRetries {
				continue
			}
			return nil, err
		}

		respBytes, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusNoContent {
			return respBytes, nil
		}

		lastErr = fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
		if isTransientStatusCode(resp.StatusCode) && attempt < maxRetries {
			continue
		}
		return nil, lastErr
	}

	return nil, lastErr
}

func isTransientStatusCode(code int) bool {
	return code == http.StatusServiceUnavailable ||
		code == http.StatusBadGateway ||
		code == http.StatusGatewayTimeout ||
		code == http.StatusTooManyRequests
}

func isTransientNetError(err error) bool {
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof")
}
