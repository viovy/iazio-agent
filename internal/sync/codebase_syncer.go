// Package sync implements the background and on-demand codebase delta sync worker for iazio-agent.
package sync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	stdsync "sync"
	"time"

	"github.com/viovy/iazio-agent/internal/client"
	"github.com/viovy/iazio-agent/internal/harness"
)

const (
	// DefaultSyncInterval is the default period between full fleet handshake passes.
	DefaultSyncInterval = 60 * time.Second
	// DefaultMaxChunksPerBatch is the maximum number of chunks sent in a single HTTP sync request.
	DefaultMaxChunksPerBatch = 100
	// DefaultMaxBatchBytes is the maximum payload size (2 MB) before splitting a chunk batch.
	DefaultMaxBatchBytes = 2 * 1024 * 1024
	// DefaultMaxExportChunks is the maximum chunks exported per CLI invocation.
	DefaultMaxExportChunks = 200
)

// SyncerConfig holds tuning parameters for the codebase syncer.
type SyncerConfig struct {
	HostID            string
	Interval          time.Duration
	MaxChunksPerBatch int
	MaxBatchBytes     int
	MaxExportChunks   int
}

// ResolveConfig resolves syncer configuration from environment variables and defaults.
func ResolveConfig(hostID string) SyncerConfig {
	interval := DefaultSyncInterval
	if raw := os.Getenv("CODEBASE_SYNC_INTERVAL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			interval = d
		} else if sec, err := strconv.Atoi(raw); err == nil {
			interval = time.Duration(sec) * time.Second
		}
	}

	return SyncerConfig{
		HostID:            hostID,
		Interval:          interval,
		MaxChunksPerBatch: DefaultMaxChunksPerBatch,
		MaxBatchBytes:     DefaultMaxBatchBytes,
		MaxExportChunks:   DefaultMaxExportChunks,
	}
}

// Syncer defines the codebase syncer contract.
type Syncer interface {
	Start(ctx context.Context) error
	Stop()
	TriggerRepoSync(repoName string)
	SyncAll(ctx context.Context) error
	SyncRepo(ctx context.Context, repoName string) error
}

// CodebaseSyncer coordinates two-phase Git tree delta synchronization with iazio-harness-api.
type CodebaseSyncer struct {
	invoker harness.CodebaseInvoker
	client  client.HarnessSyncClient
	cfg     SyncerConfig

	triggerCh chan string
	stopCh    chan struct{}
	doneCh    chan struct{}

	mu      stdsync.Mutex
	pending map[string]bool
	running bool
}

// NewCodebaseSyncer constructs a new CodebaseSyncer.
func NewCodebaseSyncer(invoker harness.CodebaseInvoker, client client.HarnessSyncClient, cfg SyncerConfig) *CodebaseSyncer {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultSyncInterval
	}
	if cfg.MaxChunksPerBatch <= 0 {
		cfg.MaxChunksPerBatch = DefaultMaxChunksPerBatch
	}
	if cfg.MaxBatchBytes <= 0 {
		cfg.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if cfg.MaxExportChunks <= 0 {
		cfg.MaxExportChunks = DefaultMaxExportChunks
	}

	return &CodebaseSyncer{
		invoker:   invoker,
		client:    client,
		cfg:       cfg,
		triggerCh: make(chan string, 128),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
		pending:   make(map[string]bool),
	}
}

// TriggerRepoSync queues an on-demand synchronization for a specific repository.
// It is non-blocking and coalesces duplicate requests for the same repository.
func (s *CodebaseSyncer) TriggerRepoSync(repoName string) {
	repoName = strings.TrimSpace(repoName)
	if repoName == "" {
		return
	}

	s.mu.Lock()
	if s.pending[repoName] {
		s.mu.Unlock()
		return
	}
	s.pending[repoName] = true
	s.mu.Unlock()

	select {
	case s.triggerCh <- repoName:
	default:
	}
}

// Start begins the background periodic and on-demand synchronization worker loop.
func (s *CodebaseSyncer) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("codebase syncer already running")
	}
	s.running = true
	s.mu.Unlock()

	go s.runLoop(ctx)
	return nil
}

// Stop gracefully shuts down the background synchronization worker.
func (s *CodebaseSyncer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	<-s.doneCh
}

func (s *CodebaseSyncer) runLoop(ctx context.Context) {
	defer close(s.doneCh)

	var ticker *time.Ticker
	if s.cfg.Interval > 0 {
		ticker = time.NewTicker(s.cfg.Interval)
		defer ticker.Stop()
	}

	for {
		var tickCh <-chan time.Time
		if ticker != nil {
			tickCh = ticker.C
		}

		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case repoName := <-s.triggerCh:
			s.mu.Lock()
			delete(s.pending, repoName)
			s.mu.Unlock()

			_ = s.SyncRepo(ctx, repoName)
		case <-tickCh:
			_ = s.SyncAll(ctx)
		}
	}
}

// SyncAll performs Phase 1 handshake for all local repositories and pushes deltas for out-of-sync repositories.
func (s *CodebaseSyncer) SyncAll(ctx context.Context) error {
	if s.invoker == nil {
		return errors.New("codebase invoker not configured")
	}
	if s.client == nil {
		return errors.New("harness sync client not configured")
	}

	// Phase 1: Submodule Manifest & Tree Handshake
	repos, err := s.invoker.ListLocalRepos(ctx)
	if err != nil {
		return fmt.Errorf("list local repos: %w", err)
	}

	res, err := s.client.SyncRepos(ctx, client.RepoSyncRequest{
		HostID: s.cfg.HostID,
		Repos:  repos,
	})
	if err != nil {
		return fmt.Errorf("sync repos handshake: %w", err)
	}

	if res == nil || len(res.OutOfSync) == 0 {
		// Fast-path: all repositories up to date. Zero export calls.
		return nil
	}

	// Phase 2: Incremental Chunk Export & Batch Upload for out-of-sync repositories
	for _, entry := range res.OutOfSync {
		if err := s.syncRepoDelta(ctx, entry); err != nil {
			return err
		}
	}

	return nil
}

// SyncRepo synchronizes a single repository by querying its local tree SHA, performing Phase 1, and exporting deltas.
func (s *CodebaseSyncer) SyncRepo(ctx context.Context, repoName string) error {
	if s.invoker == nil {
		return errors.New("codebase invoker not configured")
	}
	if s.client == nil {
		return errors.New("harness sync client not configured")
	}

	repos, err := s.invoker.ListLocalRepos(ctx)
	if err != nil {
		return fmt.Errorf("list local repos: %w", err)
	}

	var target *harness.RepoManifest
	for i := range repos {
		if repos[i].Name == repoName || repos[i].Path == repoName {
			target = &repos[i]
			break
		}
	}

	if target == nil {
		return fmt.Errorf("repository %q not found in local manifests", repoName)
	}

	res, err := s.client.SyncRepos(ctx, client.RepoSyncRequest{
		HostID: s.cfg.HostID,
		Repos:  []harness.RepoManifest{*target},
	})
	if err != nil {
		return fmt.Errorf("sync repo handshake for %s: %w", repoName, err)
	}

	if res == nil || len(res.OutOfSync) == 0 {
		// Fast-path: target repo up to date. Zero export calls.
		return nil
	}

	for _, entry := range res.OutOfSync {
		if entry.RepoName == target.Name {
			if err := s.syncRepoDelta(ctx, entry); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *CodebaseSyncer) syncRepoDelta(ctx context.Context, entry client.OutOfSyncRepo) error {
	exportResp, err := s.invoker.ExportRepoDelta(ctx, harness.ExportRequest{
		RepoName:    entry.RepoName,
		BaseTreeSHA: entry.IndexedTreeSHA,
		MaxChunks:   s.cfg.MaxExportChunks,
	})
	if err != nil {
		return fmt.Errorf("export repo delta for %s: %w", entry.RepoName, err)
	}

	gitTreeSHA := exportResp.GitTreeSHA
	if gitTreeSHA == "" {
		gitTreeSHA = entry.TargetTreeSHA
	}

	// Up to date or no modifications
	if exportResp.UpToDate || (len(exportResp.Chunks) == 0 && len(exportResp.DeletedFiles) == 0) {
		req := client.CodebaseSyncRequest{
			RepoName:     entry.RepoName,
			BaseTreeSHA:  entry.IndexedTreeSHA,
			GitTreeSHA:   gitTreeSHA,
			IsFinalBatch: true,
		}
		_, err := s.client.SyncCodebase(ctx, req)
		if err != nil {
			return fmt.Errorf("sync codebase (up-to-date) for %s: %w", entry.RepoName, err)
		}
		return nil
	}

	batches := partitionChunks(exportResp.Chunks, s.cfg.MaxChunksPerBatch, s.cfg.MaxBatchBytes)
	if len(batches) == 0 {
		req := client.CodebaseSyncRequest{
			RepoName:     entry.RepoName,
			BaseTreeSHA:  entry.IndexedTreeSHA,
			GitTreeSHA:   gitTreeSHA,
			IsFinalBatch: true,
			DeletedFiles: exportResp.DeletedFiles,
		}
		_, err := s.client.SyncCodebase(ctx, req)
		if err != nil {
			return fmt.Errorf("sync codebase deletions for %s: %w", entry.RepoName, err)
		}
		return nil
	}

	for i, chunkBatch := range batches {
		isFinal := (i == len(batches)-1)
		var deletedFiles []string
		if i == 0 {
			deletedFiles = exportResp.DeletedFiles
		}

		req := client.CodebaseSyncRequest{
			RepoName:     entry.RepoName,
			BaseTreeSHA:  entry.IndexedTreeSHA,
			GitTreeSHA:   gitTreeSHA,
			IsFinalBatch: isFinal,
			DeletedFiles: deletedFiles,
			Chunks:       chunkBatch,
		}

		_, err := s.client.SyncCodebase(ctx, req)
		if err != nil {
			return fmt.Errorf("sync codebase batch %d/%d for %s: %w", i+1, len(batches), entry.RepoName, err)
		}
	}

	return nil
}

func estimateChunkSize(c harness.CodeChunk) int {
	return len(c.FilePath) + len(c.Symbol) + len(c.Content) + len(c.FileBlobSHA) + len(c.GitTreeSHA) + 128
}

func partitionChunks(chunks []harness.CodeChunk, maxChunks, maxBytes int) [][]harness.CodeChunk {
	if len(chunks) == 0 {
		return nil
	}
	if maxChunks <= 0 {
		maxChunks = DefaultMaxChunksPerBatch
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBatchBytes
	}

	var batches [][]harness.CodeChunk
	var currentBatch []harness.CodeChunk
	currentBytes := 0

	for _, chunk := range chunks {
		chunkSize := estimateChunkSize(chunk)
		if len(currentBatch) > 0 && (len(currentBatch) >= maxChunks || currentBytes+chunkSize > maxBytes) {
			batches = append(batches, currentBatch)
			currentBatch = nil
			currentBytes = 0
		}
		currentBatch = append(currentBatch, chunk)
		currentBytes += chunkSize
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}

	return batches
}
