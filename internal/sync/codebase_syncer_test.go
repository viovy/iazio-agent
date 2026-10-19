package sync

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viovy/iazio-agent/internal/client"
	"github.com/viovy/iazio-agent/internal/harness"
)

type mockCodebaseInvoker struct {
	listLocalReposFunc  func(ctx context.Context) ([]harness.RepoManifest, error)
	exportRepoDeltaFunc func(ctx context.Context, req harness.ExportRequest) (*harness.ExportResponse, error)

	exportCallCount atomic.Int32
}

func (m *mockCodebaseInvoker) ListLocalRepos(ctx context.Context) ([]harness.RepoManifest, error) {
	if m.listLocalReposFunc != nil {
		return m.listLocalReposFunc(ctx)
	}
	return []harness.RepoManifest{
		{Name: "iazio-mcp", Path: "iazio-mcp", DefaultBranch: "main", GitTreeSHA: "tree-new"},
	}, nil
}

func (m *mockCodebaseInvoker) ExportRepoDelta(ctx context.Context, req harness.ExportRequest) (*harness.ExportResponse, error) {
	m.exportCallCount.Add(1)
	if m.exportRepoDeltaFunc != nil {
		return m.exportRepoDeltaFunc(ctx, req)
	}
	return &harness.ExportResponse{
		RepoName:    req.RepoName,
		BaseTreeSHA: req.BaseTreeSHA,
		GitTreeSHA:  "tree-new",
		UpToDate:    true,
	}, nil
}

type mockHarnessSyncClient struct {
	syncReposFunc    func(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error)
	syncCodebaseFunc func(ctx context.Context, req client.CodebaseSyncRequest) (*client.CodebaseSyncResponse, error)

	syncedBatches []client.CodebaseSyncRequest
}

func (m *mockHarnessSyncClient) SyncRepos(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error) {
	if m.syncReposFunc != nil {
		return m.syncReposFunc(ctx, req)
	}
	return &client.RepoSyncResponse{}, nil
}

func (m *mockHarnessSyncClient) SyncCodebase(ctx context.Context, req client.CodebaseSyncRequest) (*client.CodebaseSyncResponse, error) {
	m.syncedBatches = append(m.syncedBatches, req)
	if m.syncCodebaseFunc != nil {
		return m.syncCodebaseFunc(ctx, req)
	}
	return &client.CodebaseSyncResponse{
		Status:         "success",
		IndexedChunks:  len(req.Chunks),
		IndexedTreeSHA: req.GitTreeSHA,
	}, nil
}

func TestCodebaseSyncer_FastPathNoOp(t *testing.T) {
	invoker := &mockCodebaseInvoker{}
	cli := &mockHarnessSyncClient{
		syncReposFunc: func(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error) {
			// Return empty out_of_sync
			return &client.RepoSyncResponse{
				OutOfSync: []client.OutOfSyncRepo{},
			}, nil
		},
	}

	syncer := NewCodebaseSyncer(invoker, cli, SyncerConfig{HostID: "mac-mini"})
	err := syncer.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if invoker.exportCallCount.Load() != 0 {
		t.Fatalf("expected 0 export calls on fast path, got %d", invoker.exportCallCount.Load())
	}
	if len(cli.syncedBatches) != 0 {
		t.Fatalf("expected 0 codebase sync batches, got %d", len(cli.syncedBatches))
	}
}

func TestCodebaseSyncer_MultiBatchUpload(t *testing.T) {
	// Create 250 test chunks
	totalChunks := 250
	chunks := make([]harness.CodeChunk, totalChunks)
	for i := 0; i < totalChunks; i++ {
		chunks[i] = harness.CodeChunk{
			FilePath:    fmt.Sprintf("file_%d.go", i),
			StartLine:   1,
			EndLine:     50,
			Symbol:      fmt.Sprintf("Func%d", i),
			Content:     fmt.Sprintf("func Func%d() { println(%d) }", i, i),
			FileBlobSHA: fmt.Sprintf("blob-%d", i),
			GitTreeSHA:  "tree-new",
		}
	}

	invoker := &mockCodebaseInvoker{
		exportRepoDeltaFunc: func(ctx context.Context, req harness.ExportRequest) (*harness.ExportResponse, error) {
			return &harness.ExportResponse{
				RepoName:     "iazio-mcp",
				BaseTreeSHA:  req.BaseTreeSHA,
				GitTreeSHA:   "tree-new",
				DeletedFiles: []string{"legacy.go"},
				Chunks:       chunks,
			}, nil
		},
	}

	cli := &mockHarnessSyncClient{
		syncReposFunc: func(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error) {
			return &client.RepoSyncResponse{
				OutOfSync: []client.OutOfSyncRepo{
					{
						RepoName:       "iazio-mcp",
						IndexedTreeSHA: "tree-old",
						TargetTreeSHA:  "tree-new",
					},
				},
			}, nil
		},
	}

	syncer := NewCodebaseSyncer(invoker, cli, SyncerConfig{
		HostID:            "mac-mini",
		MaxChunksPerBatch: 100, // Batch limit: 100 chunks
	})

	err := syncer.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("unexpected sync error: %v", err)
	}

	// 250 chunks with max 100 per batch -> 3 batches (100, 100, 50)
	if len(cli.syncedBatches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(cli.syncedBatches))
	}

	// Batch 0
	if cli.syncedBatches[0].IsFinalBatch {
		t.Errorf("batch 0 should not be final")
	}
	if len(cli.syncedBatches[0].Chunks) != 100 {
		t.Errorf("batch 0 expected 100 chunks, got %d", len(cli.syncedBatches[0].Chunks))
	}
	if len(cli.syncedBatches[0].DeletedFiles) != 1 || cli.syncedBatches[0].DeletedFiles[0] != "legacy.go" {
		t.Errorf("batch 0 should contain deleted files")
	}

	// Batch 1
	if cli.syncedBatches[1].IsFinalBatch {
		t.Errorf("batch 1 should not be final")
	}
	if len(cli.syncedBatches[1].Chunks) != 100 {
		t.Errorf("batch 1 expected 100 chunks, got %d", len(cli.syncedBatches[1].Chunks))
	}

	// Batch 2 (final)
	if !cli.syncedBatches[2].IsFinalBatch {
		t.Errorf("batch 2 MUST be final")
	}
	if len(cli.syncedBatches[2].Chunks) != 50 {
		t.Errorf("batch 2 expected 50 chunks, got %d", len(cli.syncedBatches[2].Chunks))
	}
}

func TestCodebaseSyncer_CoalescedTrigger(t *testing.T) {
	var syncCount atomic.Int32
	invoker := &mockCodebaseInvoker{}
	cli := &mockHarnessSyncClient{
		syncReposFunc: func(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error) {
			syncCount.Add(1)
			return &client.RepoSyncResponse{}, nil
		},
	}

	syncer := NewCodebaseSyncer(invoker, cli, SyncerConfig{
		HostID:   "mac-mini",
		Interval: 1 * time.Hour, // Disable ticker during test
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := syncer.Start(ctx); err != nil {
		t.Fatalf("failed to start syncer: %v", err)
	}
	defer syncer.Stop()

	// Trigger 5 times rapidly
	for i := 0; i < 5; i++ {
		syncer.TriggerRepoSync("iazio-mcp")
	}

	// Give time for worker to process
	time.Sleep(50 * time.Millisecond)

	count := syncCount.Load()
	if count < 1 || count > 2 {
		t.Fatalf("expected 1 or 2 executions due to coalescing, got %d", count)
	}
}

func TestCodebaseSyncer_FailureResilience(t *testing.T) {
	invoker := &mockCodebaseInvoker{
		exportRepoDeltaFunc: func(ctx context.Context, req harness.ExportRequest) (*harness.ExportResponse, error) {
			return &harness.ExportResponse{
				RepoName:    "iazio-mcp",
				BaseTreeSHA: "old-sha",
				GitTreeSHA:  "new-sha",
				Chunks: []harness.CodeChunk{
					{FilePath: "main.go", Content: "package main"},
				},
			}, nil
		},
	}

	cli := &mockHarnessSyncClient{
		syncReposFunc: func(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error) {
			return &client.RepoSyncResponse{
				OutOfSync: []client.OutOfSyncRepo{
					{RepoName: "iazio-mcp", IndexedTreeSHA: "old-sha", TargetTreeSHA: "new-sha"},
				},
			}, nil
		},
		syncCodebaseFunc: func(ctx context.Context, req client.CodebaseSyncRequest) (*client.CodebaseSyncResponse, error) {
			return nil, errors.New("503 Service Unavailable")
		},
	}

	syncer := NewCodebaseSyncer(invoker, cli, SyncerConfig{HostID: "mac-mini"})
	err := syncer.SyncAll(context.Background())
	if err == nil {
		t.Fatal("expected error when codebase sync fails, got nil")
	}

	// Subsequent attempt: mock recovered API
	cli.syncCodebaseFunc = nil
	err = syncer.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("expected successful retry after recovery: %v", err)
	}
}

func TestCodebaseSyncer_SyncRepo_Single(t *testing.T) {
	invoker := &mockCodebaseInvoker{
		listLocalReposFunc: func(ctx context.Context) ([]harness.RepoManifest, error) {
			return []harness.RepoManifest{
				{Name: "iazio-mcp", Path: "iazio-mcp", DefaultBranch: "main", GitTreeSHA: "mcp-sha"},
				{Name: "iazio-harness", Path: "iazio-harness", DefaultBranch: "main", GitTreeSHA: "harness-sha"},
			}, nil
		},
		exportRepoDeltaFunc: func(ctx context.Context, req harness.ExportRequest) (*harness.ExportResponse, error) {
			return &harness.ExportResponse{
				RepoName: req.RepoName,
				UpToDate: true,
			}, nil
		},
	}

	var checkedRepo string
	cli := &mockHarnessSyncClient{
		syncReposFunc: func(ctx context.Context, req client.RepoSyncRequest) (*client.RepoSyncResponse, error) {
			if len(req.Repos) != 1 {
				t.Fatalf("expected exactly 1 repo in sync request, got %d", len(req.Repos))
			}
			checkedRepo = req.Repos[0].Name
			return &client.RepoSyncResponse{
				OutOfSync: []client.OutOfSyncRepo{
					{RepoName: req.Repos[0].Name, IndexedTreeSHA: "old-sha", TargetTreeSHA: req.Repos[0].GitTreeSHA},
				},
			}, nil
		},
	}

	syncer := NewCodebaseSyncer(invoker, cli, SyncerConfig{HostID: "mac-mini"})
	err := syncer.SyncRepo(context.Background(), "iazio-mcp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if checkedRepo != "iazio-mcp" {
		t.Fatalf("expected target repo iazio-mcp, got %s", checkedRepo)
	}
}
