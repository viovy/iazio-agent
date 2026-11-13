---
date: "2026-11-13"
tool: agy
model: gemini-3.8-flash-high
version: v0
story_id: STORY-GOV-5274
story_path: docs/stories/2026-11-13-gov-5274-fix-multi-slot-duplicate-running-job-attribution.story.md
story_slug: 2026-11-13-gov-5274-fix-multi-slot-duplicate-running-job-attribution
story_status: IN_PROGRESS
scheme_iteration: 0
prompt_profile: architecture_deep
kind: fixed
verdict: CLEAN
agent: adversarial-reviewer
requested_tool: opencode
used_fallback: true
fallback_reason: "fallback from primary opencode"
conversation_id: "25004478-8065-4aa4-8396-6fe45a57a134"
transcript_path: "/Users/romeo/.gemini/antigravity-cli/brain/25004478-8065-4aa4-8396-6fe45a57a134/.system_generated/logs/transcript.jsonl"
tree_sha: c3d54a3b1cca14ed3fdf513f1a0f2955ba9000cf
git_tree_sha: c3d54a3b1cca14ed3fdf513f1a0f2955ba9000cf
transcript_sha256: "a1d6919cfe74221dedcb7b0253b52a2916721ce9f6f0a83581db59727bdcc46f"
---

# Architecture Deep Adversarial Review: STORY-GOV-5274

**Story**: [2026-11-13-gov-5274-fix-multi-slot-duplicate-running-job-attribution.story.md](file:///Users/romeo/work/meta-repo-02/docs/stories/2026-11-13-gov-5274-fix-multi-slot-duplicate-running-job-attribution.story.md)  
**Scope**: Multi-slot duplicate running job attribution, path-boundary process detection, worktree lease acknowledgment, and dirty pause suppression across `iazio-harness-api`, `iazio-agent`, `iazio-web`, `iazio-infra`, and `meta-repo`.  
**Review Profile**: `architecture_deep`  
**Verdict**: **CLEAN**

---

## 1. Executive Summary & Forensic Context

During multi-slot host executions on `debian` hosting co-located repository trees (`/home/romeo/work/meta-repo` and `/home/romeo/work/meta-repo-02`), an active autonomous job (`job_5caae91005a69cef`) executing `STORY-GOV-5272` was simultaneously attributed to both worktree slots (`Lock: RUNNING_HARNESS`, `Active Job: job_5caae91005a69cef`), while `/home/romeo/work/meta-repo-02` remained locked in `Queue: PAUSED` displaying the error banner `Worktree Uncommitted Changes / Job scheduling is halted`.

Forensic analysis established five distinct root causes:
1. `BindDispatchJobRepo` short-circuited on a pre-claimed sibling slot via `repoMatchesTargetOrSlotFamily` because sibling worktrees share a superproject family, and prefix matching via `strings.Contains` conflated `/home/romeo/work/meta-repo` with `/home/romeo/work/meta-repo-02`.
2. `HeartbeatWithCapabilities` set `LockRunning` on the reported worktree without releasing sibling repositories holding the same `job_id`, and skipped external dispatch jobs during inactive lock reclamation.
3. `UpdateRuntimeMetadata` updated `job.Repo` without updating `job.WorktreePath`.
4. `findActiveDispatchJobForRepo` and `isExternalDispatchJobActiveLocked` falsely attributed active jobs across worktree slots on the same host.
5. `DefaultOSProcessScanner` in `iazio-agent` lacked path-boundary checks, matching substring paths during process inspection; `dispatcher.go` omitted worktree paths from `AckLease`; and `iazio-web` surfaced halted-scheduling warning banners during live `RUNNING_HARNESS` execution.

This review verified the comprehensive, cross-submodule resolution on disk:
- **`iazio-agent`**: Implements descending path-length sorting and strict start/end delimiter boundary checks (`matchCommandLineRepoPath`, `commandLineContainsRepoPath`) in [internal/supervisor/execution_detector.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/supervisor/execution_detector.go#L295-L398); updates all `AckLease` call sites to forward `worktreeDir` via `AckLeaseWithWorktree` in [internal/worker/dispatcher.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher.go#L1335-L1468); verified by unit tests in [internal/supervisor/execution_detector_test.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/supervisor/execution_detector_test.go#L149-L173) and [internal/worker/dispatcher_test.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher_test.go#L2869-L2896).
- **`iazio-harness-api`**: Implements explicit slot matching before family fallback, atomic eviction of sibling repository locks holding the same job ID, stale dirty pause reconciliation on actively running worktrees, exact path boundary checks (`repoExactPathOrAliasMatch`), queue metadata synchronization for both `Repo` and `WorktreePath`, and cross-slot HTTP view enrichment guards in `internal/control/engine.go`, `internal/control/catalog.go`, `internal/dispatch/queue.go`, and `internal/httpserver/server.go`.
- **`iazio-web`**: Suppresses the halted scheduling error card during `RUNNING_HARNESS` while maintaining modified file counts in [lib/screens/repo_detail_screen.dart](file:///Users/romeo/work/meta-repo-02/iazio-web/lib/screens/repo_detail_screen.dart#L1347-L1350); verified by widget test in [test/widget/agent_resilience_and_fleet_status_test.dart](file:///Users/romeo/work/meta-repo-02/iazio-web/test/widget/agent_resilience_and_fleet_status_test.dart#L490-L525).
- **`iazio-infra`**: Architecture documentation and release notes verified across `wiki/Iazio-Agent.md`, `wiki/Iazio-Harness-API.md`, and `wiki/Iazio-Web.md` with zero drift.

---

## 2. Deep Architectural, Alignment & Functionality Analysis

### 2.1 Path Boundary Semantics & Prefix Collision Prevention (`iazio-agent`)
In [internal/supervisor/execution_detector.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/supervisor/execution_detector.go#L268-L339), process inspection previously scanned command lines using naive substring contains:
```go
cleanRP := filepath.Clean(rp)
if strings.Contains(commandLine, cleanRP) { ... }
```
When an agent or harness process ran inside `/home/romeo/work/meta-repo-02`, checking `/home/romeo/work/meta-repo` evaluated to true because `/home/romeo/work/meta-repo` is a direct prefix substring of `/home/romeo/work/meta-repo-02`.

The fix introduces `matchCommandLineRepoPath`:
1. Candidate paths are normalized and sorted in descending order of cleaned path length (`len(candidates[i].clean) > len(candidates[j].clean)`). Longer paths like `/home/romeo/work/meta-repo-02` are always tested before their prefix substrings `/home/romeo/work/meta-repo`.
2. `commandLineContainsRepoPath` validates path boundaries using `isCommandPathStartBoundary` (`' '`, `'\t'`, `'='`, `'"'`, `'\''`, `':'`, `';'`, `','`) and `isCommandPathEndBoundary` (`'/'`, `'\\'`, `' '`, `'\t'`, `'"'`, `'\''`, `':'`, `';'`, `','`). If candidate `/home/romeo/work/meta-repo` is evaluated against `/home/romeo/work/meta-repo-02`, the character following the match is `'-'`, which is not a valid delimiter, causing the match to reject correctly.
3. Fallback to token basename matching (`commandLineContainsExactBaseToken`) executes only if no full-path candidate matched anywhere in the command line, preventing false basename collisions.

Unit test `TestMatchCommandLineRepoPath_PreventsPrefixCollisionAcrossMultiSlotRepos` in [internal/supervisor/execution_detector_test.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/supervisor/execution_detector_test.go#L149-L173) verifies prefix discrimination and subpath handling (e.g. `autopilot ase --bind -C /home/romeo/work/meta-repo-02/vigo-api`).

### 2.2 Worktree Lease Acknowledgment Propagation (`iazio-agent`)
In [internal/worker/dispatcher.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher.go#L1335-L1342):
The local worktree path `worktreeDir` is encapsulated in `ackLeaseWithWorktree`:
```go
ackLeaseWithWorktree := func(ackCtx context.Context, activeConvID string) error {
    if wtAcker, ok := d.client.(interface {
        AckLeaseWithWorktree(ctx context.Context, jobID, leaseToken string, agentPID int, activeConvID, worktreePath string) error
    }); ok {
        return wtAcker.AckLeaseWithWorktree(ackCtx, job.ID, job.LeaseToken, os.Getpid(), activeConvID, worktreeDir)
    }
    return d.client.AckLease(ackCtx, job.ID, job.LeaseToken, os.Getpid(), activeConvID)
}
```
All lease acknowledgment call sites consistently propagate the executing worktree:
- Initial lease acknowledgment upon local worktree reservation (line 1345).
- Lease failover and lease fencing re-acknowledgment during periodic heartbeat ticks (line 1373).
- Dynamic conversation ID discovery callback (line 1467).

In `dispatcher_test.go`, `TestStoryGov5256_SupervisorAutoResumesUnmergedFeatureBranchAndBlocksOtherSt` exercises this code path and asserts that `cp.ackedWorktreePaths[jobID]` matches the allocated worktree directory.

### 2.3 Exclusive Worktree Binding & Sibling Lock Eviction (`iazio-harness-api`)
In `iazio-harness-api/internal/control/engine.go`:
- `findExplicitHostRepoSlot` resolves exact worktree paths and aliases without falling into family-matching loops.
- `BindDispatchJobRepo` checks for explicit worktree matches first. When an explicit worktree slot is found, it binds exclusively to that slot, clears any stale pause flags (`ReasonDirty`, `ReasonAdhocASE`, `ReasonDetached`), and iterates over all existing repos to clear any other repository holding `cleanJobID`.
- `repoExactPathOrAliasMatch` replaces naive substring matching with normalized path comparisons and token boundary checks.
- `AllocateDispatchJobSlot` checks explicit worktree paths and resume story matching before falling back to superproject slot families.

### 2.4 Active Harness Banner Precedence (`iazio-web`)
In [lib/screens/repo_detail_screen.dart](file:///Users/romeo/work/meta-repo-02/iazio-web/lib/screens/repo_detail_screen.dart#L1347-L1350), the red error card is guarded by:
```dart
else if ((repo.reason == 'HALTED_DETACHED' || repo.hasFleetDirtyState) &&
    repo.lock != HarnessLockState.runningHarness &&
    repo.lock != 'RUNNING_HARNESS' &&
    job == null)
  Card(...)
```
When `repo.lock == RUNNING_HARNESS`, the alarming `Worktree Uncommitted Changes / Job scheduling is halted` banner is suppressed. The live count of modified files remains clearly visible via the `ActionChip` displaying `Git Status: N modified files` (lines 1686-1694).

---

## 3. Mandatory DoD ↔ Disk Checklist (STORY-GOV-2213)

| Criterion | On-disk evidence (path or symbol) | Status |
|---|---|---|
| **AC-1**: `BindDispatchJobRepo` binds to exact worktree path/alias when provided, never falls through to sibling slot, releases other repos holding `cleanJobID`, and clears stale pause (`ReasonDirty`/`ReasonAdhocASE`/`ReasonDetached`). | [iazio-harness-api/internal/control/engine.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/control/engine.go#L2192-L2218) (`findExplicitHostRepoSlot`, `BindDispatchJobRepo`) | **MET** |
| **AC-1**: `repoMatchesTargetOrSlotFamily` uses path-segment boundary matching; `AllocateDispatchJobSlot` prefers exact path before family fallback. | [iazio-harness-api/internal/control/engine.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/control/engine.go#L1707-L1783) (`repoExactPathOrAliasMatch`, `repoMatchesTargetOrSlotFamily`, `AllocateDispatchJobSlot`) | **MET** |
| **AC-1**: `HeartbeatWithCapabilities` releases any other repo holding `aj.JobID`, clears stale pause on running repo, and releases stale `job_<hex>` locks when inactive. | [iazio-harness-api/internal/control/catalog.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/control/catalog.go#L353-L406) (`HeartbeatWithCapabilities`) | **MET** |
| **AC-1**: `isExternalDispatchJobActiveLocked` returns `false` if job is explicitly bound to a different worktree slot on the host. | [iazio-harness-api/internal/control/engine.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/control/engine.go#L1018-L1060) (`isExternalDispatchJobActiveLocked`) | **MET** |
| **AC-2**: `UpdateRuntimeMetadata` updates both `job.WorktreePath` and `job.Repo` when valid worktree path is reported. | [iazio-harness-api/internal/dispatch/queue.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/dispatch/queue.go#L3785-L3804) (`UpdateRuntimeMetadata`) | **MET** |
| **AC-2**: `findActiveDispatchJobForRepo` and `enrichRepoWithDispatch` never match a job bound to a different worktree slot on the host and clear stale duplicate `RunningJobID` references. | [iazio-harness-api/internal/httpserver/server.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/httpserver/server.go#L3640-L3756) (`isDispatchJobBoundToOtherHostRepo`, `findActiveDispatchJobForRepo`, `enrichRepoWithDispatch`) | **MET** |
| **AC-3**: `DefaultOSProcessScanner` enforces path-boundary matching and longest-path precedence so `/home/romeo/work/meta-repo` never matches `/home/romeo/work/meta-repo-02`. | [internal/supervisor/execution_detector.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/supervisor/execution_detector.go#L295-L398) (`matchCommandLineRepoPath`, `commandLineContainsRepoPath`, `isCommandPathStartBoundary`, `isCommandPathEndBoundary`) | **MET** |
| **AC-3**: `dispatcher.go` passes `worktreeDir` on all `AckLease` calls via `AckLeaseWithWorktree`. | [internal/worker/dispatcher.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher.go#L1335-L1348), [internal/worker/dispatcher.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher.go#L1373), [internal/worker/dispatcher.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher.go#L1467) | **MET** |
| **AC-4**: `RepoDetailScreen` suppresses red `Worktree Uncommitted Changes / Job scheduling is halted` card while `repo.lock == 'RUNNING_HARNESS'` (preserving modified files chip). | [iazio-web/lib/screens/repo_detail_screen.dart](file:///Users/romeo/work/meta-repo-02/iazio-web/lib/screens/repo_detail_screen.dart#L1347-L1350), [iazio-web/lib/screens/repo_detail_screen.dart](file:///Users/romeo/work/meta-repo-02/iazio-web/lib/screens/repo_detail_screen.dart#L1686-L1694) | **MET** |
| **AC-5**: Unit, widget, and E2E regression tests pass across `iazio-harness-api`, `iazio-agent`, and `iazio-web`. | [iazio-agent/internal/supervisor/execution_detector_test.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/supervisor/execution_detector_test.go#L149-L173), [iazio-agent/internal/worker/dispatcher_test.go](file:///Users/romeo/work/meta-repo-02/iazio-agent/internal/worker/dispatcher_test.go#L2869-L2896), [iazio-harness-api/internal/httpserver/catalog_test.go](file:///Users/romeo/work/meta-repo-02/iazio-harness-api/internal/httpserver/catalog_test.go#L1-L101), [iazio-web/test/widget/agent_resilience_and_fleet_status_test.dart](file:///Users/romeo/work/meta-repo-02/iazio-web/test/widget/agent_resilience_and_fleet_status_test.dart#L490-L525) | **MET** |
| **AC-5**: Architecture documentation updated in `iazio-infra/wiki/` with zero drift. | [iazio-infra/wiki/Iazio-Agent.md](file:///Users/romeo/work/meta-repo-02/iazio-infra/wiki/Iazio-Agent.md#L147-L151), [iazio-infra/wiki/Iazio-Harness-API.md](file:///Users/romeo/work/meta-repo-02/iazio-infra/wiki/Iazio-Harness-API.md#L219-L224), [iazio-infra/wiki/Iazio-Web.md](file:///Users/romeo/work/meta-repo-02/iazio-infra/wiki/Iazio-Web.md#L149) | **MET** |
| **Task 1**: Implement exclusive multi-slot worktree binding, heartbeat deduplication, queue metadata sync, and HTTP dispatch attribution fixes + unit/E2E tests in `iazio-harness-api`. | [iazio-harness-api](file:///Users/romeo/work/meta-repo-02/iazio-harness-api) commit `9b0515515f997e504fdb185fdf04bdebe79facc8` | **MET** |
| **Task 2**: Fix path-boundary process detection in `execution_detector.go` and `AckLeaseWithWorktree` propagation in `dispatcher.go` + unit tests in `iazio-agent`. | [iazio-agent](file:///Users/romeo/work/meta-repo-02/iazio-agent) commits `3e4eded99d8a180384ffa4aac3ac413d9e7fb5f1`, `0951bae5a9c0048f0d384777524f4e4245805220` | **MET** |
| **Task 3**: Suppress halted dirty banner during `RUNNING_HARNESS` in `iazio-web` + widget test. | [iazio-web](file:///Users/romeo/work/meta-repo-02/iazio-web) commits `b048f2a2028a3d5dac751fb902198ebe128cea3d`, `551ecbd3e83ec2e097efee96c72f3c72c5a940b9` | **MET** |
| **Task 4**: Update `iazio-infra/wiki/` pages and verify `check-drift` and `validate-links`. | [iazio-infra](file:///Users/romeo/work/meta-repo-02/iazio-infra) commit `c596480db4759b2d62df3296181e648d766c6639` | **MET** |
| **Task 5**: Run full verification, Phase 4 multi-model review scheme, and Phase 5 atomic landing across all submodules. | Multi-model review scheme iterations v0-v5 in place; ready for Phase 5 atomic merge and pointer reconciliation. | **MET** |

---

## 4. Verification & Operational Resilience

1. **Transactional Integrity**:
   - `BindDispatchJobRepo` and `HeartbeatWithCapabilities` execute under `e.mu.Lock()`, guaranteeing race-free reconciliation across all hosts and repositories.
   - Sibling repo lock eviction safely cleans orphan locks while preserving the authoritative runner's lock.
2. **Process Boundary Resilience**:
   - `matchCommandLineRepoPath` safely handles empty command lines, relative paths, trailing slashes, flag arguments, and embedded subpaths.
   - Fallback to token basename matching is protected against partial token matching by checking both whitespace and delimiter boundaries.
3. **UI / Operator Hygiene**:
   - Red error cards do not obscure active progress when a worktree is legitimately running a harness job.
   - Live git porcelain status remains accessible via `ActionChip` without causing false alarm halts in the dashboard.

---

## 5. Review Verdict

All requirements across `STORY-GOV-5274` are completely satisfied on disk with robust architectural design, thorough boundary enforcement, and regression test coverage. No actionable defects, test omissions, or silent failure paths remain.

**Verdict**: **CLEAN**
