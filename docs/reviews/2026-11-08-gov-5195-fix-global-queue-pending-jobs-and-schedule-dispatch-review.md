# Multi-Iteration Review Report: STORY-GOV-5195

## 1. Scope & Objective

- **Story**: `STORY-GOV-5195` — Fix Global Queue Pending Tool Updates, Phantom Rehydrated Engine Jobs, and Duplicate Schedule Dispatch
- **Scope**: `iazio-harness-api`, `iazio-agent`, `iazio-web`, `iazio-infra`, and `meta-repo`.
- **Verdict**: `CLEAN` across all review iterations.

## 2. Iteration Findings

### Iteration 0 — `architecture_deep`
- Verified `InMemoryQueue.ClaimWithWorktrees` (`iazio-harness-api/internal/dispatch/queue.go`) evaluates `hostEligibilityChecker` before acquiring `q.mu`, and allows `isHostLevelToolsUpdateJob(job)` (`Type == "agent.tools.update" || Repo == "fleet"`) to bypass `hostEligibleForRepoJobs` (`DRIFT_DETECTED`), `idleWorktrees` slot-family matching, and `q.slotAllocator`.
- Verified `Engine.AllocateDispatchJobSlot` and `Engine.BindDispatchJobRepo` (`iazio-harness-api/internal/control/engine.go`) return `("", true)` and `""` immediately when `targetRepo == "fleet"`, preventing host-level tool updates from locking git worktree slots.
- State: `CLEAN`

### Iteration 1 — `adversarial_arch`
- Verified `reconcileQueueWithEngine` in `Server` (`iazio-harness-api/internal/httpserver/server.go`) and `JobLifecycleHandler` (`iazio-harness-api/internal/api/handlers/job_lifecycle.go`) never holds `Engine.mu` and `InMemoryQueue.mu` simultaneously, preserving strict lock ordering (`-race` clean).
- Verified `Server.queueToolUpdate` and `Server.queueAllToolsUpdate` supersede older `pending` `agent.tools.update` jobs for the same host (`supersedePendingToolUpdateJobsForHost`), and `Server.reconcileQueueWithEngine` / `Server.updateEagerReadiness` automatically complete pending `agent.tools.update` jobs once the target host consumes `PendingToolUpdate` and reports `EagerUpdateState == StateUpToDate`.
- State: `CLEAN`

### Iteration 2 — `autonomous_hardening`
- Verified `AgentJobsHandler.HeartbeatHandler` (`iazio-harness-api/internal/api/handlers/agent_jobs.go`) delegates `s.Engine` job heartbeats (`job-<N>`) to `Engine.HeartbeatJob` and never rehydrates `s.Engine` jobs into `s.Queue`, eliminating the phantom `RUNNING` dispatch job loop that locked `/Users/romeo/work/meta-repo-05` after `job-1566` finished.
- Verified `Engine.PostExit` and `Engine.ApplyFinish` clear both `repo.RunningJobID` and `repo.LockHolder` when unlocking `repo.Lock = LockIdle`, and `Engine.isExternalDispatchJobActiveLocked` returns `false` when `e.jobs[cleanID]` has `ExitPosted` or terminal status.
- State: `CLEAN`

### Iteration 3 — `polish_fix`
- Verified `Engine.resumeRepoInPlaceLocked`, `Engine.ResumeJob`, and `Engine.reapExpiredLocked` (`iazio-harness-api/internal/control/engine.go`) deduplicate and reconcile sibling `KindResume` schedules for the same `ParentJobID` (`sch-1565` / `sch-1567` for `job-1539`) and normalize legacy terminal schedule statuses (`EXECUTION_TIMEOUT`, `EXECUTION_STALLED_TIMEOUT`, `LEASE_EXPIRED`, `PROCESS_DISAPPEARED_ON_HOST`, `PROCESS_DIED`), and `Engine.UpdateRepoRuntimeStatus` clears `r.DiscardPending = false` when `st.IsClean && r.Lock == LockIdle`.
- State: `CLEAN`

### Iteration 4 — `security_audit`
- Verified `Dispatcher.checkPreClaimBarrier` (`iazio-agent/internal/worker/dispatcher.go`) emits `ClaimRequest{HostID: d.hostID, JobTypes: []string{JobTypeToolsUpdate}}` when `len(idle) == 0 && d.toolUpdater != nil`, and `resolveJobWorktree` returns `(".", d.invoker)` for `JobTypeToolsUpdate`.
- Verified `Server.listSchedules` (`iazio-harness-api/internal/httpserver/server.go`) honors `?include_dispatch_jobs=false` / `0`, and `GlobalQueueScreen._refreshData()` (`iazio-web/lib/screens/global_queue_screen.dart`) filters out `job_`/`job-` and `agent.tools.update` dispatch pseudo-schedules while `Schedule.isFinished` (`iazio-web/lib/models/harness.dart`) recognizes all terminal timeout/expiry/failure statuses.
- State: `CLEAN`

### Iteration 5 — `final_convergence`
- Verified unit, widget, and `-race` E2E tests pass across `iazio-harness-api` (`TestClaimWithWorktrees_ToolsUpdateFleetJob_BypassesWorktreeSlotAndDriftDetected_StoryGOV5195`, `TestEngine_DuplicateKindResumeAndPhantomJobReconcile_StoryGOV5195`, `TestE2E_GlobalQueue_ToolsUpdateClaimReconcileAndEngineHeartbeatIsolation_StoryGOV5195`), `iazio-agent` (`TestDispatcher_ToolsUpdateClaimableWhenAllWorktreesDirty_StoryGOV5195`), and `iazio-web` (`test/widget/global_queue_screen_test.dart`), and Wiki Hub pages (`iazio-infra/wiki/Iazio-Harness-API.md`, `iazio-infra/wiki/Iazio-Agent.md`, `iazio-infra/wiki/Iazio-Web.md`) pass validation.
- State: `CLEAN`

## 3. Mandatory DoD ↔ Disk Checklist (STORY-GOV-2213)

| Criterion | On-disk evidence (path or symbol) | Status |
|---|---|---|
| **AC-1**: Host-Level `agent.tools.update` (`repo == "fleet"`) Claiming & Heartbeat Tool-Update Reconciliation | `iazio-harness-api/internal/dispatch/queue.go` (`isHostLevelToolsUpdateJob`, `ClaimWithWorktrees`); `iazio-harness-api/internal/control/engine.go` (`AllocateDispatchJobSlot`, `BindDispatchJobRepo`); `iazio-harness-api/internal/api/handlers/agent_jobs.go` (`ClaimHandler`, `CompleteHandler`); `iazio-harness-api/internal/httpserver/server.go` (`reconcileQueueWithEngine`, `supersedePendingToolUpdateJobsForHost`, `updateEagerReadiness`) | **MET** |
| **AC-2**: Prevention & Cleanup of Phantom Rehydrated Engine Jobs (`job-<N>`) in `s.Queue` | `iazio-harness-api/internal/api/handlers/agent_jobs.go` (`HeartbeatHandler`, `CompleteHandler`); `iazio-harness-api/internal/control/engine.go` (`isExternalDispatchJobActiveLocked`, `PostExit`, `ApplyFinish`); `iazio-harness-api/internal/httpserver/server.go` (`completeOrReleaseEngineJobInQueue`, `postExit`, `finish`) | **MET** |
| **AC-3**: Duplicate `KindResume` Schedule Deduplication, Sibling Completion Reconciliation & `DiscardPending` Cleanup | `iazio-harness-api/internal/control/engine.go` (`reapExpiredLocked`, `resumeRepoInPlaceLocked`, `ResumeJob`, `UpdateRepoRuntimeStatus`); `iazio-harness-api/internal/control/engine_test.go` (`TestEngine_DuplicateKindResumeAndPhantomJobReconcile_StoryGOV5195`) | **MET** |
| **AC-4**: `iazio-agent` Host-Level `agent.tools.update` Claiming When Worktrees Are Dirty/Busy | `iazio-agent/internal/worker/dispatcher.go` (`checkPreClaimBarrier`, `resolveJobWorktree`); `iazio-agent/internal/worker/dispatcher_test.go` (`TestDispatcher_ToolsUpdateClaimableWhenAllWorktreesDirty_StoryGOV5195`) | **MET** |
| **AC-5**: `iazio-harness-api` & `iazio-web` Schedule Filtering & Terminal Status Classification | `iazio-harness-api/internal/httpserver/server.go` (`listSchedules`); `iazio-web/lib/models/harness.dart` (`Schedule.isFinished`); `iazio-web/lib/screens/global_queue_screen.dart` (`_refreshData`); `iazio-web/test/widget/global_queue_screen_test.dart` | **MET** |
| **AC-6**: Wiki Hub Sync, Multi-Agent Review, Git-Flow & E2E Verification | `iazio-infra/wiki/Iazio-Harness-API.md`, `iazio-infra/wiki/Iazio-Agent.md`, `iazio-infra/wiki/Iazio-Web.md`; `iazio-harness-api/test/e2e/global_queue_resilience_e2e_test.go` (`TestE2E_GlobalQueue_ToolsUpdateClaimReconcileAndEngineHeartbeatIsolation_StoryGOV5195`) | **MET** |

## 4. Verdict

**CLEAN.**
