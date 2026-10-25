// Command iazio-agent is the user-level runner supervisor.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/viovy/iazio-agent/internal/auth"
	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/elasticlog"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/preflight"
	"github.com/viovy/iazio-agent/internal/reaper"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/service"
	"github.com/viovy/iazio-agent/internal/sessionstore"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-agent/internal/telemetry"
	"github.com/viovy/iazio-agent/internal/watchdog"
	"github.com/viovy/iazio-agent/internal/worker"
	"github.com/viovy/iazio-agent/internal/workspace"
)

func main() {
	flushTracer := telemetry.InitGlobalTracer("iazio-agent", "")
	defer flushTracer()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var initialUpdateDelay = 5 * time.Second
var isTesting = false

type safeWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *safeWriter) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	if stdout != nil {
		stdout = &safeWriter{w: stdout}
	}
	if stderr != nil {
		stderr = &safeWriter{w: stderr}
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: iazio-agent version|auth|service|update|run|probe")
	}
	_, span := telemetry.StartSpan(ctx, "agent."+args[0],
		telemetry.WithKind("INTERNAL"),
		telemetry.WithTag("cli.command", args[0]),
	)
	defer span.End()
	switch args[0] {
	case "version", "--version", "-v":
		fmt.Fprintln(stdout, formatVersion())
		return nil
	case "help", "--help", "-h":
		fmt.Fprintln(stdout, "usage: iazio-agent version|auth|service|update|run|probe")
		return nil
	case "probe", "health", "--healthcheck":
		return runProbe(ctx, args[1:], stdout, stderr, getenv)
	case "auth":
		return runAuth(ctx, args[1:], stdout, getenv)
	case "service":
		return runService(args[1:], stdout, getenv)
	case "update":
		return runUpdate(ctx, args[1:], stdout, stderr, getenv)
	case "run":
		defer func() {
			if !isTesting && getenv("IAZIO_AGENT_NONINTERACTIVE") != "1" {
				if (runtime.GOOS == "linux" && service.IsSystemdAvailable()) || runtime.GOOS == "darwin" {
					home, _ := os.UserHomeDir()
					_ = service.ReviveCheck(home, execCommand)
				}
			}
		}()
		kind := supervisor.HostKind(getenv("IAZIO_AGENT_HOST_KIND"))
		fmt.Fprintln(stdout, kind)
		api := resolveLocalAPI(getenv)
		profile := getenv("IAZIO_AGENT_PROFILE")
		updateInterval := 30 * time.Minute
		if raw := getenv("IAZIO_AGENT_UPDATE_INTERVAL"); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil {
				updateInterval = d
			} else if raw == "0" || strings.ToLower(raw) == "disabled" {
				updateInterval = 0
			}
		}
		updateSuite := true
		if raw := getenv("IAZIO_AGENT_UPDATE_SUITE"); raw != "" {
			updateSuite = raw == "1" || strings.ToLower(raw) == "true"
		}

		for i := 1; i < len(args); i++ {
			switch {
			case args[i] == "--profile" && i+1 < len(args):
				i++
				profile = args[i]
			case strings.HasPrefix(args[i], "--profile="):
				profile = strings.TrimPrefix(args[i], "--profile=")
			case args[i] == "--api" && i+1 < len(args):
				i++
				api = args[i]
			case strings.HasPrefix(args[i], "--api="):
				api = strings.TrimPrefix(args[i], "--api=")
			case args[i] == "--update-interval" && i+1 < len(args):
				i++
				if d, err := time.ParseDuration(args[i]); err == nil {
					updateInterval = d
				} else if args[i] == "0" || strings.ToLower(args[i]) == "disabled" {
					updateInterval = 0
				}
			case strings.HasPrefix(args[i], "--update-interval="):
				val := strings.TrimPrefix(args[i], "--update-interval=")
				if d, err := time.ParseDuration(val); err == nil {
					updateInterval = d
				} else if val == "0" || strings.ToLower(val) == "disabled" {
					updateInterval = 0
				}
			case args[i] == "--update-suite":
				updateSuite = true
			}
		}
		if profile == "" {
			profile = "generic"
		}
		host := resolveHostID(getenv, os.Hostname, nil)
		esURL := strings.TrimSpace(getenv("IAZIO_ELASTICSEARCH_URL"))
		if esURL == "" {
			if home, err := os.UserHomeDir(); err == nil && home != "" {
				cfgFile := filepath.Join(home, ".iazio", "agent.json")
				if data, err := os.ReadFile(cfgFile); err == nil {
					var raw struct {
						ElasticsearchURL string `json:"elasticsearch_url"`
					}
					if err := json.Unmarshal(data, &raw); err == nil {
						esURL = strings.TrimSpace(raw.ElasticsearchURL)
					}
				}
			}
		}
		var sink *elasticlog.Sink
		if esURL != "" {
			sink = elasticlog.New(elasticlog.Config{
				URL:         esURL,
				ServiceName: "iazio-agent",
				Version:     formatVersion(),
				HostName:    host,
			})
			defer func() { _ = sink.Close() }()
			sink.Info("iazio-agent supervisor starting", map[string]any{
				"host_kind": string(kind),
				"api_url":   api,
				"profile":   profile,
			})
		}
		if ctx.Err() == nil {
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			watchdog.StartSystemdWatchdog(runCtx)
			if home, err := os.UserHomeDir(); err == nil && home != "" {
				binPath := filepath.Join(home, ".iazio", "bin", "iazio-agent")
				if runtime.GOOS == "windows" {
					binPath += ".exe"
				}
				go func() {
					_ = service.ReconcileServiceConfiguration(home, binPath, nil)
				}()
			}

			sup := supervisor.New(time.Now)
			hostGate := supervisor.NewHostExecutionAndUpdateGate()
			var client controlplane.Client

			var updatingTools atomic.Bool
			getTools := func() []controlplane.Tool {
				home, _ := os.UserHomeDir()
				dirs := inventory.DefaultDirs(getenv("PATH"), home, runtime.GOOS)
				found := inventory.Scan(dirs)
				classified, _ := inventory.Classify(found, nil)
				var tools []controlplane.Tool
				for _, t := range classified {
					tools = append(tools, controlplane.Tool{
						Name:             t.Name,
						Path:             t.Path,
						Version:          t.Version,
						Status:           t.Status,
						ServiceInstalled: t.ServiceInstalled,
						ServiceType:      t.ServiceType,
						ServiceStatus:    t.ServiceStatus,
						Capabilities:     t.Capabilities,
						Installed:        t.Installed,
						InstallationType: t.InstallationType,
						ActiveState:      t.ActiveState,
						WatchdogEnabled:  t.WatchdogEnabled,
						ConfigStatus:     t.ConfigStatus,
					})
				}
				return tools
			}

			repoPollInterval := 2 * time.Minute
			if raw := getenv("IAZIO_AGENT_REPO_POLL_INTERVAL"); raw != "" {
				if d, err := time.ParseDuration(raw); err == nil {
					repoPollInterval = d
				} else {
					fmt.Fprintf(stderr, "[RepoMonitor] WARN: invalid IAZIO_AGENT_REPO_POLL_INTERVAL %q: %v. Defaulting to 2m\n", raw, err)
				}
			}
			if repoPollInterval < 1*time.Minute || repoPollInterval > 5*time.Minute {
				fmt.Fprintf(stderr, "[RepoMonitor] WARN: IAZIO_AGENT_REPO_POLL_INTERVAL %v outside permitted range [1m..5m]; clamping\n", repoPollInterval)
				if repoPollInterval < 1*time.Minute {
					repoPollInterval = 1 * time.Minute
				} else {
					repoPollInterval = 5 * time.Minute
				}
			}

			var eagerUpdater *supervisor.EagerToolUpdater

			getRepos := func() ([]supervisor.RepoConfigItem, error) {
				if api == "" {
					return nil, nil
				}
				fetchCtx, fetchCancel := context.WithTimeout(runCtx, 10*time.Second)
				defer fetchCancel()
				detail, err := client.GetHost(fetchCtx, host)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch host repos: %w", err)
				}
				if len(detail.Repos) == 0 {
					return nil, nil
				}
				var items []supervisor.RepoConfigItem
				for _, r := range detail.Repos {
					p := r.WorktreePath
					if p == "" {
						p = r.Path
					}
					if p != "" {
						items = append(items, supervisor.RepoConfigItem{
							Path:  p,
							Alias: r.Alias,
						})
					}
				}
				return items, nil
			}

			repoMonitor := supervisor.NewRepoStateMonitor(
				sup,
				host,
				repoPollInterval,
				getRepos,
				func(c context.Context, h string, st []controlplane.RepoRuntimeStatus) error {
					return client.PostRepoStatus(c, h, st)
				},
				func(c context.Context, nr controlplane.NotificationRecord) error {
					if api == "" {
						return nil
					}
					return client.PostNotification(c, nr)
				},
				func(snap supervisor.FleetStateSnapshot) {
					if eagerUpdater != nil && !updatingTools.Load() {
						eagerUpdater.TriggerEvaluation("periodic_fleet_clean")
						go func() {
							agentUpdated, _, _ := eagerUpdater.EvaluateAndTrigger(runCtx, nil, false)
							if agentUpdated {
								restartOrExecSelf(stdout)
								cancel()
							}
						}()
					}
				},
			)

			var willRestart atomic.Bool

			performUpdateFn := func(c context.Context, targetTools []string, force bool, targetVersion ...string) (supervisor.UpdateResult, error) {
				updatingTools.Store(true)
				if api != "" {
					if err := client.Heartbeat(c, host, getTools(), false); err != nil && stderr != nil {
						fmt.Fprintf(stderr, "[agent] pre-update heartbeat error: %v\n", err)
					}
				}
				defer func() {
					if !willRestart.Load() {
						updatingTools.Store(false)
						if api != "" {
							if err := client.Heartbeat(c, host, getTools(), false); err != nil && stderr != nil {
								fmt.Fprintf(stderr, "[agent] post-update heartbeat error: %v\n", err)
							}
						}
					}
				}()

				var tv string
				if len(targetVersion) > 0 {
					tv = targetVersion[0]
				}

				opts := UpdateOptions{
					Suite:         updateSuite,
					Profile:       profile,
					API:           api,
					Force:         force,
					TargetTools:   targetTools,
					TargetVersion: tv,
				}
				res, err := performUpdateWithResult(c, opts, stdout, stderr, getenv)
				if res.AgentUpdated {
					willRestart.Store(true)
				}
				var aggregateErr error
				if err != nil {
					aggregateErr = err
				} else if force && len(res.Errors) > 0 {
					aggregateErr = res.Errors[0]
				}
				return supervisor.UpdateResult{
					UpdatedTools: res.UpdatedTools,
					FailedTools:  res.FailedTools,
					Errors:       res.Errors,
					AgentUpdated: res.AgentUpdated,
				}, aggregateErr
			}

			eagerUpdater = supervisor.NewEagerToolUpdater(
				sup,
				repoMonitor,
				host,
				func(c context.Context, targetTools []string, force bool) (supervisor.UpdateResult, error) {
					return performUpdateFn(c, targetTools, force)
				},
				func(c context.Context, nr controlplane.NotificationRecord) error {
					if api == "" {
						return nil
					}
					return client.PostNotification(c, nr)
				},
			)
			eagerUpdater.SetDetailedUpdateFunc(performUpdateFn)
			eagerUpdater.SetHostGate(hostGate)

			execDetector := supervisor.NewMultiSignalExecutionDetector(sup, hostGate, getRepos, nil)
			eagerUpdater.SetExecutionDetector(execDetector)
			eagerUpdater.SetReadinessReporter(func(c context.Context, h string, r supervisor.EagerUpdateReadinessReport) error {
				if api == "" {
					return nil
				}
				return client.PostEagerReadiness(c, h, r)
			})
			eagerUpdater.StartHybridTriggerLoop(runCtx, func() {
				restartOrExecSelf(stdout)
				cancel()
			})
			eagerUpdater.TriggerEvaluation("agent_startup")

			if api != "" {
				driftFetcher := &supervisor.HTTPDriftFetcher{
					Client:  &http.Client{Timeout: 10 * time.Second},
					API:     api,
					Profile: profile,
					OS:      runtime.GOOS,
					Arch:    runtime.GOARCH,
				}
				isToolInBackoff := func(tool string) bool {
					return eagerUpdater != nil && (eagerUpdater.IsToolInBackoff(tool))
				}
				getInstallRoots := func(manifest *supervisor.FleetManifest) string {
					return resolveInstallDir(ManifestInstallRoots{
						Unix:    ManifestInstallRootPaths{Primary: manifest.InstallRoots.Unix.Primary, Fallback: manifest.InstallRoots.Unix.Fallback},
						Windows: ManifestInstallRootPaths{Primary: manifest.InstallRoots.Windows.Primary, Fallback: manifest.InstallRoots.Windows.Fallback},
					}, getenv)
				}
				eagerUpdater.CheckDrift, eagerUpdater.ListSuiteTools = buildEagerHooks(
					updateSuite,
					driftFetcher,
					isToolInBackoff,
					getInstallRoots,
					runtime.GOOS,
				)
			}

			// Start repoMonitor AFTER eagerUpdater is fully constructed to eliminate initialization race
			repoMonitor.Start(runCtx)

			var lastUpdateCheck time.Time
			var lastUpdateMu sync.Mutex

			var doToolUpdateWithOpts func(ctx context.Context, opts supervisor.TriggerOptions) (supervisor.UpdateResult, bool, error)

			doToolUpdateDetailed := func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) (supervisor.UpdateResult, bool, error) {
				var tv string
				if len(targetVersion) > 0 && targetVersion[0] != "" {
					tv = targetVersion[0]
				}
				return doToolUpdateWithOpts(ctx, supervisor.TriggerOptions{
					TargetTools:   targetTools,
					Force:         force,
					TargetVersion: tv,
					BypassDirty:   false,
				})
			}

			doToolUpdateWithOpts = func(ctx context.Context, opts supervisor.TriggerOptions) (supervisor.UpdateResult, bool, error) {
				tracer := telemetry.GlobalTracer()
				_, span := tracer.StartSpan(ctx, "agent.tools.update")
				defer span.End()
				span.SetTag("host_id", host)
				span.SetTag("force", fmt.Sprintf("%t", opts.Force))
				if len(opts.TargetTools) > 0 {
					span.SetTag("target_tools", strings.Join(opts.TargetTools, ","))
				}
				if opts.TargetVersion != "" {
					span.SetTag("target_version", opts.TargetVersion)
				}

				if eagerUpdater != nil {
					res, deferred, err := eagerUpdater.EvaluateAndTriggerWithOpts(ctx, opts)
					if err != nil {
						if errors.Is(err, supervisor.ErrUpdateInProgress) {
							span.SetTag("result", "queued")
							return res, true, err
						}
						span.SetTag("result", "error")
						span.SetError(err)
						return res, false, err
					}
					if deferred {
						span.SetTag("result", "deferred")
						return res, true, nil
					}
					if len(res.UpdatedTools) > 0 || res.AgentUpdated {
						span.SetTag("result", "updated")
					} else {
						span.SetTag("result", "up-to-date")
					}
					return res, false, nil
				}

				if !opts.BypassDirty && !areAllReposCleanAndHarnessIdle(ctx, client, sup, host, api, opts.Force, stdout) {
					span.SetTag("result", "deferred")
					if sink != nil {
						sink.Info("Tool update check deferred: workstation is busy or worktree dirty", map[string]any{
							"host":   host,
							"action": "tool_update_deferred",
						})
					}
					return supervisor.UpdateResult{}, true, nil
				}

				updatingTools.Store(true)
				if api != "" {
					if err := client.Heartbeat(ctx, host, getTools(), false); err != nil && stderr != nil {
						fmt.Fprintf(stderr, "[agent] fallback pre-update heartbeat error: %v\n", err)
					}
				}
				var willRestartFallback bool
				defer func() {
					if !willRestartFallback {
						updatingTools.Store(false)
						if api != "" {
							if err := client.Heartbeat(ctx, host, getTools(), false); err != nil && stderr != nil {
								fmt.Fprintf(stderr, "[agent] fallback post-update heartbeat error: %v\n", err)
							}
						}
					}
				}()

				updateOpts := UpdateOptions{
					Suite:         updateSuite,
					Profile:       profile,
					API:           api,
					Force:         opts.Force,
					TargetTools:   opts.TargetTools,
					TargetVersion: opts.TargetVersion,
				}
				upRes, err := performUpdateWithResult(ctx, updateOpts, stdout, stderr, getenv)
				res := supervisor.UpdateResult{
					UpdatedTools: upRes.UpdatedTools,
					AgentUpdated: upRes.AgentUpdated,
				}
				if err != nil {
					span.SetTag("result", "error")
					span.SetError(err)
					return res, false, err
				}
				if upRes.AgentUpdated {
					willRestartFallback = true
				}
				if len(res.UpdatedTools) > 0 || res.AgentUpdated {
					span.SetTag("result", "updated")
				} else {
					span.SetTag("result", "up-to-date")
				}
				return res, false, nil
			}

			doToolUpdate := func(ctx context.Context, targetTools []string, force bool, targetVersion ...string) bool {
				res, deferred, err := doToolUpdateDetailed(ctx, targetTools, force, targetVersion...)
				if err != nil || deferred {
					return false
				}
				return res.AgentUpdated
			}

			var pendingRemoteMu sync.Mutex
			var pendingRemoteTools []string
			var pendingRemoteForce bool
			var pendingRemoteTargetVersion string
			var hasPendingRemote bool

			if updateInterval > 0 {
				go func() {
					select {
					case <-runCtx.Done():
						return
					case <-time.After(initialUpdateDelay):
					}

					checkOnce := func() bool {
						pendingRemoteMu.Lock()
						t := pendingRemoteTools
						f := pendingRemoteForce
						v := pendingRemoteTargetVersion
						isRemote := hasPendingRemote
						if isRemote {
							hasPendingRemote = false
							pendingRemoteTools = nil
							pendingRemoteForce = false
							pendingRemoteTargetVersion = ""
						}
						pendingRemoteMu.Unlock()

						if isRemote {
							return doToolUpdate(runCtx, t, f, v)
						}
						return doToolUpdate(runCtx, nil, false)
					}

					if checkOnce() {
						restartOrExecSelf(stdout)
						cancel()
						return
					}

					ticker := time.NewTicker(updateInterval)
					defer ticker.Stop()
					for {
						select {
						case <-runCtx.Done():
							return
						case <-ticker.C:
							if checkOnce() {
								restartOrExecSelf(stdout)
								cancel()
								return
							}
						}
					}
				}()
			}
			var wd *watchdog.Watchdog
			client = controlplane.Client{
				BaseURL: api,
				Presence: func() string {
					if updatingTools.Load() {
						return "ONLINE_UPDATING_TOOLS"
					}
					return "ONLINE"
				},
				Tools: getTools,
				SupportedCapabilities: []string{
					"user_service_watchdog_v2",
					"deterministic_resume_4tuple_v1",
					"agy_conversation_pinning_v1",
					"workspace_recovery_modes_v1",
					"background_task_watchdog_v1",
				},
				Version:   formatVersion(),
				CommitSHA: commit,
				OnToolUpdate: func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
					var tv string
					if len(targetVersion) > 0 {
						tv = targetVersion[0]
					}
					if stdout != nil {
						fmt.Fprintf(stdout, "[supervisor] remote tool update requested: tools=%v force=%v target_version=%s\n", tools, force, tv)
					}
					if eagerUpdater != nil {
						eagerUpdater.TriggerEvaluation("control_plane_broadcast")
					}
					pendingRemoteMu.Lock()
					pendingRemoteTools = tools
					pendingRemoteForce = force
					pendingRemoteTargetVersion = tv
					hasPendingRemote = true
					pendingRemoteMu.Unlock()

					go func() {
						pendingRemoteMu.Lock()
						t := pendingRemoteTools
						f := pendingRemoteForce
						v := pendingRemoteTargetVersion
						pendingRemoteTools = nil
						pendingRemoteForce = false
						pendingRemoteTargetVersion = ""
						hasPendingRemote = false
						pendingRemoteMu.Unlock()

						if doToolUpdate(runCtx, t, f, v) {
							restartOrExecSelf(stdout)
							cancel()
						}
					}()
					return nil
				},
				OnCommand: func(ctx context.Context, cmd controlplane.CommandEnvelope) error {
					switch cmd.Type {
					case "TRIGGER_IDLE_UPDATE":
						go func() {
							if doToolUpdate(runCtx, nil, false) {
								restartOrExecSelf(stdout)
								cancel()
							}
						}()
					case "PING":
						if stdout != nil {
							fmt.Fprintf(stdout, "[supervisor] received PING command (%s); host is reachable\n", cmd.ID)
						}
						go func() {
							_ = client.PostPingAck(runCtx, host, cmd.ID, version)
						}()
					case "CHECK_FLEET_GIT_STATUS":
						go func() {
							var target string
							if err := json.Unmarshal(cmd.Payload, &target); err != nil {
								target = strings.Trim(strings.TrimSpace(string(cmd.Payload)), "\"")
							}
							if target != "" {
								st := probeRepoRuntime(runCtx, target, filepath.Base(target))
								_ = client.PostRepoStatus(runCtx, host, []controlplane.RepoRuntimeStatus{st})
								return
							}
							detail, err := client.GetHost(runCtx, host)
							if err != nil || len(detail.Repos) == 0 {
								return
							}
							var statuses []controlplane.RepoRuntimeStatus
							for _, r := range detail.Repos {
								p := r.WorktreePath
								if p == "" {
									p = r.Path
								}
								if p != "" {
									statuses = append(statuses, probeRepoRuntime(runCtx, p, r.Alias))
								}
							}
							if len(statuses) > 0 {
								_ = client.PostRepoStatus(runCtx, host, statuses)
							}
						}()
					case "FETCH_METADATA_YML":
						go func() {
							var target string
							if err := json.Unmarshal(cmd.Payload, &target); err != nil {
								target = strings.Trim(strings.TrimSpace(string(cmd.Payload)), "\"")
							}
							if target != "" {
								st := probeRepoRuntime(runCtx, target, filepath.Base(target))
								_ = client.PostRepoStatus(runCtx, host, []controlplane.RepoRuntimeStatus{st})
							}
						}()
					case "FORCE_CLEAN_GIT", "HEAL_GIT_REPO":
						go func() {
							var target string
							if err := json.Unmarshal(cmd.Payload, &target); err != nil {
								target = strings.Trim(strings.TrimSpace(string(cmd.Payload)), "\"")
							}
							if target != "" {
								healGitRepo(runCtx, target)
								st := probeRepoRuntime(runCtx, target, filepath.Base(target))
								_ = client.PostRepoStatus(runCtx, host, []controlplane.RepoRuntimeStatus{st})
							}
						}()
					case "KILL_BACKGROUND_TASKS":
						go func() {
							var jobID string
							if err := json.Unmarshal(cmd.Payload, &jobID); err != nil {
								jobID = strings.Trim(strings.TrimSpace(string(cmd.Payload)), "\"")
							}
							if stderr != nil {
								fmt.Fprintf(stderr, "[supervisor] remote kill background tasks received: jobID=%s\n", jobID)
							}
							for _, rj := range sup.RunningJobs() {
								if jobID == "" || rj.JobID == jobID {
									sup.Cancel(rj.WorktreePath, "KILL_BACKGROUND_TASKS")
								}
							}
						}()
					}
					return nil
				},
				ActiveJobs: func() []controlplane.ActiveJob {
					var jobs []controlplane.ActiveJob
					seen := make(map[string]bool)
					for _, rj := range sup.RunningJobs() {
						if rj.JobID != "" && !seen[rj.JobID] {
							seen[rj.JobID] = true
							jobs = append(jobs, controlplane.ActiveJob{
								JobID:        rj.JobID,
								WorktreePath: rj.WorktreePath,
								PID:          rj.PID,
							})
						}
					}
					if wd != nil {
						for _, wj := range wd.ActiveJobs() {
							if wj.JobID != "" && !seen[wj.JobID] {
								seen[wj.JobID] = true
								jobs = append(jobs, controlplane.ActiveJob{
									JobID:        wj.JobID,
									WorktreePath: wj.WorktreePath,
									PID:          wj.PID,
								})
							}
						}
					}
					return jobs
				},
				OnTick: func(ctx context.Context, hostID string) error {
					reaped := sup.Tick()
					for _, r := range reaped {
						if stderr != nil {
							fmt.Fprintf(stderr, "[supervisor] reaped stalled or terminated process %q (reason: %s)\n",
								r.JobID, r.Reason)
						}
						// If watchdog is actively managing or resuming this job, do not abandon it to control plane
						if wd != nil && wd.IsTracked(r.JobID) {
							continue
						}
						if r.JobID != "" {
							_ = client.AbandonJob(ctx, r.JobID, r.Reason)
						}
					}
					// Host resource telemetry check
					if home, err := os.UserHomeDir(); err == nil && home != "" {
						_, _ = preflight.FreeSpace(home)
					}
					// If harness finished and idle, trigger eager update check if fleet clean
					if len(reaped) > 0 && eagerUpdater != nil && !updatingTools.Load() {
						go func() {
							repoMonitor.InspectFleet(runCtx)
							agentUpdated, _, _ := eagerUpdater.EvaluateAndTrigger(runCtx, nil, false)
							if agentUpdated {
								restartOrExecSelf(stdout)
								cancel()
							}
						}()
					}
					return nil
				},
			}

			if api != "" {
				cpClient := worker.NewHTTPControlPlaneClient(api, "", nil)
				cliInvoker := harness.NewCLIInvoker("", "")
				disp := worker.NewDispatcher(cpClient, cliInvoker, host)
				disp.SetHostGate(hostGate)
				disp.SetPreDispatchHook(func(ctx context.Context, job *worker.Job) error {
					if eagerUpdater != nil && !updatingTools.Load() {
						rep := eagerUpdater.EvaluateReadiness(ctx)
						if rep.EagerUpdateState == supervisor.StateUpToDate {
							res, _, err := eagerUpdater.EvaluateAndTriggerWithOpts(ctx, supervisor.TriggerOptions{
								BypassDirty: false,
								Force:       false,
							})
							if err == nil && res.AgentUpdated {
								restartOrExecSelf(stdout)
								cancel()
							}
						}
					}
					return nil
				})
				disp.SetPostJobHook(func(ctx context.Context, job *worker.Job) {
					if eagerUpdater != nil {
						eagerUpdater.TriggerEvaluation("post_job_completion")
					}
				})
				disp.SetToolUpdater(func(ctx context.Context, tools []string, force bool, targetVersion ...string) error {
					var tv string
					if len(targetVersion) > 0 {
						tv = targetVersion[0]
					}
					if stdout != nil {
						fmt.Fprintf(stdout, "[dispatcher] tool update job executing: tools=%v force=%v target_version=%s\n", tools, force, tv)
					}
					// Dispatched agent.tools.update jobs:
					// 1. Pass the actual caller force flag (unforced updates do not force-reinstall, N2).
					// 2. Bypass dirty worktree check so paused/healing repos do not block runner host updates.
					res, deferred, err := doToolUpdateWithOpts(ctx, supervisor.TriggerOptions{
						TargetTools:   tools,
						Force:         force,
						TargetVersion: tv,
						BypassDirty:   true,
					})
					if err != nil {
						if errors.Is(err, supervisor.ErrQueueFull) {
							return fmt.Errorf("tool update request rejected: %w", err)
						}
						if errors.Is(err, supervisor.ErrUpdateInProgress) {
							if stdout != nil {
								fmt.Fprintf(stdout, "[dispatcher] tool update already in progress; request queued (N3)\n")
							}
							return worker.ErrJobDeferred
						}
						return fmt.Errorf("tool update failed: %w", err)
					}
					if deferred {
						if stdout != nil {
							fmt.Fprintf(stdout, "[dispatcher] tool update deferred; request queued for execution when harness is idle (N3)\n")
						}
						return worker.ErrJobDeferred
					}
					if res.AgentUpdated {
						restartOrExecSelf(stdout)
						cancel()
					}
					return nil
				})
				disp.SetWorktreeResolver(func(repoName string) string {
					return resolveWorktreeForRepo(runCtx, client, host, repoName, getenv)
				})
				go func() {
					pollTicker := time.NewTicker(2 * time.Second)
					defer pollTicker.Stop()
					for {
						select {
						case <-runCtx.Done():
							return
						case <-pollTicker.C:
							for {
								processed, err := disp.PollAndProcess(runCtx)
								if err != nil || !processed || runCtx.Err() != nil {
									break
								}
							}
						}
					}
				}()

				go func() {
					defer func() {
						if r := recover(); r != nil {
							fmt.Fprintf(os.Stderr, "[SIR: PANIC_RECOVERED] recovered panic in repo status probe: %v\n", r)
						}
					}()
					repoTicker := time.NewTicker(20 * time.Second)
					defer repoTicker.Stop()
					for {
						select {
						case <-runCtx.Done():
							return
						case <-repoTicker.C:
							detail, err := client.GetHost(runCtx, host)
							if err != nil || len(detail.Repos) == 0 {
								continue
							}
							var statuses []controlplane.RepoRuntimeStatus
							for _, r := range detail.Repos {
								p := r.WorktreePath
								if p == "" {
									p = r.Path
								}
								if p == "" {
									continue
								}
								statuses = append(statuses, probeRepoRuntime(runCtx, p, r.Alias))
							}
							if len(statuses) > 0 {
								_ = client.PostRepoStatus(runCtx, host, statuses)
							}

							// Autonomic idle tool update trigger:
							// When all repos are open / idle and git is clean, trigger tool update check
							if areAllReposCleanAndHarnessIdle(runCtx, client, sup, host, api, false, nil) {
								lastUpdateMu.Lock()
								elapsed := time.Since(lastUpdateCheck)
								pendingRemoteMu.Lock()
								isRemote := hasPendingRemote
								pendingRemoteMu.Unlock()
								shouldCheck := isRemote || elapsed >= 5*time.Minute
								if shouldCheck {
									lastUpdateCheck = time.Now()
								}
								lastUpdateMu.Unlock()

								if shouldCheck && !updatingTools.Load() {
									go func() {
										pendingRemoteMu.Lock()
										t := pendingRemoteTools
										f := pendingRemoteForce
										v := pendingRemoteTargetVersion
										rem := hasPendingRemote
										pendingRemoteMu.Unlock()

										if rem {
											if doToolUpdate(runCtx, t, f, v) {
												pendingRemoteMu.Lock()
												hasPendingRemote = false
												pendingRemoteTools = nil
												pendingRemoteForce = false
												pendingRemoteTargetVersion = ""
												pendingRemoteMu.Unlock()
												restartOrExecSelf(stdout)
												cancel()
											}
										} else {
											if doToolUpdate(runCtx, nil, false) {
												restartOrExecSelf(stdout)
												cancel()
											}
										}
									}()
								}
							}
						}
					}
				}()
			}

			wdRunner := supervisor.NewOSRunner()
			spawner := func(sCtx context.Context, job watchdog.TrackedJob) (int, error) {
				argv := []string{
					"iazio-harness", "run",
					"--job-id", job.JobID,
					"--api-url", job.APIURL,
					"--worktree-path", job.WorktreePath,
					"--docs-hub-path", job.DocsHubPath,
					"--kind", "resume",
				}
				if job.ConversationID != "" {
					argv = append(argv, "--resume-conversation", job.ConversationID)
				}
				if job.AttachToolPID > 0 {
					argv = append(argv, "--attach-tool-pid", strconv.Itoa(job.AttachToolPID))
				}
				newPID, err := wdRunner.Start(argv)
				return newPID, err
			}
			wd = watchdog.New(watchdog.Options{
				Runner:  wdRunner,
				Spawner: spawner,
				OnOrphanTool: func(job watchdog.TrackedJob) {
					msg := fmt.Sprintf("\n[ASE-SUPERVISOR] [watchdog.orphan_tool_detected] Harness process (PID %d) terminated, but AI tool process (ToolPID %d, engine: %s) is STILL RUNNING for conversation %s in %s. Attaching new supervisor...\n",
						job.PID, job.ToolPID, job.Engine, job.ConversationID, job.WorktreePath)
					_ = client.PostChunk(runCtx, job.JobID, "stdout", msg)
				},
				OnAutoResume: func(job watchdog.TrackedJob, reason string, attempt, max int) {
					msg := fmt.Sprintf("\n[ASE-SUPERVISOR] [watchdog.auto_resume] Harness process (PID %d) and AI tool terminated. Story incomplete. Auto-resuming session (conversation %s, attempt %d/%d) with prompt \"resume\"...\n",
						job.PID, job.ConversationID, attempt, max)
					_ = client.PostChunk(runCtx, job.JobID, "stdout", msg)
				},
			})
			wd.StartWatchguard(runCtx, 5*time.Second)

			onJob := func(a controlplane.Assignment) error {
				if a.ID == "" {
					return nil
				}
				_, span := telemetry.StartSpan(runCtx, "agent.execute_lease",
					telemetry.WithTag("job.id", a.ID),
					telemetry.WithTag("worktree.path", a.WorktreePath),
					telemetry.WithTag("story.id", a.StoryID),
				)
				defer span.End()

				// Repo lock verification: refuse duplicate/concurrent jobs in the same worktree
				lockInfo, lockErr := repolock.Read(a.WorktreePath)
				if lockErr == nil && lockInfo.JobID != "" && lockInfo.JobID != a.ID {
					checkPID := lockInfo.PID
					if lockInfo.HarnessPID > 0 {
						checkPID = lockInfo.HarnessPID
					}
					if checkPID > 0 {
						// Check if process is still alive
						p, findErr := os.FindProcess(checkPID)
						if findErr == nil && p.Signal(syscall.Signal(0)) == nil {
							if stderr != nil {
								fmt.Fprintf(stderr, "[agent] refusing concurrent job %s on %s: worktree actively locked by job %s (pid %d)\n",
									a.ID, a.WorktreePath, lockInfo.JobID, checkPID)
							}
							_ = client.AbandonJob(runCtx, a.ID, "REPO_BUSY")
							return fmt.Errorf("repository %s busy with job %s", a.WorktreePath, lockInfo.JobID)
						}
					}
				}

				convID := ResolveDispatchConversationID(a, lockInfo.ConversationID, lockInfo.StoryID, lockInfo.Status)

				// Pre-resume workspace & session sanitization pass
				if a.WorktreePath != "" {
					reapRes, reapErr := reaper.ReapWorkspaceOrphansAndLocks(runCtx, a.WorktreePath, convID)
					if reapErr != nil {
						span.SetTag("reaper.error", reapErr.Error())
						if stderr != nil {
							fmt.Fprintf(stderr, "[agent] Warning: reaper encountered error for %s: %v\n", a.WorktreePath, reapErr)
						}
					}
					if reapRes != nil && (reapRes.OrphansReaped > 0 || reapRes.LocksReleased > 0) {
						span.SetTag("reaper.orphans_reaped", fmt.Sprintf("%d", reapRes.OrphansReaped))
						span.SetTag("reaper.locks_released", fmt.Sprintf("%d", reapRes.LocksReleased))
						if stderr != nil {
							fmt.Fprintf(stderr, "[agent] Pre-resume reaper: %d orphans reaped, %d session locks released for %s\n",
								reapRes.OrphansReaped, reapRes.LocksReleased, a.WorktreePath)
						}
					}

					sanRes, sanErr := workspace.SanitizeWorkspace(runCtx, a.WorktreePath)
					if sanErr != nil {
						span.SetTag("workspace.sanitize_error", sanErr.Error())
						if stderr != nil {
							fmt.Fprintf(stderr, "[agent] Warning: workspace sanitization failed for %s: %v\n", a.WorktreePath, sanErr)
						}
					}
					if sanRes != nil {
						span.SetTag("workspace.stale_locks_removed", fmt.Sprintf("%d", sanRes.StaleLocksRemoved))
						span.SetTag("workspace.preserved_files", fmt.Sprintf("%d", sanRes.Manifest.PreservedFilesCount))
						if sanRes.InterruptedGitOperation != "" {
							span.SetTag("workspace.interrupted_git_op", sanRes.InterruptedGitOperation)
						}
						if sanRes.StaleLocksRemoved > 0 && stderr != nil {
							fmt.Fprintf(stderr, "[agent] Cleaned %d stale git lock files in %s (preserved %d uncommitted files)\n",
								sanRes.StaleLocksRemoved, a.WorktreePath, sanRes.Manifest.PreservedFilesCount)
						}
					}

					// Report readiness payload to iazio-harness-api
					var reapMap map[string]any
					if reapRes != nil {
						reapMap = map[string]any{
							"orphans_reaped": reapRes.OrphansReaped,
							"locks_released": reapRes.LocksReleased,
						}
						if reapErr != nil {
							reapMap["error"] = reapErr.Error()
						}
					} else if reapErr != nil {
						reapMap = map[string]any{
							"error": reapErr.Error(),
						}
					}
					var manifestMap map[string]any
					if sanRes != nil {
						manifestMap = map[string]any{
							"clean_locks":           sanRes.Manifest.CleanLocks,
							"stale_locks_removed":   sanRes.StaleLocksRemoved,
							"interrupted_git_op":    sanRes.InterruptedGitOperation,
							"branch":                sanRes.Manifest.Branch,
							"head_sha":              sanRes.Manifest.HeadSHA,
							"preserved_files_count": sanRes.Manifest.PreservedFilesCount,
							"modified_submodules":   sanRes.Manifest.ModifiedSubmodules,
							"modified_files":        sanRes.Manifest.ModifiedFiles,
							"untracked_files":       sanRes.Manifest.UntrackedFiles,
						}
					}
					if reapMap != nil || manifestMap != nil {
						if err := client.PostJobReadiness(runCtx, a.ID, reapMap, manifestMap, nil); err != nil {
							span.SetTag("job.readiness_post_error", err.Error())
						}
					}

					// Synchronous harvest flush before resume attempt
					if convID != "" && (a.Kind == "resume" || a.ParentJobID != "") {
						if err := flushHarvestConversation(runCtx, convID); err != nil {
							span.SetTag("harvest.flush_error", err.Error())
						}
					}
				}

				var spawnExtras []string
				if a.Kind != "" {
					spawnExtras = append(spawnExtras, "--kind", a.Kind)
				}
				if convID != "" {
					spawnExtras = append(spawnExtras, "--resume-conversation", convID)
				}
				toolPID := 0
				if a.WorktreePath != "" {
					if lockInfo, err := repolock.Read(a.WorktreePath); err == nil && lockInfo.ToolPID > 0 {
						if watchdog.IsProcessAlive(lockInfo.ToolPID) {
							toolPID = lockInfo.ToolPID
							spawnExtras = append(spawnExtras, "--attach-tool-pid", strconv.Itoa(lockInfo.ToolPID))
						}
					}
				}

				err := sup.Spawn(wdRunner, a.ID, api, a.WorktreePath, a.DocsHubPath, "", spawnExtras...)
				if err != nil {
					if strings.Contains(err.Error(), "busy") {
						_ = client.AbandonJob(runCtx, a.ID, "REPO_BUSY")
					}
					return err
				}

				pid := sup.PID(a.WorktreePath)
				wd.Track(watchdog.TrackedJob{
					JobID:          a.ID,
					ScheduleID:     a.ScheduleID,
					WorktreePath:   a.WorktreePath,
					DocsHubPath:    a.DocsHubPath,
					APIURL:         api,
					Engine:         a.Engine,
					Prompt:         a.Prompt,
					StoryID:        a.StoryID,
					ConversationID: convID,
					PID:            pid,
					HarnessPID:     pid,
					ToolPID:        toolPID,
				})
				return nil
			}

			return client.Loop(runCtx, host, kind, 3*time.Second, onJob, profile)
		}

		if ctx.Err() != nil {
			return nil
		}
		sup := supervisor.New(time.Now)
		_ = sup.Lock("run")
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("unknown command %s", args[0])
	}
}

func flushHarvestConversation(ctx context.Context, conversationID string) error {
	if strings.TrimSpace(conversationID) == "" {
		return nil
	}
	fCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	harvesterBin := supervisor.ResolveBinary("iazio-harvester")
	cmd := exec.CommandContext(fCtx, harvesterBin, "once", "--conversation", conversationID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[agent] Warning: harvest flush for conversation %s failed: %v (output: %s)\n",
			conversationID, err, strings.TrimSpace(string(out)))
		return err
	}
	return nil
}

func runAuth(ctx context.Context, args []string, stdout io.Writer, getenv func(string) string) error {
	if len(args) == 0 {
		return fmt.Errorf("auth status|login")
	}
	switch args[0] {
	case "status":
		st, err := auth.ReadStatus(auth.ConfigPathFrom("", getenv))
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, auth.FormatStatus(st))
		return nil
	case "login":
		if auth.Noninteractive(getenv) {
			return fmt.Errorf("auth login refused: noninteractive session")
		}
		return auth.Login(ctx, auth.LoginOptions{Getenv: getenv, Stdout: stdout})
	default:
		return fmt.Errorf("unknown auth command")
	}
}

func runService(args []string, stdout io.Writer, getenv func(string) string) error {
	action, dry, force, err := parseService(args)
	if err != nil {
		return err
	}
	goos := runtime.GOOS
	if v := getenv("GOOS_OVERRIDE"); v != "" {
		goos = v
	}
	bin := "iazio-agent"
	if home, err := os.UserHomeDir(); err == nil {
		canonical := filepath.Join(home, ".iazio", "bin", "iazio-agent")
		if runtime.GOOS == "windows" {
			canonical += ".exe"
		}
		if _, err := os.Stat(canonical); err == nil {
			bin = canonical
		}
	}
	if !dry && (action == "install" || action == "restart") {
		installed, err := installCanonicalBinary()
		if err != nil {
			return err
		}
		bin = installed
	}
	text := service.ActionText(goos, action, bin, force)
	if dry || action != "status" {
		fmt.Fprintln(stdout, text)
	}
	if dry {
		return nil
	}
	switch action {
	case "dev-install":
		src := ""
		for _, a := range args[1:] {
			if !strings.HasPrefix(a, "-") {
				src = a
				break
			}
		}
		home, _ := os.UserHomeDir()
		if err := service.DevInstall(src, home, execCommand); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "iazio-agent dev version installed and supervisor active")
		return nil
	case "pause":
		home, _ := os.UserHomeDir()
		if err := service.PauseService(home, execCommand); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "iazio-agent service paused (maintenance mode active)")
		return nil
	case "resume":
		home, _ := os.UserHomeDir()
		if err := service.ResumeService(home, execCommand); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "iazio-agent service resumed (maintenance mode cleared)")
		return nil
	case "revive-check":
		home, _ := os.UserHomeDir()
		return service.ReviveCheck(home, execCommand)
	}
	switch goos {
	case "darwin":
		return applyDarwin(action, bin)
	case "windows":
		if action == "install" {
			return service.InstallWindows(bin)
		}
		return applyWindows(action, bin, stdout)
	default:
		return applyLinux(action, bin, stdout, force)
	}
}

var isSystemdAvailable = service.IsSystemdAvailable

func applyLinux(action, bin string, stdout io.Writer, force bool) error {
	switch action {
	case "install", "start", "stop", "restart", "uninstall", "status", "dev-install", "pause", "resume", "revive-check":
	default:
		return fmt.Errorf("unknown service action %s", action)
	}
	if !isSystemdAvailable() || os.Getenv("IAZIO_DISABLE_SYSTEMD") == "1" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		switch action {
		case "install":
			return service.InstallUserDaemon(bin)
		case "start":
			return service.StartUserDaemonAt(home, bin, execCommand)
		case "stop":
			return service.StopUserDaemonAt(home)
		case "restart":
			_ = service.StopUserDaemonAt(home)
			return service.StartUserDaemonAt(home, bin, execCommand)
		case "uninstall":
			return service.UninstallUserDaemonAt(home)
		case "status":
			running, pid := service.StatusUserDaemonAt(home)
			if running {
				fmt.Fprintf(stdout, "running (pid: %d)\n", pid)
				return nil
			}
			return fmt.Errorf("installed but not running")
		default:
			return fmt.Errorf("unknown service action %s", action)
		}
	}
	if action == "install" {
		return service.InstallLinux(bin)
	}
	if action == "status" {
		cmd := exec.Command("systemctl", "--user", "status", "iazio-agent.service", "--no-pager")
		out, err := cmd.CombinedOutput()
		if stdout != nil {
			fmt.Fprint(stdout, string(out))
		}
		return err
	}
	_, err := service.Apply("linux", action, bin, false, force, execCommand)
	return err
}

func applyDarwin(action, bin string) error {
	switch action {
	case "install", "restart":
		_ = bootoutDarwin(false)
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		return service.InstallDarwinAt(home, bin, execCommand)
	case "start":
		return kickstartDarwin()
	case "stop", "uninstall":
		return bootoutDarwin(action == "uninstall")
	case "status":
		out, err := execCommandOutput("launchctl", "print", darwinTarget())
		if err != nil {
			return fmt.Errorf("service not loaded: %s", strings.TrimSpace(string(out)))
		}
		if strings.Contains(string(out), "pid =") && !strings.Contains(string(out), "job state = exited") {
			fmt.Println("running (io.iazio.iazio-agent)")
			wOut, wErr := execCommandOutput("launchctl", "print", darwinWatcherTarget())
			if wErr == nil && !strings.Contains(string(wOut), "job state = exited") {
				fmt.Println("active (io.iazio.iazio-agent-watcher)")
			}
			if wdOut, wdErr := execCommandOutput("launchctl", "print", darwinWatchdogTarget()); wdErr == nil && !strings.Contains(string(wdOut), "job state = exited") {
				fmt.Println("active (io.iazio.iazio-agent-watchdog)")
			}
			return nil
		}
		return fmt.Errorf("installed but not running")
	default:
		return fmt.Errorf("unknown service action %s", action)
	}
}

func darwinTarget() string {
	return "gui/" + strconv.Itoa(os.Getuid()) + "/io.iazio.iazio-agent"
}

func darwinWatcherTarget() string {
	return "gui/" + strconv.Itoa(os.Getuid()) + "/io.iazio.iazio-agent-watcher"
}

func darwinWatchdogTarget() string {
	return "gui/" + strconv.Itoa(os.Getuid()) + "/io.iazio.iazio-agent-watchdog"
}

func kickstartDarwin() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	plist := filepath.Join(home, "Library", "LaunchAgents", "io.iazio.iazio-agent.plist")
	if _, err := os.Stat(plist); err != nil {
		return fmt.Errorf("LaunchAgent is not installed")
	}
	_ = execCommand("launchctl", "bootstrap", "gui/"+strconv.Itoa(os.Getuid()), plist)
	if err := service.InstallDarwinResilience(home, filepath.Join(home, ".iazio", "bin", "iazio-agent"), execCommand); err != nil {
		return fmt.Errorf("install darwin resilience: %w", err)
	}
	return execCommand("launchctl", "kickstart", "-k", darwinTarget())
}

func bootoutDarwin(remove bool) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	if remove {
		_ = service.UninstallDarwinResilience(home, execCommand)
	} else {
		_ = execCommand("launchctl", "bootout", darwinWatcherTarget())
		_ = execCommand("launchctl", "bootout", darwinWatchdogTarget())
	}
	_ = execCommand("launchctl", "bootout", darwinTarget())
	if !remove {
		return nil
	}
	return os.Remove(filepath.Join(home, "Library", "LaunchAgents", "io.iazio.iazio-agent.plist"))
}

var parseSchtasksStatus = service.ParseSchtasksStatus

func applyWindows(action, _ string, stdout io.Writer) error {
	switch action {
	case "status":
		out, err := execCommandOutput("schtasks", "/Query", "/TN", "iazio-agent", "/FO", "LIST")
		if err != nil {
			return fmt.Errorf("service not installed: %s", strings.TrimSpace(string(out)))
		}
		running, statusVal, ok := parseSchtasksStatus(string(out))
		if !ok {
			return fmt.Errorf("service status unavailable: Status field missing from schtasks output")
		}
		if running {
			fmt.Fprintln(stdout, "running")
			return nil
		}
		return fmt.Errorf("installed but not running (%s)", statusVal)
	case "start":
		return execCommand("schtasks", "/Run", "/TN", "iazio-agent")
	case "stop":
		return execCommand("schtasks", "/End", "/TN", "iazio-agent")
	case "restart":
		_ = execCommand("schtasks", "/End", "/TN", "iazio-agent")
		return execCommand("schtasks", "/Run", "/TN", "iazio-agent")
	case "uninstall":
		return execCommand("schtasks", "/Delete", "/TN", "iazio-agent", "/F")
	default:
		return fmt.Errorf("unknown service action %s", action)
	}
}

func installCanonicalBinary() (string, error) {
	src, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve executable: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(src); err == nil {
		src = resolved
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home: %w", err)
	}
	binName := "iazio-agent"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	dest := filepath.Join(home, ".iazio", "bin", binName)
	if err := copyBinary(src, dest); err != nil {
		return "", err
	}
	_ = copyBinary(src, filepath.Join(home, ".local", "bin", binName))
	return dest, nil
}

func copyBinary(src, dest string) error {
	if src == dest {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dest + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dest)
}

var execCommand = func(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", name, strings.TrimSpace(string(out)))
	}
	return nil
}

var execCommandOutput = func(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

func parseService(args []string) (action string, dry, force bool, err error) {
	if len(args) == 0 {
		return "", false, false, fmt.Errorf("service install|uninstall|start|stop|restart|status")
	}
	action = args[0]
	switch action {
	case "install", "uninstall", "start", "stop", "restart", "status", "dev-install", "pause", "resume", "revive-check":
	default:
		return "", false, false, fmt.Errorf("unknown service action %s", action)
	}
	for _, arg := range args[1:] {
		switch {
		case arg == "--dry-run":
			dry = true
		case arg == "--force":
			force = true
		case strings.HasPrefix(arg, "-"):
			return "", false, false, fmt.Errorf("unknown service flag %s", arg)
		}
	}
	return action, dry, force, nil
}

var hostIDSanitizeRegex = regexp.MustCompile(`[^a-z0-9-]`)

func sanitizeHostID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	return hostIDSanitizeRegex.ReplaceAllString(s, "")
}

func resolveHostID(getenv func(string) string, hostnameFn func() (string, error), isWSLFn func() bool) string {
	if getenv != nil {
		if host := strings.TrimSpace(getenv("IAZIO_AGENT_HOST_ID")); host != "" {
			return sanitizeHostID(host)
		}
	}
	if hostnameFn == nil {
		hostnameFn = os.Hostname
	}
	if isWSLFn == nil {
		isWSLFn = func() bool {
			return service.IsWSLWith(getenv, func() ([]byte, error) {
				return os.ReadFile("/proc/sys/kernel/osrelease")
			})
		}
	}
	var host string
	if h, err := hostnameFn(); err == nil && strings.TrimSpace(h) != "" {
		host = sanitizeHostID(strings.Split(strings.TrimSpace(h), ".")[0])
	}
	if host == "" {
		host = "runner"
	}
	if isWSLFn() && !strings.HasSuffix(host, "-wsl") {
		host += "-wsl"
	}
	return host
}

type repoAuditSubmodule struct {
	Name           string `json:"name"`
	Path           string `json:"path"`
	IsClean        bool   `json:"is_clean"`
	ModifiedCount  int    `json:"modified_count"`
	UntrackedCount int    `json:"untracked_count"`
}

type repoAuditResult struct {
	TotalSubmodules int                  `json:"total_submodules"`
	DirtyCount      int                  `json:"dirty_count"`
	ErrorCount      int                  `json:"error_count"`
	Submodules      []repoAuditSubmodule `json:"submodules"`
}

func isDocOrEphemeralFile(f string) bool {
	clean := filepath.ToSlash(strings.TrimSpace(f))
	clean = strings.TrimPrefix(clean, "./")
	return clean == "docs" ||
		clean == "docs/" ||
		strings.HasPrefix(clean, "docs/") ||
		strings.HasPrefix(clean, "tmp/") ||
		strings.HasPrefix(clean, ".system_generated/") ||
		strings.HasPrefix(clean, ".gemini/") ||
		strings.HasPrefix(clean, ".agents/") ||
		strings.HasSuffix(clean, ".log") ||
		strings.HasSuffix(clean, ".md")
}

func isGitSubmoduleDir(path string) bool {
	gitPath := filepath.Join(path, ".git")
	_, err := os.Stat(gitPath)
	return err == nil
}

func scanRepoFleetStatus(ctx context.Context, repoPath string) bool {
	// First: verify repoPath is a valid git repository top-level root
	topCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "rev-parse", "--show-toplevel")
	topOut, err := topCmd.Output()
	if err != nil {
		return false
	}
	cleanTop := filepath.Clean(strings.TrimSpace(string(topOut)))
	cleanRepo := filepath.Clean(repoPath)
	if resolvedRepo, rerr := filepath.EvalSymlinks(cleanRepo); rerr == nil {
		cleanRepo = resolvedRepo
	}
	if resolvedTop, rerr := filepath.EvalSymlinks(cleanTop); rerr == nil {
		cleanTop = resolvedTop
	}
	if cleanTop != cleanRepo {
		return false
	}

	// Try iazio-harness fleet-status --repo <repoPath> --json first
	harnessBin := "iazio-harness"
	if home, err := os.UserHomeDir(); err == nil {
		p := filepath.Join(home, ".iazio", "bin", "iazio-harness")
		if _, err := os.Stat(p); err == nil {
			harnessBin = p
		}
	}
	harnessCmd := exec.CommandContext(ctx, harnessBin, "fleet-status", "--repo", repoPath, "--json")
	if out, err := harnessCmd.Output(); err == nil && len(out) > 0 {
		var res struct {
			IsClean         bool     `json:"is_clean"`
			WorktreeState   string   `json:"worktree_state"`
			SubmodulesClean bool     `json:"submodules_clean"`
			DirtySubmodules []string `json:"dirty_submodules"`
			ModifiedFiles   []string `json:"modified_files"`
			UntrackedFiles  []string `json:"untracked_files"`
		}
		if json.Unmarshal(out, &res) == nil {
			if res.WorktreeState == "CLEAN" || res.WorktreeState == "CLEAN_SUBMODULE_DRIFT" {
				return true
			}
			if res.WorktreeState == "DIRTY_UNTRACKED_ONLY" {
				onlyDocs := true
				for _, f := range res.UntrackedFiles {
					if !isDocOrEphemeralFile(f) {
						onlyDocs = false
						break
					}
				}
				if onlyDocs && res.SubmodulesClean {
					return true
				}
				return false
			}
			if res.WorktreeState != "" {
				return false
			}
			if res.IsClean && res.SubmodulesClean && len(res.ModifiedFiles) == 0 && len(res.DirtySubmodules) == 0 {
				onlyDocs := true
				for _, f := range res.UntrackedFiles {
					if !isDocOrEphemeralFile(f) {
						onlyDocs = false
						break
					}
				}
				if onlyDocs {
					return true
				}
				return false
			}
			if len(res.ModifiedFiles) > 0 || len(res.DirtySubmodules) > 0 {
				return false
			}
		}
	}

	// Fallback git porcelain check:
	// Allow untracked docs/logs and submodule commit pointer drift (CLEAN_SUBMODULE_DRIFT)
	gitCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain")
	out, err := gitCmd.CombinedOutput()
	if err != nil {
		return false
	}
	lines := strings.Split(string(out), "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l == "" {
			continue
		}
		if strings.HasPrefix(l, "?? ") {
			relFile := strings.TrimPrefix(l, "?? ")
			if !isDocOrEphemeralFile(relFile) {
				return false
			}
			continue
		}
		parts := strings.Fields(l)
		if len(parts) >= 2 {
			candidateSub := filepath.Join(repoPath, parts[len(parts)-1])
			if isGitSubmoduleDir(candidateSub) {
				subCmd := exec.CommandContext(ctx, "git", "-C", candidateSub, "status", "--porcelain")
				if subOut, sErr := subCmd.CombinedOutput(); sErr == nil && strings.TrimSpace(string(subOut)) == "" {
					// Clean submodule with commit pointer drift (STORY-GOV-3652 AC-2)
					continue
				}
			}
		}
		return false
	}

	return true
}

func areAllReposCleanAndHarnessIdle(ctx context.Context, client controlplane.Client, sup *supervisor.Sup, host, api string, force bool, stdout io.Writer) bool {
	if sup != nil && sup.HasRunningHarness() {
		if stdout != nil {
			fmt.Fprintln(stdout, "[SIR: IDLE_UPDATE_DEFERRED] reason=\"harness execution is active\"")
		}
		return false
	}
	if force {
		// When force is explicitly requested, bypass worktree checks as long as harness is not executing
		return true
	}
	if api != "" {
		detail, err := client.GetHost(ctx, host)
		if err != nil {
			if stdout != nil {
				fmt.Fprintf(stdout, "[SIR: IDLE_UPDATE_DEFERRED] reason=\"failed to fetch host details: %v\"\n", err)
			}
			return false
		}
		for _, r := range detail.Repos {
			path := r.WorktreePath
			if path == "" {
				path = r.Path
			}
			if path == "" {
				continue
			}
			if _, err := os.Stat(path); os.IsNotExist(err) {
				continue
			}
			// Verify repo queue is open (not paused or blocked)
			q := strings.ToUpper(strings.TrimSpace(r.Queue))
			if q != "" && q != "OPEN" && q != "ACTIVE" {
				if stdout != nil {
					fmt.Fprintf(stdout, "[SIR: IDLE_UPDATE_DEFERRED] worktree=\"%s\" queue=\"%s\" reason=\"repo queue is not open\"\n", path, r.Queue)
				}
				return false
			}
			// Verify repo lock is idle (not running or cooling)
			lock := strings.ToUpper(strings.TrimSpace(r.Lock))
			if lock != "" && lock != "IDLE" {
				if stdout != nil {
					fmt.Fprintf(stdout, "[SIR: IDLE_UPDATE_DEFERRED] worktree=\"%s\" lock=\"%s\" reason=\"repo lock is not idle\"\n", path, r.Lock)
				}
				return false
			}
			if r.RunningJobID != "" {
				if stdout != nil {
					fmt.Fprintf(stdout, "[SIR: IDLE_UPDATE_DEFERRED] worktree=\"%s\" running_job=\"%s\"\n", path, r.RunningJobID)
				}
				return false

			}
			if sup != nil {
				supLock := sup.Lock(path)
				if supLock == supervisor.LockRunning || supLock == supervisor.LockCooling || supLock == supervisor.LockPending {
					if stdout != nil {
						fmt.Fprintf(stdout, "[SIR: IDLE_UPDATE_DEFERRED] worktree=\"%s\" lock=\"%s\"\n", path, supLock)
					}
					return false
				}
			}
			if !scanRepoFleetStatus(ctx, path) {
				if stdout != nil {
					fmt.Fprintf(stdout, "[SIR: IDLE_UPDATE_DEFERRED] worktree=\"%s\" reason=\"worktree is dirty\"\n", path)
				}
				return false
			}
		}
	}
	return true
}

var syscallExec = syscall.Exec

func restartOrExecSelf(stdout io.Writer) {
	if stdout != nil {
		fmt.Fprintln(stdout, "[supervisor] iazio-agent self-updated; executing in-PID handoff to updated binary...")
	}
	if runtime.GOOS != "windows" {
		selfBin, err := os.Executable()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[SIR: AUTO_RESTART_FAIL] os.Executable() failed: %v; exiting non-zero for systemd recovery\n", err)
			os.Exit(1)
		}
		if resolved, err := filepath.EvalSymlinks(selfBin); err == nil {
			selfBin = resolved
		}
		execErr := syscallExec(selfBin, os.Args, os.Environ())
		if execErr != nil {
			fmt.Fprintf(os.Stderr, "[SIR: AUTO_RESTART_FAIL] syscall.Exec(%s) failed: %v; exiting non-zero for systemd recovery\n", selfBin, execErr)
			os.Exit(1)
		}
	}
}

func probeRepoRuntime(ctx context.Context, repoPath, alias string) controlplane.RepoRuntimeStatus {
	st := controlplane.RepoRuntimeStatus{
		RepoPath:      repoPath,
		Alias:         alias,
		IsClean:       true,
		LastCheckedAt: time.Now(),
	}

	// Try iazio-harness fleet-status --json first
	harnessBin := "iazio-harness"
	if home, err := os.UserHomeDir(); err == nil {
		p := filepath.Join(home, ".iazio", "bin", "iazio-harness")
		if _, err := os.Stat(p); err == nil {
			harnessBin = p
		}
	}
	harnessCmd := exec.CommandContext(ctx, harnessBin, "fleet-status", "--repo", repoPath, "--json")
	if out, err := harnessCmd.Output(); err == nil && len(out) > 0 {
		var res struct {
			Branch               string   `json:"branch"`
			IsClean              bool     `json:"is_clean"`
			Ahead                int      `json:"ahead"`
			Behind               int      `json:"behind"`
			ModifiedFiles        []string `json:"modified_files"`
			UntrackedFiles       []string `json:"untracked_files"`
			SubmodulesClean      bool     `json:"submodules_clean"`
			DirtySubmodules      []string `json:"dirty_submodules"`
			NonMainSubmodules    []string `json:"non_main_submodules"`
			ASEState             string   `json:"ase_state"`
			ActiveStoryID        string   `json:"active_story_id"`
			AgentTool            string   `json:"agent_tool"`
			ToolLiveness         string   `json:"tool_liveness"`
			ExecutionProvenance  string   `json:"execution_provenance"`
			ActiveConversationID string   `json:"active_conversation_id"`
		}
		if json.Unmarshal(out, &res) == nil {
			st.Branch = res.Branch
			st.IsClean = res.IsClean
			st.SubmodulesClean = res.SubmodulesClean
			st.DirtySubmodules = res.DirtySubmodules
			st.NonMainSubmodules = res.NonMainSubmodules
			st.ASEState = res.ASEState
			st.ActiveStoryID = res.ActiveStoryID
			st.ActiveTool = res.AgentTool
			st.ToolLiveness = res.ToolLiveness
			st.ExecutionProvenance = res.ExecutionProvenance
			st.ActiveConversationID = res.ActiveConversationID
			if len(res.ModifiedFiles) > 0 || len(res.UntrackedFiles) > 0 {
				var pLines []string
				for _, m := range res.ModifiedFiles {
					pLines = append(pLines, "M "+m)
				}
				for _, u := range res.UntrackedFiles {
					pLines = append(pLines, "?? "+u)
				}
				st.Porcelain = strings.Join(pLines, "\n")
			}
		}
	}

	if st.Branch == "" {
		// Fallback git status & branch
		cmd := exec.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain=v2", "--branch")
		out, err := cmd.CombinedOutput()
		if err == nil {
			lines := strings.Split(string(out), "\n")
			var porcelainLines []string
			for _, l := range lines {
				l = strings.TrimSpace(l)
				if l == "" {
					continue
				}
				if strings.HasPrefix(l, "# branch.head ") {
					st.Branch = strings.TrimPrefix(l, "# branch.head ")
				} else if !strings.HasPrefix(l, "# ") {
					porcelainLines = append(porcelainLines, l)
				}
			}
			st.Porcelain = strings.Join(porcelainLines, "\n")
			isMain := st.Branch == "main" || st.Branch == "master"
			st.IsClean = len(porcelainLines) == 0 && isMain
		} else {
			st.IsClean = false
			st.Porcelain = err.Error()
		}
	}

	// Submodules check
	subCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "submodule", "status")
	if subOut, err := subCmd.CombinedOutput(); err == nil {
		subLines := strings.Split(string(subOut), "\n")
		st.SubmodulesClean = true
		for _, l := range subLines {
			l = strings.TrimSpace(l)
			if l == "" {
				continue
			}
			if strings.HasPrefix(l, "+") || strings.HasPrefix(l, "U") || strings.HasPrefix(l, "-") {
				st.SubmodulesClean = false
				parts := strings.Fields(l)
				if len(parts) >= 2 {
					st.DirtySubmodules = append(st.DirtySubmodules, parts[1])
				}
			}
		}
		if !st.SubmodulesClean || len(st.DirtySubmodules) > 0 || len(st.NonMainSubmodules) > 0 {
			st.IsClean = false
		}
	}

	// ASE state
	statePath := filepath.Join(repoPath, ".autopilot", "state.json")
	if b, err := os.ReadFile(statePath); err == nil {
		var s struct {
			State   string `json:"state"`
			StoryID string `json:"story_id"`
		}
		if json.Unmarshal(b, &s) == nil {
			st.ASEState = s.State
			st.ActiveStoryID = s.StoryID
		}
	}
	if st.ASEState == "" {
		if _, err := os.Stat(filepath.Join(repoPath, ".ase", "active.lock")); err == nil {
			st.ASEState = "RUNNING"
		} else if _, err := os.Stat(filepath.Join(repoPath, ".autopilot", "lock")); err == nil {
			st.ASEState = "RUNNING"
		} else {
			st.ASEState = "IDLE"
		}
	}

	// metadata.yml snapshot sync
	metaPath := filepath.Join(repoPath, "metadata.yml")
	if fi, err := os.Stat(metaPath); err == nil && !fi.IsDir() && fi.Size() < 2<<20 {
		if metaBytes, err := os.ReadFile(metaPath); err == nil {
			st.MetadataYmlRaw = string(metaBytes)
			hash := sha256.Sum256(metaBytes)
			st.MetadataYmlSHA = fmt.Sprintf("%x", hash)
			st.MetadataYmlModTime = fi.ModTime().UTC().Format(time.RFC3339)
		}
	}

	// Git flow version autodetect (git describe --tags --always)
	gitVerCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "describe", "--tags", "--always")
	if out, err := gitVerCmd.Output(); err == nil {
		st.GitVersion = strings.TrimSpace(string(out))
	}

	return st
}

// healGitRepo deterministic cleanup for broken, diverged, or locked repo state.
func healGitRepo(ctx context.Context, repoPath string) {
	if strings.TrimSpace(repoPath) == "" {
		return
	}

	// 1. Remove stale git index lock if present
	lockFile := filepath.Join(repoPath, ".git", "index.lock")
	if fi, err := os.Stat(lockFile); err == nil && !fi.IsDir() {
		_ = os.Remove(lockFile)
	}

	// 2. Remove autopilot / ase locks
	_ = os.Remove(filepath.Join(repoPath, ".ase", "active.lock"))
	_ = os.Remove(filepath.Join(repoPath, ".autopilot", "lock"))

	// 3. Reset and clean working tree
	_ = exec.CommandContext(ctx, "git", "-C", repoPath, "reset", "--hard", "HEAD").Run()
	_ = exec.CommandContext(ctx, "git", "-C", repoPath, "clean", "-fd").Run()

	// 4. Synchronize submodules
	_ = exec.CommandContext(ctx, "git", "-C", repoPath, "submodule", "foreach", "--recursive", "git reset --hard HEAD").Run()
	_ = exec.CommandContext(ctx, "git", "-C", repoPath, "submodule", "foreach", "--recursive", "git clean -fd").Run()
	_ = exec.CommandContext(ctx, "git", "-C", repoPath, "submodule", "update", "--init", "--recursive").Run()
}

// resolveWorktreeFromHostDetail resolves a target worktree from the host's registered repos.
func resolveWorktreeFromHostDetail(detail controlplane.HostDetail, repoName string) string {
	clean := strings.TrimSpace(repoName)
	// 1. Direct match by repo alias or basename
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" {
			continue
		}
		if r.Alias == clean || r.RepoAlias == clean || filepath.Base(wt) == clean {
			return wt
		}
	}
	// 2. Submodule match: prefer primary worktree without auxiliary numeric suffix (e.g. -02, -03)
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" {
			continue
		}
		base := filepath.Base(wt)
		if strings.Contains(base, "-0") || strings.Contains(base, "-1") || strings.Contains(base, "-2") {
			continue
		}
		gmPath := filepath.Join(wt, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if strings.Contains(string(b), clean) {
				return wt
			}
		}
	}
	// 3. Any other worktree containing submodule in .gitmodules
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" {
			continue
		}
		gmPath := filepath.Join(wt, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if strings.Contains(string(b), clean) {
				return wt
			}
		}
	}
	if clean == "." || clean == "" || clean == "root" {
		for _, r := range detail.Repos {
			if r.GlobalQueueAllocationEnabled {
				wt := r.WorktreePath
				if wt == "" {
					wt = r.Path
				}
				if wt != "" {
					return wt
				}
			}
		}
		if len(detail.Repos) > 0 {
			wt := detail.Repos[0].WorktreePath
			if wt == "" {
				wt = detail.Repos[0].Path
			}
			if wt != "" {
				return wt
			}
		}
	}
	return ""
}

type hostDetailGetter interface {
	GetHost(ctx context.Context, hostID string) (controlplane.HostDetail, error)
}

func resolveWorktreeForRepo(ctx context.Context, getter hostDetailGetter, host, repoName string, getenv func(string) string) string {
	clean := strings.TrimSpace(repoName)
	if getter != nil {
		if detail, err := getter.GetHost(ctx, host); err == nil {
			if wt := resolveWorktreeFromHostDetail(detail, repoName); wt != "" {
				return wt
			}
		}
	}
	if getenv != nil {
		if envRoot := getenv("IAZIO_FLEET_ROOT"); envRoot != "" {
			return envRoot
		}
	}
	var candidateRoots []string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		workDir := filepath.Join(home, "work")
		if entries, err := os.ReadDir(workDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					candidateRoots = append(candidateRoots, filepath.Join(workDir, e.Name()))
				}
			}
		}
	}
	if entries, err := os.ReadDir("/work"); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				candidateRoots = append(candidateRoots, filepath.Join("/work", e.Name()))
			}
		}
	}
	for _, cand := range candidateRoots {
		gmPath := filepath.Join(cand, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if clean == "." || clean == "" || clean == "root" || strings.Contains(string(b), clean) {
				_ = os.Setenv("IAZIO_FLEET_ROOT", cand)
				return cand
			}
			if _, err := os.Stat(filepath.Join(cand, clean)); err == nil {
				_ = os.Setenv("IAZIO_FLEET_ROOT", cand)
				return cand
			}
		}
	}
	return ""
}

// ResolveDispatchConversationID resolves the conversation ID to resume with strict priority:
// ActiveConversationID -> ResumeConversationID -> InitialConversationID -> latest from ConversationIDs -> lockInfo.
// If ResumeMode is "fallback_new_conversation" and ActiveConversationID is empty, it returns ""
// so that a fresh, unanchored session starts without reusing obsolete initial IDs (Finding C3).
func ResolveDispatchConversationID(a controlplane.Assignment, lockConvID, lockStoryID, lockStatus string) string {
	if a.ResumeMode == "fallback_new_conversation" {
		if a.ActiveConversationID != "" && sessionstore.IsValidConversationID(a.ActiveConversationID) {
			return a.ActiveConversationID
		}
		// CP explicitly instructed a clean fallback conversation: do not reuse initial/historical ID
		return ""
	}
	convID := a.ActiveConversationID
	if convID == "" {
		convID = a.ResumeConversationID
	}
	if convID == "" {
		convID = a.InitialConversationID
	}
	if convID == "" && len(a.ConversationIDs) > 0 {
		convID = a.ConversationIDs[0]
	}
	isCPResume := (a.ParentJobID != "" || a.RootJobID != "")
	if convID == "" && !isCPResume && lockConvID != "" && lockStatus != "COMPLETED" && (a.Kind == "resume" || (a.StoryID != "" && lockStoryID == a.StoryID)) {
		convID = lockConvID
	}
	if convID != "" && !sessionstore.IsValidConversationID(convID) {
		log.Printf("[agent] ignoring invalid conversation id format: %q", convID)
		convID = ""
	}
	return convID
}

// buildEagerHooks constructs the CheckDrift and ListSuiteTools hooks for eager updater (R1, S2).
func buildEagerHooks(
	updateSuite bool,
	driftFetcher supervisor.DriftFetcher,
	isToolInBackoff func(string) bool,
	getInstallRoots func(*supervisor.FleetManifest) string,
	targetOS string,
) (checkDrift func(context.Context, []string) (bool, error), listSuiteTools func(context.Context) ([]string, error)) {
	checkDrift = func(ctx context.Context, targetTools []string) (bool, error) {
		manifest, err := driftFetcher.FetchManifest(ctx)
		if err != nil {
			return false, err
		}
		activeTools := make([]supervisor.ManifestTool, 0, len(manifest.Tools))
		for _, t := range manifest.Tools {
			if isToolInBackoff != nil && (isToolInBackoff(t.Name) || isToolInBackoff(t.BinaryName)) {
				continue
			}
			activeTools = append(activeTools, t)
		}
		activeManifest := *manifest
		activeManifest.Tools = activeTools

		var binDir string
		if getInstallRoots != nil {
			binDir = getInstallRoots(manifest)
		}
		checkTools := targetTools
		if len(checkTools) == 0 && !updateSuite {
			checkTools = []string{"iazio-agent"}
		}
		return supervisor.EvaluateDrift(&activeManifest, inventory.InspectInstalledVersion, binDir, checkTools, targetOS)
	}

	if updateSuite {
		listSuiteTools = func(ctx context.Context) ([]string, error) {
			manifest, err := driftFetcher.FetchManifest(ctx)
			if err != nil {
				return nil, err
			}
			var names []string
			for _, t := range manifest.Tools {
				names = append(names, t.Name)
			}
			return names, nil
		}
	} else {
		listSuiteTools = func(ctx context.Context) ([]string, error) {
			return []string{"iazio-agent"}, nil
		}
	}

	return checkDrift, listSuiteTools
}
