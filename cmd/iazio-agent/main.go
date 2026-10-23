// Command iazio-agent is the user-level runner supervisor.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/viovy/iazio-agent/internal/elasticlog"
	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/harness"
	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/preflight"
	"github.com/viovy/iazio-agent/internal/repolock"
	"github.com/viovy/iazio-agent/internal/service"
	"github.com/viovy/iazio-agent/internal/supervisor"
	"github.com/viovy/iazio-agent/internal/telemetry"
	"github.com/viovy/iazio-agent/internal/watchdog"
	"github.com/viovy/iazio-agent/internal/worker"
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

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
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

			sup := supervisor.New(time.Now)
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
						Name:    t.Name,
						Path:    t.Path,
						Version: t.Version,
						Status:  t.Status,
					})
				}
				return tools
			}

			var lastUpdateCheck time.Time
			var lastUpdateMu sync.Mutex

			doToolUpdate := func(ctx context.Context, targetTools []string, force bool) bool {
				tracer := telemetry.GlobalTracer()
				_, span := tracer.StartSpan(ctx, "agent.tools.update")
				defer span.End()
				span.SetTag("host_id", host)
				span.SetTag("force", fmt.Sprintf("%t", force))
				if len(targetTools) > 0 {
					span.SetTag("target_tools", strings.Join(targetTools, ","))
				}

				if !areAllReposCleanAndHarnessIdle(ctx, client, sup, host, api, force, stdout) {
					span.SetTag("result", "deferred")
					if sink != nil {
						sink.Info("Tool update check deferred: workstation is busy or worktree dirty", map[string]any{
							"host":   host,
							"action": "tool_update_deferred",
						})
					}
					return false
				}
				updatingTools.Store(true)
				if api != "" {
					_ = client.Heartbeat(ctx, host, getTools(), false)
				}
				var willRestart bool
				defer func() {
					if !willRestart {
						updatingTools.Store(false)
						if api != "" {
							_ = client.Heartbeat(ctx, host, getTools(), false)
						}
					}
				}()

				if sink != nil {
					sink.Info(fmt.Sprintf("Tool update check started (force=%t, target=%v)", force, targetTools), map[string]any{
						"host":   host,
						"action": "tool_update_started",
						"force":  force,
						"tools":  targetTools,
					})
				}

				opts := UpdateOptions{
					Suite:       updateSuite,
					Profile:     profile,
					API:         api,
					Force:       force,
					TargetTools: targetTools,
				}
				res, err := performUpdateWithResult(ctx, opts, stdout, stderr, getenv)
				if err != nil {
					span.SetTag("result", "error")
					span.SetError(err)
					if stderr != nil {
						fmt.Fprintf(stderr, "[supervisor] update check error: %v\n", err)
					}
					if sink != nil {
						sink.Error("Tool update check error", err, map[string]any{
							"host":   host,
							"action": "tool_update_error",
						})
					}
					return false
				}

				if len(res.UpdatedTools) > 0 {
					span.SetTag("result", "updated")
					span.SetTag("updated_tools", strings.Join(res.UpdatedTools, ","))
					if sink != nil {
						sink.Info(fmt.Sprintf("Successfully updated %d tools: %s", len(res.UpdatedTools), strings.Join(res.UpdatedTools, ", ")), map[string]any{
							"host":          host,
							"action":        "tool_update_succeeded",
							"updated_tools": res.UpdatedTools,
						})
					}
					if api != "" {
						_ = client.PostNotification(ctx, controlplane.NotificationRecord{
							Title:    "Tool Update Completed",
							Message:  fmt.Sprintf("Successfully updated %d tools on %s: %s", len(res.UpdatedTools), host, strings.Join(res.UpdatedTools, ", ")),
							Category: "success",
							Source:   "iazio-agent",
							HostID:   host,
							Metadata: map[string]any{
								"updated_tools": res.UpdatedTools,
								"force":         force,
							},
						})
					}
				} else {
					span.SetTag("result", "up-to-date")
					if sink != nil {
						sink.Info("Tool update check complete: all tools up-to-date", map[string]any{
							"host":   host,
							"action": "tool_update_checked",
						})
					}
				}

				if res.AgentUpdated {
					willRestart = true
				}
				return res.AgentUpdated
			}

			var pendingRemoteMu sync.Mutex
			var pendingRemoteTools []string
			var pendingRemoteForce bool
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
						isRemote := hasPendingRemote
						pendingRemoteMu.Unlock()

						if isRemote {
							if doToolUpdate(runCtx, t, f) {
								pendingRemoteMu.Lock()
								hasPendingRemote = false
								pendingRemoteMu.Unlock()
								return true
							}
							return false
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
				OnToolUpdate: func(ctx context.Context, tools []string, force bool) error {
					if stdout != nil {
						fmt.Fprintf(stdout, "[supervisor] remote tool update requested: tools=%v force=%v\n", tools, force)
					}
					pendingRemoteMu.Lock()
					pendingRemoteTools = tools
					pendingRemoteForce = force
					hasPendingRemote = true
					pendingRemoteMu.Unlock()

					go func() {
						pendingRemoteMu.Lock()
						t := pendingRemoteTools
						f := pendingRemoteForce
						pendingRemoteMu.Unlock()

						if doToolUpdate(runCtx, t, f) {
							pendingRemoteMu.Lock()
							hasPendingRemote = false
							pendingRemoteMu.Unlock()

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
							target := strings.Trim(strings.TrimSpace(string(cmd.Payload)), "\"")
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
					return nil
				},
			}

			if api != "" {
				cpClient := worker.NewHTTPControlPlaneClient(api, "", nil)
				cliInvoker := harness.NewCLIInvoker("", "")
				disp := worker.NewDispatcher(cpClient, cliInvoker, host)
				disp.SetToolUpdater(func(ctx context.Context, tools []string, force bool) error {
					if stdout != nil {
						fmt.Fprintf(stdout, "[dispatcher] tool update job executing: tools=%v force=%v\n", tools, force)
					}
					if doToolUpdate(ctx, tools, force) {
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
										rem := hasPendingRemote
										pendingRemoteMu.Unlock()

										if rem {
											if doToolUpdate(runCtx, t, f) {
												pendingRemoteMu.Lock()
												hasPendingRemote = false
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
				newPID, err := wdRunner.Start(argv)
				return newPID, err
			}
			wd = watchdog.New(watchdog.Options{
				Runner:  wdRunner,
				Spawner: spawner,
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

				err := sup.Spawn(wdRunner, a.ID, api, a.WorktreePath, a.DocsHubPath, "")
				if err != nil {
					if strings.Contains(err.Error(), "busy") {
						_ = client.AbandonJob(runCtx, a.ID, "REPO_BUSY")
					}
					return err
				}

				pid := sup.PID(a.WorktreePath)
				convID := a.ResumeConversationID
				if convID == "" && len(a.ConversationIDs) > 0 {
					convID = a.ConversationIDs[len(a.ConversationIDs)-1]
				}
				if convID == "" && lockInfo.ConversationID != "" && lockInfo.Status != "COMPLETED" && (a.Kind == "resume" || (a.StoryID != "" && lockInfo.StoryID == a.StoryID)) {
					convID = lockInfo.ConversationID
				}
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
			return st
		}
	}

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
	return st
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
