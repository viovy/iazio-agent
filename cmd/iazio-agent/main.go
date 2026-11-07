// Command iazio-agent is the user-level runner supervisor.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	iaziouser "github.com/viovy/iazio-core/pkg/userservice"
	coreauth "github.com/viovy/viovy-core-go/pkg/auth"
	"github.com/viovy/viovy-core-go/pkg/userservice"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
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
	"github.com/viovy/iazio-agent/internal/procutil"
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
	preflight.HydrateToolchainEnv()
	flushTracer := telemetry.InitGlobalTracer("iazio-agent", "")
	setGlobalTracerFlusher(flushTracer)
	defer flushTracer()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

var (
	shutdownHooksMu     sync.Mutex
	globalTracerFlusher func()
	activeElasticSink   *elasticlog.Sink
)

func setGlobalTracerFlusher(fn func()) {
	shutdownHooksMu.Lock()
	globalTracerFlusher = fn
	shutdownHooksMu.Unlock()
}

func setActiveElasticSink(s *elasticlog.Sink) {
	shutdownHooksMu.Lock()
	activeElasticSink = s
	shutdownHooksMu.Unlock()
}

func flushTelemetryAndCloseSink() {
	shutdownHooksMu.Lock()
	flusher := globalTracerFlusher
	globalTracerFlusher = nil
	s := activeElasticSink
	activeElasticSink = nil
	shutdownHooksMu.Unlock()

	if flusher != nil {
		flusher()
	} else if tr := telemetry.GlobalTracer(); tr != nil && tr.Exporter != nil {
		_ = tr.Exporter.Flush(context.Background())
		_ = tr.Exporter.Close()
	}
	if s != nil {
		_ = s.Close()
	}
}

var initialUpdateDelay = 5 * time.Second
var isTesting = false

func isTestBinary() bool {
	if isTesting {
		return true
	}
	if len(os.Args) > 0 {
		base := strings.ToLower(filepath.Base(os.Args[0]))
		if strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".test.exe") || strings.HasPrefix(base, "___test") {
			return true
		}
		for _, a := range os.Args[1:] {
			if strings.HasPrefix(a, "-test.") {
				return true
			}
		}
	}
	return false
}

type safeWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *safeWriter) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

var createWindowsMutexFn = createWindowsNamedMutex
var currentGOOS = runtime.GOOS

func acquireWindowsSingleInstanceMutex(stderr io.Writer) (func(), bool) {
	user := os.Getenv("USERNAME")
	if user == "" {
		user = "user"
	}
	mutexName := "Local\\iazio-agent-supervisor-" + user
	if isTestBinary() {
		mutexName += "-test"
	}
	ret, alreadyExists, closeFn := createWindowsMutexFn(mutexName)
	if alreadyExists {
		if closeFn != nil {
			closeFn()
		}
		fmt.Fprintf(stderr, "another iazio-agent instance is already running on this host/user (mutex exists); exiting\n")
		return nil, false
	}
	if ret == 0 {
		return nil, false
	}
	return closeFn, true
}

var (
	activeAgentLockMu sync.Mutex
	activeAgentUnlock func()
)

func releaseAgentSingleInstanceLock() {
	activeAgentLockMu.Lock()
	defer activeAgentLockMu.Unlock()
	if activeAgentUnlock != nil {
		activeAgentUnlock()
		activeAgentUnlock = nil
	}
}

func acquireAgentSingleInstanceLock(home string, stdout, stderr io.Writer) (func(), bool) {
	if running, pid := service.StatusUserDaemonAt(home); running && pid > 0 && pid != os.Getpid() {
		handoff := os.Getenv("IAZIO_HANDOFF_PID")
		if handoff != "" && strconv.Itoa(pid) == handoff {
			// Upgraded child process waiting for parent process to exit
			deadline := time.Now().Add(15 * time.Second)
			for time.Now().Before(deadline) {
				if r, _ := service.StatusUserDaemonAt(home); !r {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		if running, pid := service.StatusUserDaemonAt(home); running && pid > 0 && pid != os.Getpid() {
			if os.Getenv("IAZIO_AGENT_NONINTERACTIVE") != "1" && stderr != nil {
				fmt.Fprintf(stderr, "another iazio-agent instance (PID %d) is already running on this host/user; exiting\n", pid)
			}
			return nil, false
		}
	}

	var winUnlock func()
	if currentGOOS == "windows" {
		var ok bool
		winUnlock, ok = acquireWindowsSingleInstanceMutex(stderr)
		if !ok {
			return nil, false
		}
	}

	pidFile := filepath.Join(home, ".iazio", "agent.pid")
	_ = os.MkdirAll(filepath.Dir(pidFile), 0755)
	_ = os.WriteFile(pidFile, []byte(strconv.Itoa(os.Getpid())+"\n"), 0644)

	unlock := func() {
		_ = os.Remove(pidFile)
		if winUnlock != nil {
			winUnlock()
		}
	}

	activeAgentLockMu.Lock()
	activeAgentUnlock = unlock
	activeAgentLockMu.Unlock()

	return func() {
		releaseAgentSingleInstanceLock()
	}, true
}

func shouldEnsureCanonicalSingleHome(args []string) bool {
	if strings.HasPrefix(strings.TrimSpace(os.Getenv("XPC_SERVICE_NAME")), "io.iazio.") {
		return true
	}
	if len(args) == 0 {
		return false
	}
	sub := strings.ToLower(strings.TrimSpace(args[0]))
	switch sub {
	case "run", "update":
		return true
	default:
		return false
	}
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	preflight.HydrateToolchainEnv()
	if getenv == nil {
		getenv = os.Getenv
	}

	var cleanArgs []string
	var logFilePath string
	var nonInteractive bool
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--log-file" && i+1 < len(args) {
			logFilePath = args[i+1]
			i++
		} else if strings.HasPrefix(a, "--log-file=") {
			logFilePath = strings.TrimPrefix(a, "--log-file=")
		} else if a == "--non-interactive" || a == "-non-interactive" {
			nonInteractive = true
		} else {
			cleanArgs = append(cleanArgs, a)
		}
	}
	args = cleanArgs

	isDaemonRun := len(args) > 0 && strings.ToLower(strings.TrimSpace(args[0])) == "run"
	if logFilePath == "" {
		if envFile := strings.TrimSpace(getenv("IAZIO_LOG_FILE")); envFile != "" {
			logFilePath = envFile
		} else if envDir := strings.TrimSpace(getenv("IAZIO_LOG_DIR")); envDir != "" {
			logFilePath = filepath.Join(envDir, "agent.log")
		}
	}
	if !isTestBinary() && runtime.GOOS == "windows" && (nonInteractive || isDaemonRun || logFilePath != "" || getenv("IAZIO_AGENT_NONINTERACTIVE") == "1") {
		detachWindowsConsole()
	}
	if nonInteractive || isDaemonRun {
		_ = os.Setenv("IAZIO_AGENT_NONINTERACTIVE", "1")
	}
	if !isDaemonRun && shouldEnsureCanonicalSingleHome(args) {
		ensureCanonicalSingleHome()
	}
	if logFilePath != "" {
		_ = os.MkdirAll(filepath.Dir(logFilePath), 0755)
		if f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			defer f.Close()
			sw := &safeWriter{w: f}
			stdout = sw
			stderr = sw
			log.SetOutput(sw)
		}
	}

	if stdout != nil {
		stdout = &safeWriter{w: stdout}
	}
	if stderr != nil {
		stderr = &safeWriter{w: stderr}
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
		for _, a := range args[1:] {
			if a == "--json" || a == "-json" {
				fmt.Fprintln(stdout, formatVersionJSON())
				return nil
			}
		}
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
	case "cleanup-binaries", "clean-dev-binaries", "cleanup":
		return runCleanupBinaries(ctx, args[1:], stdout, stderr, getenv)
	case "update":
		return runUpdate(ctx, args[1:], stdout, stderr, getenv)
	case "run":
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 8192)
				n := runtime.Stack(buf, false)
				msg := fmt.Sprintf("[agent] PANIC RECOVERED: %v\n%s", r, string(buf[:n]))
				log.Print(msg)
				if stderr != nil {
					fmt.Fprintln(stderr, msg)
				}
			}
		}()
		home, _ := os.UserHomeDir()
		if home != "" && !isTestBinary() {
			unlock, ok := acquireAgentSingleInstanceLock(home, stdout, stderr)
			if !ok {
				return nil
			}
			defer unlock()
		}
		if shouldEnsureCanonicalSingleHome(args) {
			ensureCanonicalSingleHome()
		}
		defer func() {
			if !isTestBinary() && ctx.Err() == nil {
				if (runtime.GOOS == "linux" && service.IsSystemdAvailable()) || runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
					home, _ := os.UserHomeDir()
					if home == "" || !iaziouser.IsMaintenancePaused(home) {
						_ = service.ReviveCheck(home, execCommand)
					}
				}
			}
		}()
		kind := supervisor.HostKind(getenv("IAZIO_AGENT_HOST_KIND"))
		fmt.Fprintln(stdout, kind)
		api := resolveLocalAPI(getenv)
		profile := getenv("IAZIO_AGENT_PROFILE")
		updateInterval := 15 * time.Minute
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
		gitUpdateInterval := resolveGitUpdateInterval(args, getenv, stderr)

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
			case args[i] == "--git-update-interval" && i+1 < len(args):
				i++
			case strings.HasPrefix(args[i], "--git-update-interval="):
				// parsed via resolveGitUpdateInterval
			case args[i] == "--update-suite":
				updateSuite = true
			case args[i] == "--log-file" && i+1 < len(args):
				i++
			case strings.HasPrefix(args[i], "--log-file="):
				// ok
			case args[i] == "--non-interactive" || args[i] == "-non-interactive":
				// ok
			}
		}
		if profile == "" {
			profile = "generic"
		}
		if !isTestBinary() || isLoopbackTelemetryURL(api) {
			telemetry.ConfigureZipkinFromAPI(api)
		}
		host := resolveHostID(getenv, os.Hostname, nil)
		destURL, isGateway := resolveTelemetryDestination(api, getenv, ctx)
		if isTestBinary() && !isLoopbackTelemetryURL(destURL) {
			destURL = ""
		}
		authToken := resolveControlPlaneToken(getenv)
		var sink *elasticlog.Sink
		if destURL != "" {
			sink = elasticlog.New(elasticlog.Config{
				URL:         destURL,
				IsGateway:   isGateway,
				AuthToken:   authToken,
				TokenFunc:   func() string { return resolveControlPlaneToken(getenv) },
				ServiceName: "iazio-agent",
				Version:     formatVersion(),
				HostName:    host,
			})
			setActiveElasticSink(sink)
			defer func() {
				setActiveElasticSink(nil)
				_ = sink.Close()
			}()
			sink.Info("iazio-agent supervisor starting", map[string]any{
				"host_kind":  string(kind),
				"api_url":    api,
				"profile":    profile,
				"is_gateway": isGateway,
			})
		}
		if ctx.Err() == nil {
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			watchdog.StartSystemdWatchdog(runCtx)
			if !isTestBinary() {
				if home, err := os.UserHomeDir(); err == nil && home != "" {
					binPath := filepath.Join(home, ".local", "bin", "iazio-agent")
					if runtime.GOOS == "windows" {
						binPath = filepath.Join(home, "bin", "iazio-agent.exe")
					}
					go func() {
						_ = service.ReconcileServiceConfiguration(home, binPath, nil)
						_ = iaziouser.EnforceEdgeNonServicePolicy(home, runtime.GOOS, nil)
						_ = iaziouser.HealCompanionServices(home, runtime.GOOS, nil, watchdog.IsProcessAlive)
					}()
				}
				if runtime.GOOS == "windows" {
					wslSup := service.NewWSLCompanionSupervisor(home)
					wslSup.StartSupervisor(runCtx, 2*time.Minute)
				}
			}

			sup := supervisor.New(time.Now)
			liveMon := watchdog.NewConversationLivenessMonitor(5 * time.Minute)
			sup.SetProgressChecker(func(worktreePath string, pid int) bool {
				if pid <= 0 || !watchdog.IsProcessAlive(pid) {
					return false
				}
				snap := liveMon.CheckLiveness(worktreePath, "", pid, time.Time{})
				return !snap.IsStalled
			})
			hostGate := supervisor.NewHostExecutionAndUpdateGate()
			var disp *worker.Dispatcher
			var onJobGateMu sync.Mutex
			onJobGateLeases := make(map[string]func())
			releaseOnJobGateLease := func(jobID string) {
				if jobID == "" {
					return
				}
				onJobGateMu.Lock()
				rel := onJobGateLeases[jobID]
				delete(onJobGateLeases, jobID)
				onJobGateMu.Unlock()
				if rel != nil {
					rel()
				}
			}
			var client controlplane.Client

			var updatingTools atomic.Bool
			authStatusInterval := inventory.DefaultAuthStatusInterval
			if rawAuthInt := strings.TrimSpace(getenv("IAZIO_AGENT_AUTH_STATUS_INTERVAL")); rawAuthInt != "" {
				if parsedInt, err := time.ParseDuration(rawAuthInt); err == nil && parsedInt >= 5*time.Second {
					authStatusInterval = parsedInt
				}
			}
			authProber := inventory.NewAuthStatusProber(authStatusInterval, func(fetchCtx context.Context) (map[string]bool, error) {
				return fetchManifestAuthEnabledTools(fetchCtx, api, profile, resolveControlPlaneToken(getenv))
			})
			getTools := func() []controlplane.Tool {
				home, _ := os.UserHomeDir()
				dirs := inventory.DefaultDirs(getenv("PATH"), home, runtime.GOOS)
				found := inventory.Scan(dirs)
				classified, _ := inventory.Classify(found, nil)
				classified = authProber.EnrichTools(runCtx, classified)
				var tools []controlplane.Tool
				for _, t := range classified {
					tools = append(tools, controlplane.Tool{
						Name:              t.Name,
						Path:              t.Path,
						Version:           t.Version,
						CommitSHA:         t.CommitSHA,
						Status:            t.Status,
						ServiceInstalled:  t.ServiceInstalled,
						ServiceType:       t.ServiceType,
						ServiceStatus:     t.ServiceStatus,
						Capabilities:      t.Capabilities,
						Installed:         t.Installed,
						InstallationType:  t.InstallationType,
						ActiveState:       t.ActiveState,
						WatchdogEnabled:   t.WatchdogEnabled,
						ConfigStatus:      t.ConfigStatus,
						AuthStatusEnabled: t.AuthStatusEnabled,
						AuthStatus:        t.AuthStatus,
						AuthStatusError:   t.AuthStatusError,
						AuthCheckedAt:     t.AuthCheckedAt,
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
					enriched := make([]controlplane.RepoRuntimeStatus, 0, len(st))
					for _, item := range st {
						probed := probeRepoRuntime(c, item.RepoPath, item.Alias)
						if probed.Branch == "" {
							probed.Branch = item.Branch
						}
						if probed.HeadSHA == "" {
							probed.HeadSHA = item.HeadSHA
						}
						if probed.DirtyFilesCount == 0 && !probed.IsClean {
							probed.DirtyFilesCount = item.DirtyFilesCount
						}
						if probed.UntrackedFilesCount == 0 && !probed.IsClean {
							probed.UntrackedFilesCount = item.UntrackedFilesCount
						}
						if probed.Ahead == 0 {
							probed.Ahead = item.Ahead
						}
						if probed.Behind == 0 {
							probed.Behind = item.Behind
						}
						if item.HasActiveGitLock {
							probed.HasActiveGitLock = true
						}
						if probed.Branch == "" {
							if !item.IsClean {
								probed.IsClean = false
							}
							if !item.SubmodulesClean {
								probed.SubmodulesClean = false
							}
							if len(probed.DirtySubmodules) == 0 && len(item.DirtySubmodules) > 0 {
								probed.DirtySubmodules = item.DirtySubmodules
							}
						}
						enriched = append(enriched, probed)
					}
					return client.PostRepoStatus(c, h, enriched)
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
					}
				},
			)
			shellGitUpdateFn := supervisor.DefaultShellGitUpdate
			agyEscalationFn := supervisor.DefaultAgyEscalation
			if isTestBinary() {
				shellGitUpdateFn = func(context.Context, string) (string, error) { return "", nil }
				agyEscalationFn = func(context.Context, string, string, string) (string, error) { return "", nil }
			}
			repoMonitor.ConfigureGitUpdate(
				gitUpdateInterval,
				shellGitUpdateFn,
				agyEscalationFn,
				func(c context.Context, h, repoPath string) error {
					if api == "" {
						return nil
					}
					return client.ResumeRepo(c, h, repoPath)
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
					Suite:         updateSuite && len(targetTools) == 0,
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
			if isTestBinary() {
				execDetector.SetProcessScanner(nil)
			}
			eagerUpdater.SetExecutionDetector(execDetector)
			eagerUpdater.SetReadinessReporter(func(c context.Context, h string, r supervisor.EagerUpdateReadinessReport) error {
				if api == "" {
					return nil
				}
				return client.PostEagerReadiness(c, h, r)
			})

			if api != "" {
				driftFetcher := &supervisor.HTTPDriftFetcher{
					Client:        &http.Client{Timeout: 10 * time.Second},
					API:           api,
					Profile:       profile,
					OS:            runtime.GOOS,
					Arch:          runtime.GOARCH,
					AuthToken:     authToken,
					TokenProvider: func(context.Context) string { return resolveControlPlaneToken(getenv) },
				}
				isToolInBackoff := func(tool string) bool {
					return eagerUpdater != nil && (eagerUpdater.IsToolInBackoff(tool))
				}
				getInstallRoots := func(manifest *supervisor.FleetManifest) string {
					return resolveInstallDir(ManifestInstallRoots{
						Unix:    ManifestInstallRootPaths{Primary: manifest.InstallRoots.Unix.Primary},
						Windows: ManifestInstallRootPaths{Primary: manifest.InstallRoots.Windows.Primary},
					}, getenv)
				}
				eagerUpdater.CheckDrift, eagerUpdater.ListSuiteTools = buildEagerHooks(
					updateSuite,
					driftFetcher,
					isToolInBackoff,
					getInstallRoots,
					runtime.GOOS,
				)
				eagerUpdater.InspectOutdatedTools = buildInspectOutdatedHook(
					updateSuite,
					driftFetcher,
					getInstallRoots,
					runtime.GOOS,
				)
			}

			// Start hybrid trigger loop and repoMonitor AFTER eagerUpdater is fully constructed to eliminate initialization race
			eagerUpdater.StartHybridTriggerLoop(runCtx, func() {
				restartOrExecSelf(stdout)
				cancel()
			})
			eagerUpdater.TriggerEvaluation("agent_startup")
			repoMonitor.Start(runCtx)

			var lastUpdateCheck time.Time = time.Now().UTC()
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

				if !opts.BypassDirty && !areAllReposCleanAndHarnessIdle(ctx, client, sup, host, api, opts.Force, stdout, hostGate, disp) {
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
			var pendingRemoteSeq uint64

			snapshotPendingRemote := func() ([]string, bool, string, bool, uint64) {
				pendingRemoteMu.Lock()
				defer pendingRemoteMu.Unlock()
				return append([]string(nil), pendingRemoteTools...), pendingRemoteForce, pendingRemoteTargetVersion, hasPendingRemote, pendingRemoteSeq
			}

			clearPendingRemoteIfSeq := func(expectedSeq uint64) {
				pendingRemoteMu.Lock()
				if pendingRemoteSeq == expectedSeq {
					hasPendingRemote = false
					pendingRemoteTools = nil
					pendingRemoteForce = false
					pendingRemoteTargetVersion = ""
				}
				pendingRemoteMu.Unlock()
			}

			if updateInterval > 0 {
				go func() {
					select {
					case <-runCtx.Done():
						return
					case <-time.After(initialUpdateDelay):
					}

					checkOnce := func() bool {
						lastUpdateMu.Lock()
						lastUpdateCheck = time.Now().UTC()
						lastUpdateMu.Unlock()
						t, f, v, isRemote, seq := snapshotPendingRemote()

						if isRemote {
							res, deferred, err := doToolUpdateDetailed(runCtx, t, f, v)
							if !deferred {
								clearPendingRemoteIfSeq(seq)
							}
							if err != nil || deferred {
								return false
							}
							return res.AgentUpdated
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
			var inFlightPrepMu sync.Mutex
			inFlightPrepJobs := make(map[string]controlplane.ActiveJob)
			var lastHarvesterCheckMu sync.Mutex
			var lastHarvesterCheck time.Time
			spoolDir := ""
			if isTestBinary() {
				if h := strings.TrimSpace(getenv("HOME")); h != "" {
					spoolDir = filepath.Join(h, ".iazio", "spool")
				} else {
					spoolDir = filepath.Join(os.TempDir(), fmt.Sprintf("iazio-test-spool-%d", os.Getpid()))
				}
			}
			spooler, _ := controlplane.NewOfflineReportSpooler(spoolDir, nil)
			tokenProvider := func(context.Context) string {
				return resolveControlPlaneToken(getenv)
			}
			if spooler != nil {
				spooler.TokenProvider = tokenProvider
			}
			client = controlplane.Client{
				BaseURL:            api,
				Token:              authToken,
				TokenProvider:      tokenProvider,
				Spooler:            spooler,
				EagerUpdateEnabled: updateInterval > 0 || eagerUpdater != nil,
				PrePollBarrier:     makePrePollBarrier(hostGate, eagerUpdater),
				LastEagerUpdateAt: func() *time.Time {
					lastUpdateMu.Lock()
					defer lastUpdateMu.Unlock()
					if lastUpdateCheck.IsZero() {
						return nil
					}
					t := lastUpdateCheck
					return &t
				},
				Presence: func() string {
					if updatingTools.Load() {
						return "ONLINE_UPDATING_TOOLS"
					}
					return "ONLINE"
				},
				Tools: getTools,
				DiscoveredDevBinaries: func() []string {
					home, _ := os.UserHomeDir()
					searchDirs := inventory.DefaultDirs(getenv("PATH"), home, runtime.GOOS)
					canonicalBinDir := filepath.Join(home, ".local", "bin")
					if runtime.GOOS == "windows" {
						canonicalBinDir = filepath.Join(home, "bin")
					}
					candidates := inventory.ScanDevAndShadowBinaries(searchDirs, canonicalBinDir)
					var paths []string
					for _, c := range candidates {
						paths = append(paths, c.Path)
					}
					return paths
				},
				SupportedCapabilities: []string{
					"user_service_watchdog_v2",
					"deterministic_resume_4tuple_v1",
					"agy_conversation_pinning_v1",
					"workspace_recovery_modes_v1",
					"background_task_watchdog_v1",
				},
				Version:   formatVersion(),
				CommitSHA: resolveCommit(),
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
						if !force && len(tools) == 0 && tv == "" {
							return nil
						}
					}
					pendingRemoteMu.Lock()
					pendingRemoteSeq++
					seq := pendingRemoteSeq
					pendingRemoteTools = append([]string(nil), tools...)
					pendingRemoteForce = force
					pendingRemoteTargetVersion = tv
					hasPendingRemote = true
					tSnap := append([]string(nil), pendingRemoteTools...)
					fSnap := pendingRemoteForce
					vSnap := pendingRemoteTargetVersion
					pendingRemoteMu.Unlock()

					go func(expectedSeq uint64, t []string, f bool, v string) {
						res, deferred, err := doToolUpdateDetailed(runCtx, t, f, v)
						if !deferred {
							clearPendingRemoteIfSeq(expectedSeq)
						}
						if err == nil && !deferred && res.AgentUpdated {
							restartOrExecSelf(stdout)
							cancel()
						}
					}(seq, tSnap, fSnap, vSnap)
					return nil
				},
				OnCommand: func(ctx context.Context, cmd controlplane.CommandEnvelope) error {
					switch cmd.Type {
					case "TRIGGER_IDLE_UPDATE", "EVALUATE_EAGER_UPDATE":
						if eagerUpdater != nil {
							eagerUpdater.TriggerEvaluation("command")
						} else {
							go func() {
								if doToolUpdate(runCtx, nil, false) {
									restartOrExecSelf(stdout)
									cancel()
								}
							}()
						}
					case "FORCE_TOOL_UPDATE":
						go func() {
							var p struct {
								Tools         []string `json:"tools"`
								Force         *bool    `json:"force"`
								TargetVersion string   `json:"target_version"`
							}
							force := true
							if len(cmd.Payload) > 0 && json.Unmarshal(cmd.Payload, &p) == nil {
								if p.Force != nil {
									force = *p.Force
								}
							}
							if doToolUpdate(runCtx, p.Tools, force, p.TargetVersion) {
								restartOrExecSelf(stdout)
								cancel()
							}
						}()
					case "CLEANUP_BINARIES", "CLEANUP_DEV_BINARIES":
						go func() {
							_ = runCleanupBinaries(runCtx, nil, stdout, stderr, nil)
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
								resolvedTarget, ok := resolveRegisteredRepoWorktree(runCtx, getRepos, target)
								if !ok {
									if stderr != nil {
										fmt.Fprintf(stderr, "[agent] refusing %s on unregistered repository path %q\n", cmd.Type, target)
									}
									return
								}
								// First cancel and abort any active harness executions on target repo
								for _, rj := range sup.RunningJobs() {
									if rj.WorktreePath == resolvedTarget || filepath.Base(rj.WorktreePath) == filepath.Base(resolvedTarget) || rj.WorktreePath == target || filepath.Base(rj.WorktreePath) == target || rj.JobID == target {
										sup.Cancel(rj.WorktreePath, "FORCE_CLEAN_GIT")
										releaseOnJobGateLease(rj.JobID)
										if wd != nil {
											wd.AbortJob(rj.JobID, "FORCE_CLEAN_GIT")
										}
									}
								}
								healGitRepo(runCtx, resolvedTarget)
								st := probeRepoRuntime(runCtx, resolvedTarget, filepath.Base(resolvedTarget))
								_ = client.PostRepoStatus(runCtx, host, []controlplane.RepoRuntimeStatus{st})
							}
						}()
					case "KILL_BACKGROUND_TASKS":
						go func() {
							var target string
							if err := json.Unmarshal(cmd.Payload, &target); err != nil {
								target = strings.Trim(strings.TrimSpace(string(cmd.Payload)), "\"")
							}
							if stderr != nil {
								fmt.Fprintf(stderr, "[supervisor] remote kill background tasks received: target=%s\n", target)
							}
							for _, rj := range sup.RunningJobs() {
								if target == "" || rj.JobID == target || rj.WorktreePath == target || filepath.Base(rj.WorktreePath) == target {
									sup.Cancel(rj.WorktreePath, "KILL_BACKGROUND_TASKS")
									releaseOnJobGateLease(rj.JobID)
									if wd != nil {
										wd.AbortJob(rj.JobID, "KILL_BACKGROUND_TASKS")
									}
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
					if disp != nil {
						for _, dj := range disp.ActiveJobs() {
							if dj.JobID != "" && !seen[dj.JobID] {
								seen[dj.JobID] = true
								jobs = append(jobs, dj)
							}
						}
					}
					inFlightPrepMu.Lock()
					for _, pj := range inFlightPrepJobs {
						if pj.JobID != "" && !seen[pj.JobID] {
							seen[pj.JobID] = true
							jobs = append(jobs, pj)
						}
					}
					inFlightPrepMu.Unlock()
					if !isTestBinary() && home != "" {
						harnessActivePath := filepath.Join(home, ".iazio", "harness-active-jobs.json")
						if rawH, errH := os.ReadFile(harnessActivePath); errH == nil {
							var hJobs []watchdog.TrackedJob
							if json.Unmarshal(rawH, &hJobs) == nil {
								for _, hj := range hJobs {
									if hj.JobID == "" || seen[hj.JobID] || hj.Status != watchdog.StatusRunning {
										continue
									}
									if isStaleOrSyntheticHarnessActiveJob(hj) {
										continue
									}
									livePID := hj.HarnessPID
									if livePID <= 0 {
										livePID = hj.PID
									}
									if (livePID > 0 && watchdog.IsProcessAlive(livePID)) || (hj.ToolPID > 0 && watchdog.IsProcessAlive(hj.ToolPID)) {
										if livePID <= 0 {
											livePID = hj.ToolPID
										}
										seen[hj.JobID] = true
										jobs = append(jobs, controlplane.ActiveJob{
											JobID:        hj.JobID,
											WorktreePath: hj.WorktreePath,
											PID:          livePID,
										})
									}
								}
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
						// If watchdog is actively managing, completed, or auto-resuming this job, do not abandon it to control plane
						if wd != nil {
							if status, ok := wd.JobStatus(r.JobID); ok && (status == watchdog.StatusCompleted || status == watchdog.StatusRunning || status == watchdog.StatusAutoResuming) {
								sup.MarkIdle(r.WorktreePath)
								continue
							}
						}
						if r.JobID != "" {
							releaseOnJobGateLease(r.JobID)
							_ = client.AbandonJob(ctx, r.JobID, r.Reason)
						}
					}
					// Host resource telemetry check
					if home, err := os.UserHomeDir(); err == nil && home != "" {
						_, _ = preflight.FreeSpace(home)
					}
					// Periodically inspect and self-heal iazio-harvester service on darwin/linux
					lastHarvesterCheckMu.Lock()
					shouldCheckHarvester := lastHarvesterCheck.IsZero() || time.Since(lastHarvesterCheck) >= harvesterHealInterval
					if shouldCheckHarvester {
						lastHarvesterCheck = time.Now()
					}
					lastHarvesterCheckMu.Unlock()
					if shouldCheckHarvester {
						homeDir := strings.TrimSpace(getenv("HOME"))
						if homeDir == "" {
							homeDir, _ = os.UserHomeDir()
						}
						healHarvesterServiceIfNeeded(ctx, homeDir, runtime.GOOS)
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
				cpClient := worker.NewHTTPControlPlaneClient(api, authToken, nil, spooler)
				cpClient.TokenProvider = tokenProvider
				cliInvoker := harness.NewCLIInvoker("", "")
				disp = worker.NewDispatcher(cpClient, cliInvoker, host)
				disp.SetHostGate(hostGate)
				wireDispatcherReadinessAndIdleWorktrees(disp, &client, eagerUpdater, getRepos)
				disp.SetHarvestFlusher(func(hCtx context.Context, convID string) (int, string, error) {
					return flushHarvestConversationDetailed(hCtx, convID)
				})
				var pendingSelfRestart atomic.Bool
				disp.SetPreDispatchHook(buildPreDispatchHook(eagerUpdater, &updatingTools, &pendingSelfRestart))
				disp.SetPostJobHook(func(ctx context.Context, job *worker.Job) {
					if eagerUpdater != nil {
						eagerUpdater.TriggerEvaluation("post_job_completion")
					}
					t, f, v, rem, seq := snapshotPendingRemote()
					if rem {
						res, deferred, err := doToolUpdateDetailed(runCtx, t, f, v)
						if !deferred {
							clearPendingRemoteIfSeq(seq)
						}
						if err == nil && !deferred && res.AgentUpdated {
							pendingSelfRestart.Store(true)
						}
					}
					if pendingSelfRestart.CompareAndSwap(true, false) {
						restartOrExecSelf(stdout)
						cancel()
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
						pendingSelfRestart.Store(true)
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
							if areAllReposCleanAndHarnessIdle(runCtx, client, sup, host, api, false, nil, hostGate, disp) {
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
										t, f, v, rem, seq := snapshotPendingRemote()

										if rem {
											res, deferred, err := doToolUpdateDetailed(runCtx, t, f, v)
											if !deferred {
												clearPendingRemoteIfSeq(seq)
											}
											if err == nil && !deferred && res.AgentUpdated {
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
				if runCtx.Err() != nil {
					return 0, runCtx.Err()
				}
				argv := []string{
					"iazio-harness", "run",
					"--job-id", job.JobID,
					"--api-url", job.APIURL,
					"--worktree-path", job.WorktreePath,
					"--docs-hub-path", job.DocsHubPath,
				}
				if job.ConversationID != "" {
					argv = append(argv, "--kind", "resume", "--resume-conversation", job.ConversationID)
				} else {
					if repolock.IsWorktreeDirty(job.WorktreePath) {
						argv = append(argv, "--kind", "resume")
					} else {
						kind := job.Kind
						if kind == "" || kind == "resume" {
							kind = "story_execution"
						}
						argv = append(argv, "--kind", kind)
					}
				}
				if job.AttachToolPID > 0 {
					argv = append(argv, "--attach-tool-pid", strconv.Itoa(job.AttachToolPID))
				}
				newPID, err := wdRunner.Start(argv)
				return newPID, err
			}
			wdStateFile := ""
			if isTestBinary() {
				if h := strings.TrimSpace(getenv("HOME")); h != "" {
					wdStateFile = filepath.Join(h, ".iazio", "watchdog-active-jobs.json")
				} else {
					wdStateFile = filepath.Join(os.TempDir(), fmt.Sprintf("iazio-test-watchdog-%d.json", os.Getpid()))
				}
			}
			wd = watchdog.New(watchdog.Options{
				StateFile: wdStateFile,
				Runner:    wdRunner,
				Spawner:   spawner,
				OnOrphanTool: func(job watchdog.TrackedJob) {
					if runCtx.Err() != nil {
						return
					}
					msg := fmt.Sprintf("\n[ASE-SUPERVISOR] [watchdog.orphan_tool_detected] Harness process (PID %d) terminated, but AI tool process (ToolPID %d, engine: %s) is STILL RUNNING for conversation %s in %s. Attaching new supervisor...\n",
						job.PID, job.ToolPID, job.Engine, job.ConversationID, job.WorktreePath)
					if stdout != nil {
						fmt.Fprint(stdout, msg)
					}
					_ = client.PostChunk(runCtx, job.JobID, "stdout", msg)
				},
				OnAutoResume: func(job watchdog.TrackedJob, reason string, attempt, max int) {
					if runCtx.Err() != nil {
						return
					}
					promptDesc := "resume"
					if job.ConversationID == "" {
						if repolock.IsWorktreeDirty(job.WorktreePath) {
							promptDesc = "complete dirty ASE flow"
						} else {
							promptDesc = "restart story flow"
						}
					}
					msg := fmt.Sprintf("\n[ASE-SUPERVISOR] [watchdog.auto_resume] Harness process (PID %d) and AI tool terminated. Story incomplete. Auto-resuming session (conversation: %q, attempt %d/%d, reason: %s) with prompt mode %q...\n",
						job.PID, job.ConversationID, attempt, max, reason, promptDesc)
					if stdout != nil {
						fmt.Fprint(stdout, msg)
					}
					_ = client.PostChunk(runCtx, job.JobID, "stdout", msg)
				},
				OnJobCompleted: func(job watchdog.TrackedJob) {
					releaseOnJobGateLease(job.JobID)
					if job.WorktreePath != "" {
						sup.MarkIdle(job.WorktreePath)
					}
				},
				OnJobTerminalFailure: func(job watchdog.TrackedJob, reason string) {
					releaseOnJobGateLease(job.JobID)
					if runCtx.Err() != nil {
						return
					}
					msg := fmt.Sprintf("\n[ASE-SUPERVISOR] [watchdog.terminal_failure] Execution terminated permanently for job %s: %s\n",
						job.JobID, reason)
					if stderr != nil {
						fmt.Fprint(stderr, msg)
					}
					_ = client.PostChunk(runCtx, job.JobID, "stderr", msg)
					exitOpts := controlplane.ExitOptions{
						ExitCode:                 1,
						TerminationReasonCode:    "CIRCUIT_BREAKER_TRIPPED",
						TerminationReasonMessage: reason,
						Resumable:                false,
						NonResumableReason:       reason,
					}
					_ = client.PostExit(runCtx, job.JobID, exitOpts)
					if job.WorktreePath != "" {
						sup.MarkIdle(job.WorktreePath)
					}
				},
			})
			wd.StartWatchguard(runCtx, 5*time.Second)
			defer func() {
				cancel()
				repoMonitor.Wait()
				wd.WaitWatchguard()
			}()

			reattached := sup.ScanAndReattachRuns(wdRunner)
			for _, r := range reattached {
				if stdout != nil {
					fmt.Fprintf(stdout, "[supervisor] Re-attached to running job %s (PID %d) in %s\n", r.JobID, r.PID, r.WorktreePath)
				}
				wd.Track(watchdog.TrackedJob{
					JobID:        r.JobID,
					WorktreePath: r.WorktreePath,
					DocsHubPath:  r.DocsHubPath,
					APIURL:       r.APIURL,
					PID:          r.PID,
					HarnessPID:   r.PID,
					Status:       watchdog.StatusRunning,
				})
			}

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

				// Repo lock verification: refuse duplicate/concurrent jobs in the same worktree (cross-platform liveness via watchdog.IsProcessAlive)
				lockInfo, checkPID, repoBusy := checkRepoLockBusy(a.WorktreePath, a.ID, watchdog.IsProcessAlive)
				if repoBusy {
					if stderr != nil {
						fmt.Fprintf(stderr, "[agent] refusing concurrent job %s on %s: worktree actively locked by job %s (pid %d)\n",
							a.ID, a.WorktreePath, lockInfo.JobID, checkPID)
					}
					_ = client.AbandonJob(runCtx, a.ID, reasonRepoBusy)
					return fmt.Errorf("repository %s busy with job %s", a.WorktreePath, lockInfo.JobID)
				}

				convID := ResolveDispatchConversationID(a, lockInfo.ConversationID, lockInfo.StoryID, lockInfo.Status)

				// Reserve worktree state across supervisor, dispatcher, and in-flight prep table before running multi-second reaper/sanitization.
				if busyOwner, busy := tryReserveOnJobWorktree(&inFlightPrepMu, inFlightPrepJobs, sup, disp, a.ID, a.WorktreePath); busy {
					if stderr != nil {
						fmt.Fprintf(stderr, "[agent] refusing concurrent job %s on %s: worktree already reserved by job %s\n",
							a.ID, a.WorktreePath, busyOwner)
					}
					_ = client.AbandonJob(runCtx, a.ID, reasonRepoBusy)
					return fmt.Errorf("repository %s busy with job %s", a.WorktreePath, busyOwner)
				}
				defer func() {
					inFlightPrepMu.Lock()
					delete(inFlightPrepJobs, a.ID)
					inFlightPrepMu.Unlock()
				}()

				if hostGate != nil {
					releaseOnJobGateLease(a.ID)
					relGate, gateErr := hostGate.AcquireExecutionLease(runCtx, a.ID, a.WorktreePath)
					if gateErr != nil {
						_ = client.AbandonJob(runCtx, a.ID, reasonRepoBusy)
						return gateErr
					}
					onJobGateMu.Lock()
					onJobGateLeases[a.ID] = relGate
					onJobGateMu.Unlock()
				}
				leaseHandedOff := false
				defer func() {
					if !leaseHandedOff {
						releaseOnJobGateLease(a.ID)
					}
				}()

				var prepRepoLock *repolock.LockHandle
				if a.WorktreePath != "" {
					if h, lockErr := repolock.Acquire(a.WorktreePath, repolock.LockInfo{
						JobID:          a.ID,
						ScheduleID:     a.ScheduleID,
						PID:            os.Getpid(),
						ToolPID:        lockInfo.ToolPID,
						ConversationID: convID,
						StoryID:        a.StoryID,
						Status:         "RUNNING",
					}, watchdog.IsProcessAlive); lockErr == nil {
						prepRepoLock = h
					} else {
						releaseOnJobGateLease(a.ID)
						_ = client.AbandonJob(runCtx, a.ID, reasonRepoBusy)
						return lockErr
					}
				}
				releasePrepRepoLock := func() {
					if prepRepoLock != nil {
						_ = prepRepoLock.Release()
						prepRepoLock = nil
					}
				}
				defer releasePrepRepoLock()

				prepCtx, prepCancel := context.WithCancel(runCtx)
				defer prepCancel()
				go func() {
					interval := prepHeartbeatInterval
					if interval <= 0 {
						interval = 10 * time.Second
					}
					ticker := time.NewTicker(interval)
					defer ticker.Stop()
					for {
						select {
						case <-prepCtx.Done():
							return
						case <-ticker.C:
							_ = client.HeartbeatJob(prepCtx, a.ID, a.LeaseToken)
						}
					}
				}()

				// Phase 2 of Two-Phase Lease Handshake: Acknowledge lease to transition from LEASED to RUNNING
				if a.LeaseToken != "" {
					if ackErr := client.AckLease(runCtx, a.ID, a.LeaseToken, os.Getpid(), convID); ackErr != nil {
						if stderr != nil {
							fmt.Fprintf(stderr, "[agent] Warning: lease ack failed for job %s: %v\n", a.ID, ackErr)
						}
					}
				}

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

				prepCancel()
				releasePrepRepoLock()
				err := sup.Spawn(wdRunner, a.ID, api, a.WorktreePath, a.DocsHubPath, "", spawnExtras...)
				inFlightPrepMu.Lock()
				delete(inFlightPrepJobs, a.ID)
				inFlightPrepMu.Unlock()
				if err != nil {
					releaseOnJobGateLease(a.ID)
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
					Kind:           a.Kind,
					Engine:         a.Engine,
					Prompt:         a.Prompt,
					StoryID:        a.StoryID,
					ConversationID: convID,
					PID:            pid,
					HarnessPID:     pid,
					ToolPID:        toolPID,
				})
				leaseHandedOff = true
				return nil
			}

			loopErr := userservice.RunForever(runCtx, userservice.LoopConfig{
				ServiceName:    "iazio-agent",
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     30 * time.Second,
				ShouldPause: func() bool {
					return !isTestBinary() && home != "" && iaziouser.IsMaintenancePaused(home)
				},
				OnError: func(err error, attempt int, backoff time.Duration) {
					if sink != nil {
						sink.Error("iazio-agent controlplane loop error; supervising restart", err, map[string]any{
							"attempt": attempt,
							"backoff": backoff.String(),
						})
					}
				},
				OnPanic: func(recovered any, stack []byte, attempt int, backoff time.Duration) {
					if sink != nil {
						sink.Error("iazio-agent controlplane loop panic recovered; supervising restart", fmt.Errorf("%v", recovered), map[string]any{
							"attempt": attempt,
							"backoff": backoff.String(),
							"stack":   string(stack),
						})
					}
				},
			}, func(stepCtx context.Context) error {
				err := client.Loop(stepCtx, host, kind, 3*time.Second, onJob, profile)
				if isTestBinary() && stepCtx.Err() == nil {
					cancel()
				}
				return err
			})
			if errors.Is(loopErr, context.Canceled) || errors.Is(loopErr, context.DeadlineExceeded) || errors.Is(loopErr, userservice.ErrMaintenancePaused) {
				return nil
			}
			return loopErr
		}
		return nil
	default:
		return fmt.Errorf("unknown command %s", args[0])
	}
}

func resolveControlPlaneToken(getenv func(string) string) string {
	if getenv != nil {
		for _, k := range []string{"IAZIO_AUTH_TOKEN", "IAZIO_HARNESS_API_TOKEN", "HARNESS_AUTH_TOKEN", "IAZIO_ACCESS_TOKEN"} {
			if v := strings.TrimSpace(getenv(k)); v != "" {
				return v
			}
		}
	}
	hasExplicitConfig := getenv != nil && (strings.TrimSpace(getenv("IAZIO_AGENT_CONFIG")) != "" || strings.TrimSpace(getenv("IAZIO_AGENT_CONFIG_PATH")) != "")
	if !isTestBinary() || hasExplicitConfig {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		tok, err := auth.Bearer(ctx, getenv)
		cancel()
		if err == nil && strings.TrimSpace(tok) != "" {
			return strings.TrimSpace(tok)
		}
	}
	if !isTestBinary() {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			for _, fn := range []string{"agent.json", "config.json"} {
				if data, err := os.ReadFile(filepath.Join(home, ".iazio", fn)); err == nil {
					var cfg struct {
						AuthToken string `json:"auth_token"`
						Token     string `json:"token"`
					}
					if json.Unmarshal(data, &cfg) == nil {
						if v := strings.TrimSpace(cfg.AuthToken); v != "" {
							return v
						}
						if v := strings.TrimSpace(cfg.Token); v != "" {
							return v
						}
					}
				}
			}
		}
	}
	return ""
}

func flushHarvestConversationDetailed(ctx context.Context, conversationID string) (int, string, error) {
	if strings.TrimSpace(conversationID) == "" {
		return 0, "", nil
	}
	fCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	harvesterBin := supervisor.ResolveBinary("iazio-harvester")
	cmd := procutil.CommandContext(fCtx, harvesterBin, "once", "--conversation", conversationID)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[agent] Warning: harvest flush for conversation %s failed: %v (output: %s)\n",
			conversationID, err, strings.TrimSpace(string(out)))
		return 0, "", err
	}
	var parsed struct {
		TurnCount             int    `json:"turn_count"`
		FinalTurnCount        int    `json:"final_turn_count"`
		TranscriptSHA256      string `json:"transcript_sha256"`
		FinalTranscriptSHA256 string `json:"final_transcript_sha256"`
	}
	if len(out) > 0 && json.Unmarshal(out, &parsed) == nil {
		turns := parsed.FinalTurnCount
		if turns == 0 {
			turns = parsed.TurnCount
		}
		sha := parsed.FinalTranscriptSHA256
		if sha == "" {
			sha = parsed.TranscriptSHA256
		}
		return turns, sha, nil
	}
	return 0, "", nil
}

func flushHarvestConversation(ctx context.Context, conversationID string) error {
	_, _, err := flushHarvestConversationDetailed(ctx, conversationID)
	return err
}

func runAuth(ctx context.Context, args []string, stdout io.Writer, getenv func(string) string) error {
	if len(args) == 0 {
		return fmt.Errorf("auth status|login|logout")
	}
	switch args[0] {
	case "login", "status", "logout":
	default:
		return fmt.Errorf("unknown auth command: %s", args[0])
	}
	if getenv == nil {
		getenv = os.Getenv
	}
	configPath := auth.ConfigPathFrom("", getenv)
	var lastLoginErr error
	opts := coreauth.CLIOptions{
		Stdout:     stdout,
		Stderr:     os.Stderr,
		Getenv:     getenv,
		ConfigPath: configPath,
		ClientID:   auth.ClientID,
		LoginFunc: func(loginCtx context.Context, cfg *coreauth.Config, apiURL string) error {
			if auth.Noninteractive(getenv) {
				lastLoginErr = fmt.Errorf("auth login refused: noninteractive session")
				return lastLoginErr
			}
			lastLoginErr = auth.Login(loginCtx, auth.LoginOptions{Getenv: getenv, Stdout: stdout})
			return lastLoginErr
		},
	}
	code := coreauth.RunAuthCLI(ctx, "iazio-agent", args, opts)
	if code != 0 {
		if lastLoginErr != nil {
			return lastLoginErr
		}
		return fmt.Errorf("auth command failed with exit code %d", code)
	}
	return nil
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
		canonicalDir := filepath.Join(home, ".local", "bin")
		binName := "iazio-agent"
		if runtime.GOOS == "windows" {
			canonicalDir = filepath.Join(home, "bin")
			binName += ".exe"
		}
		bin = filepath.Join(canonicalDir, binName)
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
	case "reconcile":
		home, _ := os.UserHomeDir()
		if err := service.ReconcileServiceConfiguration(home, bin, execCommand); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "iazio-agent user service configuration reconciled")
		return nil
	}
	switch goos {
	case "darwin":
		return applyDarwin(action, bin, stdout)
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
	case "install", "start", "stop", "restart", "uninstall", "status", "dev-install", "pause", "resume", "revive-check", "reconcile":
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
		out, err := execCommandOutput("systemctl", "--user", "status", "iazio-agent.service", "--no-pager")
		if stdout != nil {
			fmt.Fprint(stdout, string(out))
		}
		return err
	}
	_, err := service.Apply("linux", action, bin, false, force, execCommand)
	return err
}

func applyDarwin(action, bin string, stdout ...io.Writer) error {
	var outWriter io.Writer = os.Stdout
	if len(stdout) > 0 && stdout[0] != nil {
		outWriter = stdout[0]
	}
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
			fmt.Fprintln(outWriter, "running (io.iazio.iazio-agent)")
			wOut, wErr := execCommandOutput("launchctl", "print", darwinWatcherTarget())
			if wErr == nil && !strings.Contains(string(wOut), "job state = exited") {
				fmt.Fprintln(outWriter, "active (io.iazio.iazio-agent-watcher)")
			}
			if wdOut, wdErr := execCommandOutput("launchctl", "print", darwinWatchdogTarget()); wdErr == nil && !strings.Contains(string(wdOut), "job state = exited") {
				fmt.Fprintln(outWriter, "active (io.iazio.iazio-agent-watchdog)")
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
	if err := service.InstallDarwinResilience(home, filepath.Join(home, ".local", "bin", "iazio-agent"), execCommand); err != nil {
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
		if home, hErr := os.UserHomeDir(); hErr == nil && home != "" {
			if alive, pid := service.StatusUserDaemonAt(home); alive && pid > 0 {
				fmt.Fprintf(stdout, "running (pid: %d)\n", pid)
				return nil
			}
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

var (
	prepHeartbeatInterval = 10 * time.Second
	harvesterHealInterval = 60 * time.Second
	inspectToolServiceFn  = func(toolName, goos string) (bool, string) {
		installed, _, status, _ := inventory.InspectToolService(toolName, goos, nil)
		return installed, status
	}
	reconcileHarvesterServiceFn = func(ctx context.Context, harvesterBin string) error {
		return procutil.CommandContext(ctx, harvesterBin, "service", "reconcile").Run()
	}
	restartToolServiceFn = service.RestartToolService
)

func healHarvesterServiceIfNeeded(ctx context.Context, home, goos string) {
	if goos != "darwin" && goos != "linux" && goos != "windows" {
		return
	}
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil || home == "" {
			return
		}
	}
	harvesterBin := filepath.Join(home, ".local", "bin", "iazio-harvester")
	if goos == "windows" {
		harvesterBin = filepath.Join(home, "bin", "iazio-harvester.exe")
	}
	if _, err := os.Stat(harvesterBin); err != nil {
		return
	}
	_, svcStatus := inspectToolServiceFn("iazio-harvester", goos)
	if svcStatus == "failed" || svcStatus == "inactive" || svcStatus == "installed" || svcStatus == "ready" || svcStatus == "not_installed" {
		_ = reconcileHarvesterServiceFn(ctx, harvesterBin)
		if _, postStatus := inspectToolServiceFn("iazio-harvester", goos); postStatus != "active" {
			_ = restartToolServiceFn("iazio-harvester")
		}
	}
}

func ensureCanonicalSingleHome() {
	if isTestBinary() {
		return
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return
	}
	ensureCanonicalSingleHomeAt(home, service.ReconcileServiceConfiguration)
}

func ensureCanonicalSingleHomeAt(home string, reconcileFn func(string, string, func(string, ...string) error) error) {
	if home == "" {
		return
	}
	binName := "iazio-agent"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	canonicalDir := filepath.Join(home, ".local", "bin")
	if runtime.GOOS == "windows" {
		canonicalDir = filepath.Join(home, "bin")
	}
	canonicalPath := filepath.Join(canonicalDir, binName)

	if _, err := os.Stat(canonicalPath); err != nil {
		_, _ = installCanonicalBinary()
	}

	if reconcileFn == nil {
		reconcileFn = service.ReconcileServiceConfiguration
	}
	_ = reconcileFn(home, canonicalPath, nil)

	// Purge secondary installation binary and stale staged files from ~/.iazio/bin/
	legacyDir := filepath.Join(home, ".iazio", "bin")
	shadowPath := filepath.Join(legacyDir, binName)
	_ = os.Remove(shadowPath)
	_ = os.Remove(shadowPath + ".old")
	_ = os.Remove(shadowPath + ".next")
	_ = os.Remove(filepath.Join(legacyDir, "iazio-harvester.next"))
	if entries, err := os.ReadDir(legacyDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			lower := strings.ToLower(name)
			if strings.HasSuffix(lower, ".next") || strings.HasSuffix(lower, ".old") || strings.HasSuffix(lower, ".tmp") || strings.HasSuffix(lower, ".bak") {
				_ = os.Remove(filepath.Join(legacyDir, name))
			}
		}
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
	canonicalDir := filepath.Join(home, ".local", "bin")
	if runtime.GOOS == "windows" {
		canonicalDir = filepath.Join(home, "bin")
	}
	dest := filepath.Join(canonicalDir, binName)
	if err := copyBinary(src, dest); err != nil {
		return "", err
	}
	if !isTestBinary() {
		_ = service.ReconcileServiceConfiguration(home, dest, nil)
	}
	// Purge secondary installation binary from ~/.iazio/bin/
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", binName))
	return dest, nil
}

func copyBinary(src, dest string) error {
	if src == dest {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	// Unlink existing destination if symlink to prevent write-through
	if lfi, err := os.Lstat(dest); err == nil && (lfi.Mode()&os.ModeSymlink != 0) {
		_ = os.Remove(dest)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return installExecutableSafeDirect(dest, data)
}

func isServiceControlCommand(name string) bool {
	base := strings.ToLower(strings.TrimSuffix(filepath.Base(name), ".exe"))
	switch base {
	case "schtasks", "systemctl", "launchctl", "loginctl", "crontab", "taskkill", "pkill", "killall":
		return true
	default:
		return false
	}
}

var execCommand = func(name string, args ...string) error {
	if isTestBinary() && isServiceControlCommand(name) {
		return nil
	}
	cmd := procutil.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", name, strings.TrimSpace(string(out)))
	}
	return nil
}

var execCommandOutput = func(name string, args ...string) ([]byte, error) {
	if isTestBinary() && isServiceControlCommand(name) {
		return nil, errors.New("service control command disabled in test binary")
	}
	cmd := procutil.Command(name, args...)
	return cmd.CombinedOutput()
}

func parseService(args []string) (action string, dry, force bool, err error) {
	if len(args) == 0 {
		return "", false, false, fmt.Errorf("service install|uninstall|start|stop|restart|status")
	}
	action = args[0]
	switch action {
	case "install", "uninstall", "start", "stop", "restart", "status", "dev-install", "pause", "resume", "revive-check", "reconcile":
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
	return clean == "tmp" ||
		strings.HasPrefix(clean, "tmp/") ||
		clean == ".system_generated" ||
		strings.HasPrefix(clean, ".system_generated/") ||
		clean == ".gemini" ||
		strings.HasPrefix(clean, ".gemini/") ||
		clean == ".agents" ||
		strings.HasPrefix(clean, ".agents/")
}

func isGitSubmoduleDir(path string) bool {
	gitPath := filepath.Join(path, ".git")
	_, err := os.Stat(gitPath)
	return err == nil
}

func scanRepoFleetStatus(ctx context.Context, repoPath string) bool {
	// First: verify repoPath is a valid git repository top-level root
	topCmd := procutil.CommandContext(ctx, "git", "-C", repoPath, "rev-parse", "--show-toplevel")
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

	// Try iazio-harness fleet-status --repo <repoPath> --auto-remediate --json first
	harnessBin := resolveHarnessBinaryPath()
	harnessCmd := procutil.CommandContext(ctx, harnessBin, "fleet-status", "--repo", repoPath, "--auto-remediate", "--json")
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
				if len(res.UntrackedFiles) == 0 {
					return false
				}
				onlyEphemeral := true
				for _, f := range res.UntrackedFiles {
					if !isDocOrEphemeralFile(f) {
						onlyEphemeral = false
						break
					}
				}
				if onlyEphemeral && res.SubmodulesClean {
					return true
				}
				return false
			}
			if res.WorktreeState != "" {
				return false
			}
			if res.IsClean && res.SubmodulesClean && len(res.ModifiedFiles) == 0 && len(res.DirtySubmodules) == 0 {
				onlyEphemeral := true
				for _, f := range res.UntrackedFiles {
					if !isDocOrEphemeralFile(f) {
						onlyEphemeral = false
						break
					}
				}
				if onlyEphemeral {
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
	// Allow untracked gitignored ephemeral scratch directories and submodule commit pointer drift (CLEAN_SUBMODULE_DRIFT)
	gitCmd := procutil.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain")
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
				subCmd := procutil.CommandContext(ctx, "git", "-C", candidateSub, "status", "--porcelain")
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

func checkRepoLockBusy(worktreePath, jobID string, isAlive func(int) bool) (repolock.LockInfo, int, bool) {
	if isAlive == nil {
		isAlive = watchdog.IsProcessAlive
	}
	lockInfo, lockErr := repolock.Read(worktreePath)
	if lockErr == nil && lockInfo.JobID != "" && lockInfo.JobID != jobID {
		checkPID := lockInfo.PID
		if lockInfo.HarnessPID > 0 {
			checkPID = lockInfo.HarnessPID
		}
		if checkPID > 0 && isAlive(checkPID) {
			return lockInfo, checkPID, true
		}
	}
	return lockInfo, 0, false
}

func areAllReposCleanAndHarnessIdle(ctx context.Context, client controlplane.Client, sup *supervisor.Sup, host, api string, force bool, stdout io.Writer, extraTrackers ...any) bool {
	if sup != nil && sup.HasRunningHarness() {
		if stdout != nil {
			fmt.Fprintln(stdout, "[SIR: IDLE_UPDATE_DEFERRED] reason=\"harness execution is active\"")
		}
		return false
	}
	if client.ActiveJobs != nil && len(client.ActiveJobs()) > 0 {
		if stdout != nil {
			fmt.Fprintln(stdout, "[SIR: IDLE_UPDATE_DEFERRED] reason=\"active controlplane or dispatcher jobs in flight\"")
		}
		return false
	}
	for _, tracker := range extraTrackers {
		switch t := tracker.(type) {
		case *supervisor.HostExecutionAndUpdateGate:
			if t != nil && (t.ActiveCount() > 0 || t.IsMaintenanceHeld()) {
				if stdout != nil {
					fmt.Fprintln(stdout, "[SIR: IDLE_UPDATE_DEFERRED] reason=\"host execution gate has active execution or maintenance lease\"")
				}
				return false
			}
		case *worker.Dispatcher:
			if t != nil && len(t.ActiveJobs()) > 0 {
				if stdout != nil {
					fmt.Fprintln(stdout, "[SIR: IDLE_UPDATE_DEFERRED] reason=\"worker dispatcher has active jobs\"")
				}
				return false
			}
		}
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

func resolveSelfBinaryPath(goos string, selfBin string, home string, args []string) string {
	cleanSelf := strings.TrimSuffix(selfBin, " (deleted)")
	if goos == "windows" {
		cleanSelf = strings.TrimSuffix(cleanSelf, ".old")
	}

	var candidates []string
	if cleanSelf != "" {
		candidates = append(candidates, cleanSelf)
		if resolved, err := filepath.EvalSymlinks(cleanSelf); err == nil && resolved != cleanSelf {
			candidates = append(candidates, resolved)
		}
	}
	if home != "" {
		if goos == "windows" {
			candidates = append(candidates,
				filepath.Join(home, "bin", "iazio-agent.exe"),
				filepath.Join(home, ".local", "bin", "iazio-agent.exe"),
			)
		} else {
			candidates = append(candidates,
				filepath.Join(home, ".local", "bin", "iazio-agent"),
			)
		}
	}
	if len(args) > 0 && filepath.IsAbs(args[0]) {
		candidates = append(candidates, args[0])
	}

	for _, cand := range candidates {
		if fi, err := os.Stat(cand); err == nil && !fi.IsDir() {
			if goos == "windows" || fi.Mode()&0111 != 0 {
				return cand
			}
		}
	}
	if cleanSelf != "" {
		return cleanSelf
	}
	return selfBin
}

func restartOrExecSelf(stdout io.Writer) {
	if stdout != nil {
		fmt.Fprintln(stdout, "[supervisor] iazio-agent self-updated; executing in-PID handoff to updated binary...")
	}
	flushTelemetryAndCloseSink()
	selfBin, _ := os.Executable()
	home, _ := os.UserHomeDir()
	targetBin := resolveSelfBinaryPath(runtime.GOOS, selfBin, home, os.Args)

	if runtime.GOOS == "windows" {
		if isTesting {
			return
		}
		env := append(os.Environ(), fmt.Sprintf("IAZIO_HANDOFF_PID=%d", os.Getpid()))
		spawnCmd, _ := buildWindowsSelfRestartCommand(targetBin, os.Args, env)
		_ = userservice.TriggerWindowsScheduledTaskRestart(
			"iazio-agent",
			func() error {
				releaseAgentSingleInstanceLock()
				return nil
			},
			nil,
			func() error { return spawnCmd.Start() },
		)
		os.Exit(0)
	}

	execErr := syscallExec(targetBin, os.Args, os.Environ())
	if execErr != nil {
		if isTesting {
			return
		}
		fmt.Fprintf(os.Stderr, "[SIR: AUTO_RESTART_FAIL] syscall.Exec(%s) failed: %v; attempting background restart and service revive\n", targetBin, execErr)
		// Fallback: spawn background restart so the host never stays offline
		cmd := procutil.Command(targetBin, os.Args[1:]...)
		cmd.Env = os.Environ()
		if startErr := cmd.Start(); startErr == nil {
			os.Exit(0)
		}
		// Attempt to revive via systemd if available (non-blocking to prevent 90s deadlock)
		_ = procutil.Command("systemctl", "--user", "--no-block", "restart", "iazio-agent.service").Run()
		os.Exit(0)
	}
}

func resolveHarnessBinaryPath() string {
	binName := "iazio-harness"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		candidates := []string{
			filepath.Join(home, ".local", "bin", binName),
			filepath.Join(home, "bin", binName),
			filepath.Join(home, ".iazio", "bin", binName),
		}
		if runtime.GOOS == "windows" {
			candidates = []string{
				filepath.Join(home, "bin", binName),
				filepath.Join(home, ".local", "bin", binName),
				filepath.Join(home, ".iazio", "bin", binName),
			}
		}
		for _, p := range candidates {
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	if lp, err := exec.LookPath(binName); err == nil && lp != "" {
		return lp
	}
	return binName
}

func isStoryCompletedOnDiskFallback(repoPath, storyID string) bool {
	storyID = strings.TrimSpace(storyID)
	if !strings.HasPrefix(storyID, "STORY-") {
		return false
	}
	indexPath := filepath.Join(repoPath, "docs", "stories", "index.yml")
	if b, err := os.ReadFile(indexPath); err == nil {
		lines := strings.Split(string(b), "\n")
		inTarget := false
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "- id:") || strings.HasPrefix(trimmed, "id:") {
				val := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(trimmed, "- id:"), "id:"))
				val = strings.Trim(val, `"'`)
				inTarget = (val == storyID)
				continue
			}
			if inTarget && strings.HasPrefix(trimmed, "status:") {
				st := strings.ToUpper(strings.Trim(strings.TrimSpace(strings.TrimPrefix(trimmed, "status:")), `"'`))
				if st == "DONE" || st == "COMPLETED" || st == "ARCHIVED" || st == "CANCELLED" {
					return true
				}
				break
			}
		}
	}
	return false
}

func isStaleOrSyntheticHarnessActiveJob(hj watchdog.TrackedJob) bool {
	id := strings.TrimSpace(hj.JobID)
	if id == "" || id == "job-cp-fallback" || strings.HasPrefix(id, "job-reap-") || strings.HasPrefix(id, "job-missing-") {
		return true
	}
	wt := hj.WorktreePath
	if strings.Contains(wt, "TestExecuteJob_") || strings.Contains(wt, "/T/Test") || strings.Contains(wt, `\Temp\Test`) {
		return true
	}
	// Guard against recycled OS PIDs for old harness-active-jobs.json entries (>2h without lock ownership)
	if !hj.UpdatedAt.IsZero() && time.Since(hj.UpdatedAt) > 2*time.Hour {
		if wt == "" {
			return true
		}
		info, err := repolock.Read(wt)
		if err != nil || info.JobID != id {
			return true
		}
	}
	return false
}

type harnessFleetStatusPayload struct {
	Branch               string   `json:"branch"`
	HeadSHA              string   `json:"head_sha"`
	IsClean              bool     `json:"is_clean"`
	Ahead                int      `json:"ahead"`
	Behind               int      `json:"behind"`
	DirtyFilesCount      int      `json:"dirty_files_count"`
	UntrackedFilesCount  int      `json:"untracked_files_count"`
	HasActiveGitLock     bool     `json:"has_active_git_lock"`
	ModifiedFiles        []string `json:"modified_files"`
	UntrackedFiles       []string `json:"untracked_files"`
	SubmodulesClean      bool     `json:"submodules_clean"`
	DirtySubmodules      []string `json:"dirty_submodules"`
	NonMainSubmodules    []string `json:"non_main_submodules"`
	ASEState             string   `json:"ase_state"`
	ActiveStoryID        string   `json:"active_story_id"`
	StoryID              string   `json:"story_id"`
	AgentTool            string   `json:"agent_tool"`
	ToolLiveness         string   `json:"tool_liveness"`
	ExecutionProvenance  string   `json:"execution_provenance"`
	ActiveConversationID string   `json:"active_conversation_id"`
	MetadataYmlRaw       string   `json:"metadata_yml_raw"`
	MetadataYmlSHA       string   `json:"metadata_yml_sha"`
	MetadataYmlModTime   string   `json:"metadata_yml_mod_time"`
	GitVersion           string   `json:"git_version"`
}

func applyHarnessFleetStatusPayload(res harnessFleetStatusPayload, repoPath string, st *controlplane.RepoRuntimeStatus) {
	st.Branch = res.Branch
	st.HeadSHA = res.HeadSHA
	st.IsClean = res.IsClean
	st.Ahead = res.Ahead
	st.Behind = res.Behind
	st.DirtyFilesCount = res.DirtyFilesCount
	if st.DirtyFilesCount == 0 && len(res.ModifiedFiles) > 0 {
		st.DirtyFilesCount = len(res.ModifiedFiles)
	}
	st.UntrackedFilesCount = res.UntrackedFilesCount
	if st.UntrackedFilesCount == 0 && len(res.UntrackedFiles) > 0 {
		st.UntrackedFilesCount = len(res.UntrackedFiles)
	}
	st.HasActiveGitLock = res.HasActiveGitLock
	st.SubmodulesClean = res.SubmodulesClean
	st.DirtySubmodules = res.DirtySubmodules
	st.NonMainSubmodules = res.NonMainSubmodules
	st.ASEState = res.ASEState
	if st.ASEState == "" {
		st.ASEState = "IDLE"
	}
	st.ActiveStoryID = res.ActiveStoryID
	if st.ActiveStoryID == "" {
		st.ActiveStoryID = res.StoryID
	}
	if st.ActiveStoryID == "" {
		statePath := filepath.Join(repoPath, ".autopilot", "state.json")
		if b, err := os.ReadFile(statePath); err == nil {
			var s struct {
				State   string `json:"state"`
				StoryID string `json:"story_id"`
			}
			if json.Unmarshal(b, &s) == nil && s.StoryID != "" {
				st.ActiveStoryID = s.StoryID
				if st.ASEState == "" || st.ASEState == "IDLE" {
					st.ASEState = s.State
				}
			}
		}
	}
	st.ActiveTool = res.AgentTool
	st.ToolLiveness = res.ToolLiveness
	st.ExecutionProvenance = res.ExecutionProvenance
	if (st.ASEState == "IDLE" || st.ASEState == "FAILED_DIRTY") && st.ExecutionProvenance == "INDEPENDENT_USER_SESSION" {
		st.ExecutionProvenance = "NONE"
	}
	st.ActiveConversationID = res.ActiveConversationID
	if res.MetadataYmlRaw != "" {
		st.MetadataYmlRaw = res.MetadataYmlRaw
		st.MetadataYmlSHA = res.MetadataYmlSHA
		st.MetadataYmlModTime = res.MetadataYmlModTime
	}
	if res.GitVersion != "" {
		st.GitVersion = res.GitVersion
	}
	if len(res.ModifiedFiles) > 0 || len(res.UntrackedFiles) > 0 {
		var pLines []string
		for _, m := range res.ModifiedFiles {
			pLines = append(pLines, "M "+m)
		}
		for _, u := range res.UntrackedFiles {
			pLines = append(pLines, "?? "+u)
		}
		st.Porcelain = strings.Join(pLines, "\n")
	} else {
		st.Porcelain = ""
	}
}

func isASEFlowCompletedAndLastJobSucceeded(repoPath string, st controlplane.RepoRuntimeStatus) bool {
	if st.ASEState == "RUNNING" || st.ASEState == "IN_PROGRESS" || strings.EqualFold(st.ASEState, "active") || st.HasActiveGitLock {
		return false
	}
	if st.Branch != "main" && st.Branch != "master" {
		return false
	}
	if _, err := os.Stat(filepath.Join(repoPath, ".autopilot", "lock")); err == nil {
		return false
	}
	if _, err := os.Stat(filepath.Join(repoPath, ".ase", "active.lock")); err == nil {
		return false
	}
	if lockInfo, err := repolock.Read(repoPath); err == nil && lockInfo.PID > 0 && watchdog.IsProcessAlive(lockInfo.PID) {
		return false
	}

	hasCompletedASEEvidence := false
	statePath := filepath.Join(repoPath, ".autopilot", "state.json")
	if b, err := os.ReadFile(statePath); err == nil {
		var s struct {
			State              string `json:"state"`
			StoryID            string `json:"story_id"`
			VerificationStatus string `json:"verification_status"`
		}
		if json.Unmarshal(b, &s) == nil {
			if strings.EqualFold(s.State, "RUNNING") || strings.EqualFold(s.State, "IN_PROGRESS") ||
				strings.EqualFold(s.State, "FAILED") || strings.EqualFold(s.VerificationStatus, "FAILED") {
				return false
			}
			if s.StoryID != "" && !isStoryCompletedOnDiskFallback(repoPath, s.StoryID) {
				return false
			}
			if strings.EqualFold(s.VerificationStatus, "PASSED") || strings.EqualFold(s.State, "DONE") || strings.EqualFold(s.State, "COMPLETED") {
				hasCompletedASEEvidence = true
			}
		}
	}
	aseStatePath := filepath.Join(repoPath, ".autopilot", "ase-state.json")
	if b, err := os.ReadFile(aseStatePath); err == nil {
		var as struct {
			Status string `json:"status"`
			Phase  string `json:"phase"`
		}
		if json.Unmarshal(b, &as) == nil {
			if strings.EqualFold(as.Status, "RUNNING") || strings.EqualFold(as.Status, "IN_PROGRESS") || strings.EqualFold(as.Status, "FAILED") {
				return false
			}
			if strings.EqualFold(as.Status, "DONE") || strings.EqualFold(as.Phase, "completed") {
				hasCompletedASEEvidence = true
			}
		}
	}

	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cleanRepo := filepath.Clean(repoPath)
		var latestJob *watchdog.TrackedJob
		for _, stateFile := range []string{
			filepath.Join(home, ".iazio", "harness-active-jobs.json"),
			filepath.Join(home, ".iazio", "watchdog-state.json"),
		} {
			if raw, rErr := os.ReadFile(stateFile); rErr == nil {
				var jobs []watchdog.TrackedJob
				if json.Unmarshal(raw, &jobs) == nil {
					for i := range jobs {
						j := jobs[i]
						if filepath.Clean(j.WorktreePath) != cleanRepo {
							continue
						}
						if latestJob == nil || j.UpdatedAt.After(latestJob.UpdatedAt) {
							copyJ := j
							latestJob = &copyJ
						}
					}
				}
			}
		}
		if latestJob != nil {
			if latestJob.Status == watchdog.StatusRunning || latestJob.Status == watchdog.StatusAutoResuming || latestJob.Status == watchdog.StatusResumed {
				return false
			}
			if latestJob.Status == watchdog.StatusFailed || latestJob.Status == watchdog.StatusTerminatedUnexpectedly || latestJob.ExitCode != 0 {
				return false
			}
			if latestJob.Status == watchdog.StatusCompleted && latestJob.ExitCode == 0 {
				hasCompletedASEEvidence = true
			}
		}
	}

	return hasCompletedASEEvidence
}

func cleanOrRestoreFallbackSessionStoryFile(repoPath, sessionFile string) {
	if procutil.Command("git", "-C", repoPath, "ls-files", "--error-unmatch", ".session_story_id").Run() == nil {
		_ = procutil.Command("git", "-C", repoPath, "checkout", "HEAD", "--", ".session_story_id").Run()
		return
	}
	_ = os.Remove(sessionFile)
}

func resolveGitUpdateInterval(args []string, getenv func(string) string, stderr io.Writer) time.Duration {
	raw := ""
	if getenv != nil {
		raw = strings.TrimSpace(getenv("IAZIO_GIT_UPDATE_INTERVAL"))
	}
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--git-update-interval" && i+1 < len(args):
			i++
			raw = strings.TrimSpace(args[i])
		case strings.HasPrefix(args[i], "--git-update-interval="):
			raw = strings.TrimSpace(strings.TrimPrefix(args[i], "--git-update-interval="))
		}
	}
	if raw == "" {
		return supervisor.DefaultGitUpdateInterval
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		if stderr != nil {
			fmt.Fprintf(stderr, "[RepoMonitor] WARN: invalid git-update interval %q; defaulting to %v\n", raw, supervisor.DefaultGitUpdateInterval)
		}
		return supervisor.DefaultGitUpdateInterval
	}
	clamped := supervisor.ClampGitUpdateInterval(d)
	if clamped != d && stderr != nil {
		fmt.Fprintf(stderr, "[RepoMonitor] WARN: git-update interval %v outside permitted range [15m..1h]; clamping to %v\n", d, clamped)
	}
	return clamped
}

func probeRepoRuntime(ctx context.Context, repoPath, alias string) controlplane.RepoRuntimeStatus {
	st := controlplane.RepoRuntimeStatus{
		RepoPath:      repoPath,
		Alias:         alias,
		IsClean:       true,
		LastCheckedAt: time.Now(),
	}

	// Step 0: Before preflight/runtime status probing halts a worktree, sanitize idle ephemeral artifacts.
	_, _ = supervisor.SanitizeIdleEphemeralArtifacts(ctx, repoPath, nil, watchdog.IsProcessAlive)

	// Delegate repo evaluation and Tier 1/Tier 2 self-remediation to iazio-harness (SSOT Intelligence Engine).
	harnessSucceeded := false
	harnessBin := resolveHarnessBinaryPath()
	harnessCmd := procutil.CommandContext(ctx, harnessBin, "fleet-status", "--repo", repoPath, "--auto-remediate", "--json")
	if out, err := harnessCmd.Output(); err == nil && len(out) > 0 {
		var res harnessFleetStatusPayload
		if json.Unmarshal(out, &res) == nil && res.Branch != "" {
			harnessSucceeded = true
			applyHarnessFleetStatusPayload(res, repoPath, &st)

			// If git tree or submodules have dirty files after a completed ASE flow whose last job succeeded without errors,
			// trigger iazio-harness to execute "autopilot git-update" with autoremediation.
			hasDirtyTree := !st.IsClean || !st.SubmodulesClean || st.DirtyFilesCount > 0 || st.UntrackedFilesCount > 0 || len(st.DirtySubmodules) > 0
			if hasDirtyTree && isASEFlowCompletedAndLastJobSucceeded(repoPath, st) {
				remCmd := procutil.CommandContext(ctx, harnessBin, "repo", "remediate", "--repo", repoPath, "--strategy", "git-update", "--json")
				if remOut, remErr := remCmd.Output(); remErr == nil && len(remOut) > 0 {
					var remRes struct {
						Success     bool                       `json:"success"`
						Skipped     bool                       `json:"skipped"`
						FleetStatus *harnessFleetStatusPayload `json:"fleet_status"`
					}
					if json.Unmarshal(remOut, &remRes) == nil && remRes.Success && !remRes.Skipped && remRes.FleetStatus != nil && remRes.FleetStatus.Branch != "" {
						applyHarnessFleetStatusPayload(*remRes.FleetStatus, repoPath, &st)
					}
				}
			}
		}
	}

	if !harnessSucceeded {
		// Fallback git status & branch when iazio-harness binary is unavailable
		cmd := procutil.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain=v2", "--branch")
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
				} else if strings.HasPrefix(l, "# branch.oid ") {
					st.HeadSHA = strings.TrimPrefix(l, "# branch.oid ")
				} else if !strings.HasPrefix(l, "# ") {
					if strings.HasPrefix(l, "? ") {
						untrackedPath := strings.TrimSpace(strings.TrimPrefix(l, "? "))
						slash := strings.TrimPrefix(strings.ReplaceAll(untrackedPath, "\\", "/"), "./")
						if slash == ".iazio" || strings.HasPrefix(slash, ".iazio/") ||
							slash == ".autopilot" || strings.HasPrefix(slash, ".autopilot/") ||
							supervisor.IsEphemeralArtifactPath(slash) {
							continue
						}
					}
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

		// Submodules fallback check using native C builtin (submodule--helper status) to avoid spawning sh.exe on Windows
		subCmd := procutil.CommandContext(ctx, "git", "-C", repoPath, "submodule--helper", "status")
		subOut, err := subCmd.CombinedOutput()
		if err != nil {
			subCmd = procutil.CommandContext(ctx, "git", "-C", repoPath, "submodule", "status")
			subOut, err = subCmd.CombinedOutput()
		}
		if err == nil {
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

		// ASE state fallback
		statePath := filepath.Join(repoPath, ".autopilot", "state.json")
		if b, err := os.ReadFile(statePath); err == nil {
			var s struct {
				State   string `json:"state"`
				StoryID string `json:"story_id"`
			}
			if json.Unmarshal(b, &s) == nil {
				if s.State != "" {
					st.ASEState = s.State
				}
				if s.StoryID != "" {
					st.ActiveStoryID = s.StoryID
				}
			}
		}
		if st.ASEState == "" || st.ASEState == "IDLE" {
			if _, err := os.Stat(filepath.Join(repoPath, ".ase", "active.lock")); err == nil {
				st.ASEState = "RUNNING"
			} else if _, err := os.Stat(filepath.Join(repoPath, ".autopilot", "lock")); err == nil {
				st.ASEState = "RUNNING"
			}
		}
		// Detect ad-hoc ASE session bound via .session_story_id or scoped tmp/ase-resume-<storyID>.json
		sessionFile := filepath.Join(repoPath, ".session_story_id")
		if sessionBytes, err := os.ReadFile(sessionFile); err == nil {
			storyID := strings.TrimSpace(string(sessionBytes))
			if strings.HasPrefix(storyID, "STORY-") {
				if isStoryCompletedOnDiskFallback(repoPath, storyID) {
					cleanOrRestoreFallbackSessionStoryFile(repoPath, sessionFile)
					_ = os.Remove(filepath.Join(repoPath, "tmp", "ase-resume-"+storyID+".json"))
				} else {
					if st.ActiveStoryID == "" {
						st.ActiveStoryID = storyID
					}
					nonMainBranch := st.Branch != "" && st.Branch != "main" && st.Branch != "master" && st.Branch != "HEAD"
					hasResumeFile := false
					if _, err := os.Stat(filepath.Join(repoPath, "tmp", "ase-resume-"+storyID+".json")); err == nil {
						hasResumeFile = true
					}
					if nonMainBranch || len(st.NonMainSubmodules) > 0 || hasResumeFile {
						if st.ASEState == "" || st.ASEState == "IDLE" {
							st.ASEState = "RUNNING"
						}
						if st.ExecutionProvenance == "" || st.ExecutionProvenance == "IDLE" {
							st.ExecutionProvenance = "INDEPENDENT_USER_SESSION"
						}
						if st.ToolLiveness == "" || st.ToolLiveness == "NONE" {
							st.ToolLiveness = "LIVE"
						}
						if st.ActiveTool == "" {
							st.ActiveTool = "autopilot-ase"
						}
					}
				}
			}
		}
		if st.ASEState == "" {
			st.ASEState = "IDLE"
		}
	}

	// metadata.yml snapshot sync (fallback if not populated by harness)
	if st.MetadataYmlRaw == "" {
		metaPath := filepath.Join(repoPath, "metadata.yml")
		if fi, err := os.Stat(metaPath); err == nil && !fi.IsDir() && fi.Size() < 2<<20 {
			if metaBytes, err := os.ReadFile(metaPath); err == nil {
				st.MetadataYmlRaw = string(metaBytes)
				hash := sha256.Sum256(metaBytes)
				st.MetadataYmlSHA = fmt.Sprintf("%x", hash)
				st.MetadataYmlModTime = fi.ModTime().UTC().Format(time.RFC3339)
			}
		}
	}

	// Git flow version autodetect (fallback if not populated by harness)
	if st.GitVersion == "" {
		gitVerCmd := procutil.CommandContext(ctx, "git", "-C", repoPath, "describe", "--tags", "--always")
		if out, err := gitVerCmd.Output(); err == nil {
			st.GitVersion = strings.TrimSpace(string(out))
		}
	}

	return st
}

func buildPreDispatchHook(eagerUpdater *supervisor.EagerToolUpdater, updatingTools *atomic.Bool, pendingSelfRestart *atomic.Bool) func(ctx context.Context, job *worker.Job) error {
	return func(ctx context.Context, job *worker.Job) error {
		if job != nil && job.Type == worker.JobTypeToolsUpdate {
			return nil
		}
		if eagerUpdater != nil && (updatingTools == nil || !updatingTools.Load()) {
			rep := eagerUpdater.EvaluateReadiness(ctx)
			if rep.EagerUpdateState != supervisor.StateUpToDate {
				res, _, err := eagerUpdater.EvaluateAndTriggerWithOpts(ctx, supervisor.TriggerOptions{
					BypassDirty: false,
					Force:       false,
				})
				if err == nil && res.AgentUpdated && pendingSelfRestart != nil {
					pendingSelfRestart.Store(true)
				}
			}
		}
		return nil
	}
}

func tryReserveOnJobWorktree(
	inFlightPrepMu *sync.Mutex,
	inFlightPrepJobs map[string]controlplane.ActiveJob,
	sup *supervisor.Sup,
	disp *worker.Dispatcher,
	jobID, worktreePath string,
) (string, bool) {
	inFlightPrepMu.Lock()
	defer inFlightPrepMu.Unlock()

	cleanWT := filepath.Clean(strings.TrimSpace(worktreePath))
	if cleanWT != "" && cleanWT != "." {
		for existingID, aj := range inFlightPrepJobs {
			if existingID != jobID && filepath.Clean(strings.TrimSpace(aj.WorktreePath)) == cleanWT {
				return existingID, true
			}
		}
		if sup != nil && sup.Lock(worktreePath) != "IDLE" {
			return sup.Lock(worktreePath), true
		}
		if disp != nil {
			for _, aj := range disp.ActiveJobs() {
				if aj.JobID != jobID && filepath.Clean(strings.TrimSpace(aj.WorktreePath)) == cleanWT {
					return aj.JobID, true
				}
			}
		}
	}

	inFlightPrepJobs[jobID] = controlplane.ActiveJob{
		JobID:        jobID,
		WorktreePath: worktreePath,
		PID:          os.Getpid(),
	}
	return "", false
}

func resolveRegisteredRepoWorktree(_ context.Context, getRepos func() ([]supervisor.RepoConfigItem, error), target string) (string, bool) {
	trimmed := strings.TrimSpace(target)
	if trimmed == "" || getRepos == nil {
		return "", false
	}
	repos, err := getRepos()
	if err != nil || len(repos) == 0 {
		return "", false
	}
	cleanTarget := filepath.Clean(filepath.FromSlash(trimmed))
	for _, r := range repos {
		cand := strings.TrimSpace(r.Path)
		if cand == "" {
			continue
		}
		cleanCand := filepath.Clean(filepath.FromSlash(cand))
		if cleanCand == cleanTarget || strings.EqualFold(r.Alias, trimmed) || filepath.Base(cleanCand) == trimmed {
			return cleanCand, true
		}
	}
	return "", false
}

func isVerifiedGitTopLevel(ctx context.Context, repoPath string) bool {
	cleanPath := filepath.Clean(strings.TrimSpace(repoPath))
	if cleanPath == "" || cleanPath == "." || cleanPath == string(filepath.Separator) || !filepath.IsAbs(cleanPath) {
		return false
	}
	if len(cleanPath) == 3 && cleanPath[1] == ':' && (cleanPath[2] == '\\' || cleanPath[2] == '/') {
		return false
	}
	out, err := procutil.CommandContext(ctx, "git", "-C", cleanPath, "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return false
	}
	topLevel := filepath.Clean(filepath.FromSlash(strings.TrimSpace(string(out))))
	if topLevel == cleanPath {
		return true
	}
	evalTop, err1 := filepath.EvalSymlinks(topLevel)
	evalRepo, err2 := filepath.EvalSymlinks(cleanPath)
	return err1 == nil && err2 == nil && filepath.Clean(evalTop) == filepath.Clean(evalRepo)
}

// healGitRepo delegates destructive cleanup for broken, diverged, or locked repo state to iazio-harness,
// falling back to local lock & git cleanup if iazio-harness is unavailable.
func healGitRepo(ctx context.Context, repoPath string) {
	cleanRepo := filepath.Clean(strings.TrimSpace(repoPath))
	if cleanRepo == "" || cleanRepo == "." || cleanRepo == string(filepath.Separator) || !filepath.IsAbs(cleanRepo) {
		return
	}

	// Always release local locks and stale ASE session artifacts
	_ = os.Remove(filepath.Join(cleanRepo, ".ase", "active.lock"))
	_ = os.Remove(filepath.Join(cleanRepo, ".autopilot", "lock"))
	_ = os.Remove(filepath.Join(cleanRepo, ".autopilot", "state.json"))
	_ = os.Remove(filepath.Join(cleanRepo, ".autopilot", "ase-state.json"))
	_ = os.Remove(filepath.Join(cleanRepo, ".session_story_id"))
	if matches, err := filepath.Glob(filepath.Join(cleanRepo, "tmp", "ase-resume-*.json")); err == nil {
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
	_ = repolock.ForceRelease(cleanRepo)

	gitDir := filepath.Join(cleanRepo, ".git")
	if out, err := procutil.CommandContext(ctx, "git", "-C", cleanRepo, "rev-parse", "--git-dir").Output(); err == nil {
		clean := strings.TrimSpace(string(out))
		if clean != "" {
			if filepath.IsAbs(clean) {
				gitDir = clean
			} else {
				gitDir = filepath.Join(cleanRepo, clean)
			}
		}
	}
	_ = os.Remove(filepath.Join(gitDir, "index.lock"))
	_ = os.Remove(filepath.Join(gitDir, "HEAD.lock"))
	_ = os.Remove(filepath.Join(gitDir, "config.lock"))

	// Delegate Tier 3 force-clean remediation to iazio-harness
	harnessBin := resolveHarnessBinaryPath()
	harnessCmd := procutil.CommandContext(ctx, harnessBin, "repo", "remediate", "--repo", cleanRepo, "--strategy", "force-clean", "--json")
	if err := harnessCmd.Run(); err == nil {
		return
	}

	// Fallback if iazio-harness binary is unavailable: verify cleanRepo is an actual git top-level worktree
	if !isVerifiedGitTopLevel(ctx, cleanRepo) {
		return
	}

	// Archive any dirty/untracked state before resetting so unmerged work is never blindly lost without backup.
	_, _ = workspace.ResetAndSanitizeFreshWorkspace(ctx, cleanRepo, "heal-git-repo", "")

	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "merge", "--abort").Run()
	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "rebase", "--abort").Run()
	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "cherry-pick", "--abort").Run()

	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "reset", "--hard", "HEAD").Run()
	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "clean", "-ffd").Run()

	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "submodule", "foreach", "--recursive", "git reset --hard HEAD || true").Run()
	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "submodule", "foreach", "--recursive", "git clean -ffd || true").Run()
	_ = procutil.CommandContext(ctx, "git", "-C", cleanRepo, "submodule", "update", "--init", "--recursive", "--force").Run()
}

func stripSlotSuffix(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	base = strings.TrimSuffix(base, "/")
	base = strings.TrimSuffix(base, "\\")
	idx := strings.LastIndex(base, "-")
	if idx > 0 && idx < len(base)-1 {
		suffix := base[idx+1:]
		isNum := true
		for _, c := range suffix {
			if c < '0' || c > '9' {
				isNum = false
				break
			}
		}
		if isNum {
			return base[:idx]
		}
	}
	return base
}

func isRepoSummaryIdle(r controlplane.RepoSummary) bool {
	if strings.EqualFold(strings.TrimSpace(r.Queue), "PAUSED") {
		return false
	}
	if strings.TrimSpace(r.RunningJobID) != "" {
		return false
	}
	lock := strings.ToUpper(strings.TrimSpace(r.Lock))
	return lock == "" || lock == "IDLE"
}

// gitmodulesHasSubmodule checks whether .gitmodules content declares a submodule
// matching repoName by exact submodule section name or path (avoiding substring false positives).
func gitmodulesHasSubmodule(content, repoName string) bool {
	clean := strings.TrimSpace(repoName)
	if clean == "" || clean == "." || clean == "root" {
		return false
	}
	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[submodule \"") && strings.HasSuffix(line, "\"]") {
			subName := strings.TrimSpace(line[len("[submodule \"") : len(line)-2])
			if subName == clean || filepath.Base(subName) == clean {
				return true
			}
			continue
		}
		if k, v, ok := strings.Cut(line, "="); ok {
			key := strings.ToLower(strings.TrimSpace(k))
			val := strings.Trim(strings.TrimSpace(v), "\"")
			if key == "path" {
				if val == clean || filepath.Base(val) == clean {
					return true
				}
			}
		}
	}
	return false
}

// resolveWorktreeFromHostDetail resolves a target worktree from the host's registered repos.
func resolveWorktreeFromHostDetail(detail controlplane.HostDetail, repoName string) string {
	clean := strings.TrimSpace(repoName)
	// 1. Direct match by exact worktree path, repo alias, or basename (preferring IDLE if multiple match)
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" {
			continue
		}
		if wt == clean {
			return wt
		}
		if (r.Alias == clean || r.RepoAlias == clean || filepath.Base(wt) == clean) && isRepoSummaryIdle(r) {
			return wt
		}
	}
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
	// 2. Submodule match: prefer IDLE primary worktree first, then any IDLE worktree, then primary worktree
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" || !isRepoSummaryIdle(r) {
			continue
		}
		base := filepath.Base(wt)
		if strings.Contains(base, "-0") || strings.Contains(base, "-1") || strings.Contains(base, "-2") {
			continue
		}
		gmPath := filepath.Join(wt, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if gitmodulesHasSubmodule(string(b), clean) {
				return wt
			}
		}
	}
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" || !isRepoSummaryIdle(r) {
			continue
		}
		gmPath := filepath.Join(wt, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if gitmodulesHasSubmodule(string(b), clean) {
				return wt
			}
		}
	}
	for _, r := range detail.Repos {
		wt := r.WorktreePath
		if wt == "" {
			wt = r.Path
		}
		if wt == "" {
			continue
		}
		base := filepath.Base(wt)
		if stripSlotSuffix(base) != base {
			continue
		}
		gmPath := filepath.Join(wt, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if gitmodulesHasSubmodule(string(b), clean) {
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
			if gitmodulesHasSubmodule(string(b), clean) {
				return wt
			}
		}
	}
	// 4. Slot family fallback: if clean contains a slot suffix or matches umbrella/meta-repo,
	// match any registered IDLE worktree on this host with the same root base name.
	cleanBase := stripSlotSuffix(clean)
	if cleanBase != "" && cleanBase != "." && cleanBase != "root" {
		for _, r := range detail.Repos {
			wt := r.WorktreePath
			if wt == "" {
				wt = r.Path
			}
			if wt != "" && stripSlotSuffix(wt) == cleanBase && r.GlobalQueueAllocationEnabled && isRepoSummaryIdle(r) {
				return wt
			}
		}
		for _, r := range detail.Repos {
			wt := r.WorktreePath
			if wt == "" {
				wt = r.Path
			}
			if wt != "" && stripSlotSuffix(wt) == cleanBase && isRepoSummaryIdle(r) {
				return wt
			}
		}
		for _, r := range detail.Repos {
			wt := r.WorktreePath
			if wt == "" {
				wt = r.Path
			}
			if wt != "" && stripSlotSuffix(wt) == cleanBase && r.GlobalQueueAllocationEnabled {
				return wt
			}
		}
		for _, r := range detail.Repos {
			wt := r.WorktreePath
			if wt == "" {
				wt = r.Path
			}
			if wt != "" && stripSlotSuffix(wt) == cleanBase {
				return wt
			}
		}
	}
	if clean == "." || clean == "" || clean == "root" {
		for _, r := range detail.Repos {
			if r.GlobalQueueAllocationEnabled && isRepoSummaryIdle(r) {
				wt := r.WorktreePath
				if wt == "" {
					wt = r.Path
				}
				if wt != "" {
					return wt
				}
			}
		}
		for _, r := range detail.Repos {
			if isRepoSummaryIdle(r) {
				wt := r.WorktreePath
				if wt == "" {
					wt = r.Path
				}
				if wt != "" {
					return wt
				}
			}
		}
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

var (
	cachedFleetRootMu sync.RWMutex
	cachedFleetRoot   string
)

func getCachedFleetRoot() string {
	cachedFleetRootMu.RLock()
	defer cachedFleetRootMu.RUnlock()
	return cachedFleetRoot
}

func setCachedFleetRoot(root string) {
	cachedFleetRootMu.Lock()
	cachedFleetRoot = root
	cachedFleetRootMu.Unlock()
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
	matchesCandidate := func(cand string) bool {
		gmPath := filepath.Join(cand, ".gitmodules")
		if b, err := os.ReadFile(gmPath); err == nil {
			if clean == "." || clean == "" || clean == "root" || gitmodulesHasSubmodule(string(b), clean) {
				return true
			}
			if clean != "" {
				if _, err := os.Stat(filepath.Join(cand, clean)); err == nil {
					return true
				}
			}
		}
		if strings.Contains(clean, "meta-repo") && strings.Contains(filepath.Base(cand), "meta-repo") {
			if _, err := os.Stat(filepath.Join(cand, ".gitmodules")); err == nil {
				return true
			}
		}
		return false
	}
	if cached := getCachedFleetRoot(); cached != "" {
		if matchesCandidate(cached) {
			return cached
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
			if clean == "." || clean == "" || clean == "root" || gitmodulesHasSubmodule(string(b), clean) {
				setCachedFleetRoot(cand)
				return cand
			}
			if clean != "" {
				if _, err := os.Stat(filepath.Join(cand, clean)); err == nil {
					setCachedFleetRoot(cand)
					return cand
				}
			}
		}
	}
	if strings.Contains(clean, "meta-repo") {
		for _, cand := range candidateRoots {
			if strings.Contains(filepath.Base(cand), "meta-repo") {
				if _, err := os.Stat(filepath.Join(cand, ".gitmodules")); err == nil {
					setCachedFleetRoot(cand)
					return cand
				}
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

func buildInspectOutdatedHook(
	updateSuite bool,
	driftFetcher supervisor.DriftFetcher,
	getInstallRoots func(*supervisor.FleetManifest) string,
	targetOS string,
) func(context.Context) ([]supervisor.OutdatedToolInfo, error) {
	return func(ctx context.Context) ([]supervisor.OutdatedToolInfo, error) {
		if driftFetcher == nil {
			return nil, nil
		}
		manifest, err := driftFetcher.FetchManifest(ctx)
		if err != nil {
			return nil, err
		}
		var binDir string
		if getInstallRoots != nil {
			binDir = getInstallRoots(manifest)
		}
		var checkTools []string
		if !updateSuite {
			checkTools = []string{"iazio-agent"}
		}
		return supervisor.FindOutdatedTools(manifest, inventory.InspectInstalledVersion, binDir, checkTools, targetOS)
	}
}

func isLoopbackTelemetryURL(rawURL string) bool {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return false
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return true
	}
	return false
}

func resolveTelemetryDestination(apiURL string, getenv func(string) string, ctx ...context.Context) (string, bool) {
	if getenv == nil {
		getenv = os.Getenv
	}
	for _, envKey := range []string{"CENTRAL_TELEMETRY_GATEWAY_URL", "IAZIO_CENTRAL_TELEMETRY_GATEWAY_URL", "TELEMETRY_GATEWAY_URL"} {
		if u := strings.TrimSpace(getenv(envKey)); u != "" {
			return u, true
		}
	}

	if trimmedAPI := strings.TrimRight(strings.TrimSpace(apiURL), "/"); trimmedAPI != "" {
		baseCtx := context.Background()
		if len(ctx) > 0 && ctx[0] != nil {
			baseCtx = ctx[0]
		}
		client := &http.Client{Timeout: 2 * time.Second}
		token := strings.TrimSpace(resolveControlPlaneToken(getenv))
		for _, path := range []string{"/v1/telemetry/config", "/api/v1/telemetry/config", "/v1/status"} {
			reqCtx, cancel := context.WithTimeout(baseCtx, 2*time.Second)
			req, reqErr := http.NewRequestWithContext(reqCtx, http.MethodGet, trimmedAPI+path, nil)
			if reqErr != nil {
				cancel()
				continue
			}
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			resp, err := client.Do(req)
			if err == nil {
				var gwURL string
				if resp.StatusCode == http.StatusOK {
					var raw struct {
						CentralTelemetryGatewayURL string `json:"central_telemetry_gateway_url"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&raw); err == nil && strings.TrimSpace(raw.CentralTelemetryGatewayURL) != "" {
						gwURL = strings.TrimSpace(raw.CentralTelemetryGatewayURL)
					}
				}
				_ = resp.Body.Close()
				cancel()
				if gwURL != "" {
					return gwURL, true
				}
			} else {
				cancel()
			}
		}
	}

	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cfgFile := filepath.Join(home, ".iazio", "agent.json")
		if data, err := os.ReadFile(cfgFile); err == nil {
			var raw struct {
				CentralTelemetryGatewayURL string `json:"central_telemetry_gateway_url"`
				ElasticsearchURL           string `json:"elasticsearch_url"`
			}
			if err := json.Unmarshal(data, &raw); err == nil {
				if strings.TrimSpace(raw.CentralTelemetryGatewayURL) != "" {
					return strings.TrimSpace(raw.CentralTelemetryGatewayURL), true
				}
				if strings.TrimSpace(raw.ElasticsearchURL) != "" {
					return strings.TrimSpace(raw.ElasticsearchURL), false
				}
			}
		}
	}

	if esURL := strings.TrimSpace(getenv("IAZIO_ELASTICSEARCH_URL")); esURL != "" {
		return esURL, false
	}

	return "", false
}

const reasonRepoBusy = "REPO_BUSY"

func makePrePollBarrier(hostGate *supervisor.HostExecutionAndUpdateGate, eagerUpdater *supervisor.EagerToolUpdater) func(context.Context) bool {
	return func(ctx context.Context) bool {
		if hostGate != nil && hostGate.State() == supervisor.GateStateUpdatingTools {
			return false
		}
		if eagerUpdater != nil {
			_, ok := eagerUpdater.EnsureUpToDateBeforeClaim(ctx)
			return ok
		}
		return true
	}
}

func wireDispatcherReadinessAndIdleWorktrees(disp *worker.Dispatcher, client *controlplane.Client, eagerUpdater *supervisor.EagerToolUpdater, getRepos func() ([]supervisor.RepoConfigItem, error)) {
	if eagerUpdater != nil {
		disp.SetEagerReadinessGate(func(c context.Context) (bool, string) {
			rep, ok := eagerUpdater.EnsureUpToDateBeforeClaim(c)
			return ok, rep.Details
		})
	}
	client.IdleWorktrees = func() []string {
		repos, err := getRepos()
		if err != nil || len(repos) == 0 {
			return nil
		}
		candidates := make([]string, 0, len(repos))
		for _, r := range repos {
			if strings.TrimSpace(r.Path) != "" {
				candidates = append(candidates, r.Path)
			}
		}
		return disp.CollectIdleWorktrees(candidates)
	}
}
