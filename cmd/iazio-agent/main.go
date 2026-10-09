// Command iazio-agent is the user-level runner supervisor.
package main

import (
	"context"
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
	"syscall"
	"time"

	"github.com/viovy/iazio-agent/internal/auth"
	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/inventory"
	"github.com/viovy/iazio-agent/internal/preflight"
	"github.com/viovy/iazio-agent/internal/service"
	"github.com/viovy/iazio-agent/internal/supervisor"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	if getenv == nil {
		getenv = os.Getenv
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: iazio-agent version|auth|service|update|run")
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, formatVersion())
		return nil
	case "auth":
		return runAuth(ctx, args[1:], stdout, getenv)
	case "service":
		return runService(args[1:], stdout, getenv)
	case "update":
		return runUpdate(ctx, args[1:], stdout, stderr, getenv)
	case "run":
		kind := supervisor.HostKind(getenv("IAZIO_AGENT_HOST_KIND"))
		fmt.Fprintln(stdout, kind)
		api := resolveLocalAPI(getenv)
		profile := getenv("IAZIO_AGENT_PROFILE")
		updateInterval := 6 * time.Hour
		if raw := getenv("IAZIO_AGENT_UPDATE_INTERVAL"); raw != "" {
			if d, err := time.ParseDuration(raw); err == nil {
				updateInterval = d
			} else if raw == "0" || strings.ToLower(raw) == "disabled" {
				updateInterval = 0
			}
		}
		updateSuite := getenv("IAZIO_AGENT_UPDATE_SUITE") == "1" || strings.ToLower(getenv("IAZIO_AGENT_UPDATE_SUITE")) == "true"

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
		if ctx.Err() == nil {
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			sup := supervisor.New(time.Now)
			runner := supervisor.NewOSRunner()
			var client controlplane.Client

			if updateInterval > 0 {
				go func() {
					select {
					case <-runCtx.Done():
						return
					case <-time.After(5 * time.Second):
					}

					checkOnce := func() bool {
						if api != "" {
							detail, err := client.GetHost(runCtx, host)
							if err == nil {
								for _, r := range detail.Repos {
									path := r.WorktreePath
									if path == "" {
										path = r.Path
									}
									if path == "" {
										continue
									}
									if sup.Lock(path) == supervisor.LockRunning {
										if stdout != nil {
											fmt.Fprintf(stdout, "[supervisor] worktree %s is running a job; deferring background update\n", path)
										}
										return false
									}
									out, err := exec.CommandContext(runCtx, "git", "-C", path, "status", "--porcelain").CombinedOutput()
									if err == nil && strings.TrimSpace(string(out)) != "" {
										if stdout != nil {
											fmt.Fprintf(stdout, "[supervisor] worktree %s is dirty; deferring background update\n", path)
										}
										return false
									}
								}
							}
						}

						updated, err := performUpdate(runCtx, UpdateOptions{
							Suite:   updateSuite,
							Profile: profile,
							API:     api,
						}, stdout, stderr, getenv)
						if err != nil {
							if stderr != nil {
								fmt.Fprintf(stderr, "[supervisor] update check error: %v\n", err)
							}
							return false
						}
						return updated
					}

					if checkOnce() {
						if stdout != nil {
							fmt.Fprintln(stdout, "[supervisor] iazio-agent self-updated; restarting service...")
						}
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
								if stdout != nil {
									fmt.Fprintln(stdout, "[supervisor] iazio-agent self-updated; restarting service...")
								}
								cancel()
								return
							}
						}
					}
				}()
			}
			client = controlplane.Client{
				BaseURL: api,
				Tools: func() []controlplane.Tool {
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
				},
				ActiveJobs: func() []controlplane.ActiveJob {
					var jobs []controlplane.ActiveJob
					for _, rj := range sup.RunningJobs() {
						jobs = append(jobs, controlplane.ActiveJob{
							JobID:        rj.JobID,
							WorktreePath: rj.WorktreePath,
							PID:          rj.PID,
						})
					}
					return jobs
				},
				OnTick: func(ctx context.Context, hostID string) error {
					reaped := sup.Tick()
					for _, r := range reaped {
						if stderr != nil {
							fmt.Fprintf(stderr, "[supervisor] reaped stalled or terminated harness job %q on %s (reason: %s)\n",
								r.JobID, r.WorktreePath, r.Reason)
						}
						if r.JobID != "" {
							_ = client.AbandonJob(ctx, r.JobID, r.Reason)
						}
					}
					detail, err := client.GetHost(ctx, hostID)
					if err != nil {
						if stderr != nil {
							fmt.Fprintf(stderr, "reconcile get host error for %s: %v\n", hostID, err)
						}
						return nil
					}
					repoStates := make([]supervisor.RepoState, 0, len(detail.Repos))
					for _, r := range detail.Repos {
						path := r.WorktreePath
						if path == "" {
							path = r.Path
						}
						repoStates = append(repoStates, supervisor.RepoState{
							WorktreePath:   path,
							Queue:          r.Queue,
							Reason:         r.Reason,
							DiscardPending: r.DiscardPending,
							Lock:           r.Lock,
							RunningJobID:   r.RunningJobID,
						})
					}
					supervisor.ReconcileStrandedCheckouts(
						ctx,
						hostID,
						repoStates,
						func(w string) bool { return sup.Lock(w) == supervisor.LockRunning },
						func(ctx context.Context, jobID, reason string) error {
							return client.AbandonJob(ctx, jobID, reason)
						},
						func(ctx context.Context, hostID, worktree string) error {
							return client.RemediateRepo(ctx, hostID, worktree)
						},
						stdout,
						stderr,
					)
					supervisor.ReconcileHostCheckouts(
						repoStates,
						func(w string) bool { return sup.Lock(w) == supervisor.LockIdle },
						func(w string) error {
							gitCmd := func(args ...string) error {
								cmd := exec.CommandContext(ctx, "git", append([]string{"-C", w}, args...)...)
								return cmd.Run()
							}
							return supervisor.DiscardCheckout(true, false, nil, gitCmd)
						},
						func(w string) (bool, error) {
							out, err := exec.CommandContext(ctx, "git", "-C", w, "status", "--porcelain").CombinedOutput()
							if err != nil {
								return false, err
							}
							return strings.TrimSpace(string(out)) == "", nil
						},
						func(w string) error {
							return client.ResumeRepo(ctx, hostID, w)
						},
						stdout,
						stderr,
						func(w string) (bool, error) {
							bytes, err := preflight.FreeSpace(w)
							if err != nil {
								return false, err
							}
							return bytes >= preflight.RequiredMinFreeBytes(), nil
						},
					)
					return nil
				},
			}
			return client.Loop(runCtx, host, kind, 30*time.Second, func(job controlplane.Assignment) error {
				if sup.Lock(job.WorktreePath) == supervisor.LockRunning {
					fmt.Fprintf(os.Stderr, "worktree %s busy with another job; declining assignment %s\n", job.WorktreePath, job.ID)
					_ = client.DeclineJob(ctx, job.ID, "worktree_busy")
					return nil
				}
				rep, err := preflight.Collect(ctx, job.WorktreePath, job.DocsHubPath, job.Kind)
				if err != nil {
					fmt.Fprintf(os.Stderr, "preflight collect error for %s: %v\n", job.WorktreePath, err)
					_ = client.DeclineJob(ctx, job.ID, fmt.Sprintf("preflight_collect_error: %v", err))
					return nil
				}
				dec := preflight.Decide(rep)
				if dec.Reason != "" {
					if _, postErr := client.PostPreflight(ctx, host, job.WorktreePath, rep); postErr != nil {
						fmt.Fprintf(os.Stderr, "post preflight error for %s: %v\n", job.WorktreePath, postErr)
					}
					_ = client.DeclineJob(ctx, job.ID, fmt.Sprintf("preflight_rejected: %s", dec.Reason))
					return nil
				}
				if err := sup.Spawn(runner, job.ID, api, job.WorktreePath, job.DocsHubPath, ""); err != nil {
					fmt.Fprintf(os.Stderr, "spawn error for job %s: %v\n", job.ID, err)
					_ = client.DeclineJob(ctx, job.ID, fmt.Sprintf("spawn_error: %v", err))
					return nil
				}
				go func(worktree, docsHub, jobID, kind string) {
					pid := sup.PID(worktree)
					exitCode, waitErr := runner.Wait(pid)
					sup.BeginCooling(worktree)
					time.Sleep(supervisor.CoolingOff)
					sup.Promote()

					workPorc := ""
					hubPorc := ""
					if out, err := exec.Command("git", "-C", worktree, "status", "--porcelain").CombinedOutput(); err == nil {
						workPorc = strings.TrimSpace(string(out))
					}
					if docsHub != "" {
						if out, err := exec.Command("git", "-C", docsHub, "status", "--porcelain").CombinedOutput(); err == nil {
							hubPorc = strings.TrimSpace(string(out))
						}
					}
					ok := (waitErr == nil && exitCode == 0)
					finish := controlplane.FinishReport{
						Kind:          kind,
						ASEComplete:   ok,
						WorkPorcelain: workPorc,
						HubPorcelain:  hubPorc,
						StoryDraftOK:  ok,
						HubPushOK:     ok,
					}
					_, _ = client.PostFinish(context.Background(), host, worktree, jobID, finish)
				}(job.WorktreePath, job.DocsHubPath, job.ID, job.Kind)
				return nil
			}, profile)
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
	if !dry && action == "install" {
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
	switch goos {
	case "darwin":
		return applyDarwin(action, bin)
	case "windows":
		if action == "install" {
			return service.InstallWindows(bin)
		}
		_, err = service.Apply(goos, action, bin, false, force, execCommand)
		return err
	default:
		if action == "install" {
			return service.InstallLinux(bin)
		}
		_, err = service.Apply(goos, action, bin, false, force, execCommand)
		return err
	}
}

func applyDarwin(action, bin string) error {
	switch action {
	case "install", "restart":
		_ = bootoutDarwin(false)
		return service.InstallDarwin(bin)
	case "start":
		return kickstartDarwin()
	case "stop", "uninstall":
		return bootoutDarwin(action == "uninstall")
	case "status":
		out, err := exec.Command("launchctl", "print", darwinTarget()).CombinedOutput()
		if err != nil {
			return fmt.Errorf("service not loaded: %s", strings.TrimSpace(string(out)))
		}
		if strings.Contains(string(out), "pid =") && !strings.Contains(string(out), "job state = exited") {
			fmt.Println("running")
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
	return execCommand("launchctl", "kickstart", "-k", "-p", darwinTarget())
}

func bootoutDarwin(remove bool) error {
	_ = execCommand("launchctl", "bootout", darwinTarget())
	if !remove {
		return nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	return os.Remove(filepath.Join(home, "Library", "LaunchAgents", "io.iazio.iazio-agent.plist"))
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
	dest := filepath.Join(home, ".iazio", "bin", "iazio-agent")
	if err := copyBinary(src, dest); err != nil {
		return "", err
	}
	_ = copyBinary(src, filepath.Join(home, ".local", "bin", "iazio-agent"))
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

func execCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", name, strings.TrimSpace(string(out)))
	}
	return nil
}

func parseService(args []string) (action string, dry, force bool, err error) {
	if len(args) == 0 {
		return "", false, false, fmt.Errorf("service install|uninstall|start|stop|restart|status")
	}
	action = args[0]
	switch action {
	case "install", "uninstall", "start", "stop", "restart", "status":
	default:
		return "", false, false, fmt.Errorf("unknown service action %s", action)
	}
	for _, arg := range args[1:] {
		switch arg {
		case "--dry-run":
			dry = true
		case "--force":
			force = true
		default:
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
