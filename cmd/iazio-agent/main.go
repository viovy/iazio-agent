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
	"sync/atomic"
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
		return fmt.Errorf("usage: iazio-agent version|auth|service|update|run|probe")
	}
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
		if ctx.Err() == nil {
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			sup := supervisor.New(time.Now)
			runner := supervisor.NewOSRunner()
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

			doToolUpdate := func(ctx context.Context, targetTools []string, force bool) bool {
				if !areAllReposCleanAndHarnessIdle(ctx, client, sup, host, api, stdout) {
					return false
				}
				updatingTools.Store(true)
				if api != "" {
					_ = client.Heartbeat(ctx, host, getTools(), false)
				}
				defer func() {
					updatingTools.Store(false)
					if api != "" {
						_ = client.Heartbeat(ctx, host, getTools(), false)
					}
				}()

				opts := UpdateOptions{
					Suite:       updateSuite,
					Profile:     profile,
					API:         api,
					Force:       force,
					TargetTools: targetTools,
				}
				updated, err := performUpdate(ctx, opts, stdout, stderr, getenv)
				if err != nil {
					if stderr != nil {
						fmt.Fprintf(stderr, "[supervisor] update check error: %v\n", err)
					}
					return false
				}
				return updated
			}

			if updateInterval > 0 {
				go func() {
					select {
					case <-runCtx.Done():
						return
					case <-time.After(5 * time.Second):
					}

					checkOnce := func() bool {
						return doToolUpdate(runCtx, nil, false)
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
					go func() {
						if doToolUpdate(runCtx, tools, force) {
							if stdout != nil {
								fmt.Fprintln(stdout, "[supervisor] iazio-agent self-updated; restarting service...")
							}
							cancel()
						}
					}()
					return nil
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
						func(w string) bool {
							l := sup.Lock(w)
							return l == supervisor.LockRunning || l == supervisor.LockCooling
						},
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
					time.Sleep(2 * time.Second)
					return nil
				}
				rep, err := preflight.Collect(ctx, job.WorktreePath, job.DocsHubPath, job.Kind)
				if err != nil {
					fmt.Fprintf(os.Stderr, "preflight collect error for %s: %v\n", job.WorktreePath, err)
					_ = client.DeclineJob(ctx, job.ID, fmt.Sprintf("preflight_collect_error: %v", err))
					time.Sleep(2 * time.Second)
					return nil
				}
				dec := preflight.Decide(rep)
				if dec.Reason != "" {
					if _, postErr := client.PostPreflight(ctx, host, job.WorktreePath, rep); postErr != nil {
						fmt.Fprintf(os.Stderr, "post preflight error for %s: %v\n", job.WorktreePath, postErr)
					}
					_ = client.DeclineJob(ctx, job.ID, fmt.Sprintf("preflight_rejected: %s", dec.Reason))
					time.Sleep(2 * time.Second)
					return nil
				}
				if err := sup.Spawn(runner, job.ID, api, job.WorktreePath, job.DocsHubPath, ""); err != nil {
					fmt.Fprintf(os.Stderr, "spawn error for job %s: %v\n", job.ID, err)
					_ = client.DeclineJob(ctx, job.ID, fmt.Sprintf("spawn_error: %v", err))
					time.Sleep(2 * time.Second)
					return nil
				}
				jobPID := sup.PID(job.WorktreePath)
				if stdout != nil {
					fmt.Fprintf(stdout, "[supervisor] spawned harness job %s (PID %d) on %s\n", job.ID, jobPID, job.WorktreePath)
				}
				go func(worktree, docsHub, jobID, kind, storyID string, pid int) {
					exitCode, waitErr := runner.Wait(pid)
					if stdout != nil {
						fmt.Fprintf(stdout, "[supervisor] harness job %s (PID %d) on %s exited with code %d (waitErr: %v)\n", jobID, pid, worktree, exitCode, waitErr)
					}
					sup.BeginCooling(worktree)
					time.Sleep(supervisor.CoolingOff)

					workPorc := ""
					hubPorc := ""
					if out, err := exec.Command("git", "-C", worktree, "status", "--porcelain").CombinedOutput(); err == nil {
						workPorc = strings.TrimSpace(preflight.CleanPorcelain(string(out)))
					}
					if docsHub != "" {
						if out, err := exec.Command("git", "-C", docsHub, "status", "--porcelain").CombinedOutput(); err == nil {
							hubPorc = strings.TrimSpace(preflight.CleanPorcelain(string(out)))
						}
					}
					isClean := (workPorc == "")
					aseOk := (waitErr == nil && exitCode == 0 && isClean)

					// Check for auto-healable SIR immediately post-run
					if !isClean {
						healable, sirID, sirBranch, _ := supervisor.DetectAutoHealableSIR(worktree)
						if healable {
							gitCmd := func(args ...string) error {
								return exec.Command("git", append([]string{"-C", worktree}, args...)...).Run()
							}
							if err := supervisor.AutoHealSupersededWorktree(true, sirBranch, gitCmd); err == nil {
								_ = supervisor.ArchiveSIR(worktree, sirID)
								if out, err := exec.Command("git", "-C", worktree, "status", "--porcelain").CombinedOutput(); err == nil {
									workPorc = strings.TrimSpace(preflight.CleanPorcelain(string(out)))
									if workPorc == "" {
										isClean = true
										aseOk = (waitErr == nil && exitCode == 0)
									}
								}
							}
						}
					}

					// Check harness execution checkpoint if present
					checkpointPath := filepath.Join(worktree, ".iazio", "execution-checkpoint.json")
					if cpData, err := os.ReadFile(checkpointPath); err == nil {
						var cp struct {
							ASEComplete bool `json:"ase_complete"`
							ExitCode    int  `json:"exit_code"`
						}
						if json.Unmarshal(cpData, &cp) == nil {
							if cp.ASEComplete && isClean {
								aseOk = true
							}
						}
					}

					storyDone := false
					if storyID != "" {
						matches, _ := filepath.Glob(filepath.Join(worktree, "docs", "stories", "*.story.md"))
						for _, m := range matches {
							if data, err := os.ReadFile(m); err == nil {
								content := string(data)
								if strings.Contains(content, storyID) && (strings.Contains(content, "status: DONE") || strings.Contains(content, "status: \"DONE\"") || strings.Contains(content, "status: 'DONE'")) {
									storyDone = true
									break
								}
							}
						}
					}

					// If not yet considered complete but worktree is clean, check if story completed on origin/main
					if !aseOk && isClean {
						branchOut, _ := exec.Command("git", "-C", worktree, "rev-parse", "--abbrev-ref", "HEAD").CombinedOutput()
						curBranch := strings.TrimSpace(string(branchOut))
						branchMerged := false

						if curBranch != "" && curBranch != "main" && curBranch != "master" {
							if exec.Command("git", "-C", worktree, "merge-base", "--is-ancestor", "HEAD", "origin/main").Run() == nil {
								branchMerged = true
							} else if storyID != "" {
								if out, err := exec.Command("git", "-C", worktree, "log", "origin/main", "-n", "1", "--grep="+storyID, "--format=%H").CombinedOutput(); err == nil && strings.TrimSpace(string(out)) != "" {
									branchMerged = true
								}
							}
							if !branchMerged {
								if out, err := exec.Command("git", "-C", worktree, "log", "origin/main", "-n", "1", "--grep="+curBranch, "--format=%H").CombinedOutput(); err == nil && strings.TrimSpace(string(out)) != "" {
									branchMerged = true
								}
							}

							if branchMerged {
								// Auto-heal to main
								if exec.Command("git", "-C", worktree, "checkout", "main").Run() == nil {
									_ = exec.Command("git", "-C", worktree, "pull", "--ff-only").Run()
									_ = exec.Command("git", "-C", worktree, "branch", "-D", curBranch).Run()
									aseOk = true
									if stdout != nil {
										fmt.Fprintf(stdout, "[supervisor] auto-healed merged branch %s to main on %s for job %s\n", curBranch, worktree, jobID)
									}
								} else {
									aseOk = true
								}
							}
						} else if curBranch == "main" || curBranch == "master" {
							// On main: only consider complete if story is confirmed DONE or merged on origin/main
							if storyDone {
								aseOk = true
							} else if storyID != "" {
								if out, err := exec.Command("git", "-C", worktree, "log", "origin/main", "-n", "1", "--grep="+storyID, "--format=%H").CombinedOutput(); err == nil && strings.TrimSpace(string(out)) != "" {
									if logMsg, err := exec.Command("git", "-C", worktree, "log", "origin/main", "-n", "5", "--grep="+storyID, "--format=%s %b").CombinedOutput(); err == nil {
										lower := strings.ToLower(string(logMsg))
										if strings.Contains(lower, "closeout") || strings.Contains(lower, "done") {
											aseOk = true
										}
									}
								}
							}
						}
					}

					finish := controlplane.FinishReport{
						Kind:          kind,
						ASEComplete:   aseOk,
						WorkPorcelain: workPorc,
						HubPorcelain:  hubPorc,
						StoryDraftOK:  (waitErr == nil && exitCode == 0) || aseOk,
						HubPushOK:     (waitErr == nil && exitCode == 0) || aseOk,
					}
					_, _ = client.PostFinish(context.Background(), host, worktree, jobID, finish)
					sup.Promote()
				}(job.WorktreePath, job.DocsHubPath, job.ID, job.Kind, job.StoryID, jobPID)
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
		return applyWindows(action, bin, stdout)
	default:
		return applyLinux(action, bin, stdout, force)
	}
}

func applyLinux(action, bin string, stdout io.Writer, force bool) error {
	if !service.IsSystemdAvailable() {
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

// parseSchtasksStatus extracts the Status field from `schtasks /Query /FO LIST` output.
// ok is false when the Status: line is absent so callers fail closed instead of
// treating unparseable output as a successful status probe.
func parseSchtasksStatus(out string) (running bool, status string, ok bool) {
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if strings.HasPrefix(trimmed, "Status:") {
			statusVal := strings.TrimSpace(strings.TrimPrefix(trimmed, "Status:"))
			return strings.EqualFold(statusVal, "Running"), statusVal, true
		}
	}
	return false, "", false
}

func applyWindows(action, _ string, stdout io.Writer) error {
	switch action {
	case "status":
		cmd := exec.Command("schtasks", "/Query", "/TN", "iazio-agent", "/FO", "LIST")
		out, err := cmd.CombinedOutput()
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

type repoAuditSubmodule struct {
	Name           string `json:"name"`
	Path           string `json:"path"`
	IsClean        bool   `json:"is_clean"`
	ModifiedCount  int    `json:"modified_count"`
	UntrackedCount int    `json:"untracked_count"`
}

type repoAuditResult struct {
	Total      int                  `json:"total"`
	Clean      int                  `json:"clean"`
	Dirty      int                  `json:"dirty"`
	Submodules []repoAuditSubmodule `json:"submodules"`
}

func scanRepoFleetStatus(ctx context.Context, repoPath string) bool {
	// First query autopilot-fleet audit -json
	cmd := exec.CommandContext(ctx, "autopilot-fleet", "audit", "-json")
	cmd.Dir = repoPath
	if out, err := cmd.Output(); err == nil && len(out) > 0 {
		var audit repoAuditResult
		if json.Unmarshal(out, &audit) == nil {
			if audit.Dirty > 0 {
				return false
			}
			for _, sub := range audit.Submodules {
				if !sub.IsClean || sub.ModifiedCount > 0 || sub.UntrackedCount > 0 {
					return false
				}
			}
		}
	}
	// Fallback/direct check with git status --porcelain
	gitCmd := exec.CommandContext(ctx, "git", "-C", repoPath, "status", "--porcelain")
	if out, err := gitCmd.CombinedOutput(); err == nil {
		if strings.TrimSpace(string(out)) != "" {
			return false
		}
	}
	return true
}

func areAllReposCleanAndHarnessIdle(ctx context.Context, client controlplane.Client, sup *supervisor.Sup, host, api string, stdout io.Writer) bool {
	if sup != nil && sup.HasRunningHarness() {
		if stdout != nil {
			fmt.Fprintln(stdout, "[supervisor] harness execution is active; deferring background tool update")
		}
		return false
	}
	if api != "" {
		detail, err := client.GetHost(ctx, host)
		if err == nil {
			for _, r := range detail.Repos {
				path := r.WorktreePath
				if path == "" {
					path = r.Path
				}
				if path == "" {
					continue
				}
				if sup != nil {
					lock := sup.Lock(path)
					if lock == supervisor.LockRunning || lock == supervisor.LockCooling || lock == supervisor.LockPending {
						if stdout != nil {
							fmt.Fprintf(stdout, "[supervisor] worktree %s is busy (%s); deferring background tool update\n", path, lock)
						}
						return false
					}
				}
				if !scanRepoFleetStatus(ctx, path) {
					if stdout != nil {
						fmt.Fprintf(stdout, "[supervisor] worktree %s is dirty; deferring background tool update\n", path)
					}
					return false
				}
			}
		}
	}
	return true
}
