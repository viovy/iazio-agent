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
		return runUpdate(args[1:], stdout)
	case "run":
		kind := supervisor.HostKind(getenv("IAZIO_AGENT_HOST_KIND"))
		fmt.Fprintln(stdout, kind)
		api := getenv("IAZIO_HARNESS_API_URL")
		if api == "" {
			api = "http://localhost:8090"
		}
		host := getenv("IAZIO_AGENT_HOST_ID")
		if host == "" {
			if h, err := os.Hostname(); err == nil && h != "" {
				host = strings.ToLower(strings.Split(h, ".")[0])
			} else {
				host = "runner"
			}
		}
		if ctx.Err() == nil {
			sup := supervisor.New(time.Now)
			runner := supervisor.NewOSRunner()
			client := controlplane.Client{
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
			}
			return client.Loop(ctx, host, kind, 30*time.Second, func(job controlplane.Assignment) error {
				if sup.Lock(job.WorktreePath) == supervisor.LockRunning {
					return fmt.Errorf("worktree %s busy", job.WorktreePath)
				}
				rep, err := preflight.Collect(ctx, job.WorktreePath, job.DocsHubPath, job.Kind)
				if err != nil {
					return err
				}
				dec := preflight.Decide(rep)
				if dec.Reason != "" {
					_, postErr := client.PostPreflight(ctx, host, job.WorktreePath, rep)
					return postErr
				}
				if err := sup.Spawn(runner, job.ID, api, job.WorktreePath, job.DocsHubPath, ""); err != nil {
					return err
				}
				go func(worktree, docsHub, jobID, kind string) {
					pid := sup.PID(worktree)
					exitCode, _ := runner.Wait(pid)
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
					finish := controlplane.FinishReport{
						Kind:          kind,
						ASEComplete:   (exitCode == 0),
						WorkPorcelain: workPorc,
						HubPorcelain:  hubPorc,
						StoryDraftOK:  (exitCode == 0),
						HubPushOK:     (exitCode == 0),
					}
					_, _ = client.PostFinish(context.Background(), host, worktree, jobID, finish)
				}(job.WorktreePath, job.DocsHubPath, job.ID, job.Kind)
				return nil
			})
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

func runUpdate(args []string, stdout io.Writer) error {
	suite := false
	onTimeout := "reject"
	expired := false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--suite":
			suite = true
		case args[i] == "--on-timeout" && i+1 < len(args):
			i++
			onTimeout = args[i]
		case strings.HasPrefix(args[i], "--on-timeout="):
			onTimeout = strings.TrimPrefix(args[i], "--on-timeout=")
		case args[i] == "--drain-expired":
			expired = true
		default:
			return fmt.Errorf("unknown update flag %s", args[i])
		}
	}
	if suite {
		if !service.SuiteAllowed(false, onTimeout, expired) {
			return service.ErrSuiteBusy
		}
		fmt.Fprintln(stdout, "update suite")
		return nil
	}
	fmt.Fprintln(stdout, "update")
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
