// Package service renders and installs the user-level LaunchAgent for the agent.
package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"
)

const agentLabel = "io.iazio.iazio-agent"

// DefaultFleetAPI is the default canonical fleet control plane API URL.
const DefaultFleetAPI = ""

// ErrSuiteBusy is returned when a suite update cannot start.
var ErrSuiteBusy = errors.New("suite update refused while a harness is running")

// UnitText returns the launchd, systemd, or scheduler text for a dry run.
func UnitText(goos, bin string) string {
	return ActionText(goos, "install", bin, false)
}

func resolveHarnessAPIURL() string {
	if apiEnv := strings.TrimSpace(os.Getenv("IAZIO_HARNESS_API_URL")); apiEnv != "" {
		return apiEnv
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cfgPath := filepath.Join(home, ".iazio", "config.json")
		if data, err := os.ReadFile(cfgPath); err == nil {
			var cfg struct {
				Endpoints map[string]string `json:"endpoints"`
			}
			if err := json.Unmarshal(data, &cfg); err == nil && cfg.Endpoints != nil {
				if v, ok := cfg.Endpoints["harness_api"]; ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
				if v, ok := cfg.Endpoints["harness-api"]; ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
		if b, err := os.ReadFile(filepath.Join(home, ".iazio", "api_url")); err == nil {
			if s := strings.TrimSpace(string(b)); s != "" {
				return s
			}
		}
	}
	return DefaultFleetAPI
}

// ActionText returns the dry-run text for a service action.
// Install yields a launchd plist, a systemd user unit, or a Task Scheduler command.
func ActionText(goos, action, bin string, force bool) string {
	if action == "" {
		action = "install"
	}
	if action == "install" {
		switch goos {
		case "darwin":
			return darwinPlist(bin, agentLogPath())
		case "windows":
			flag := ""
			if force {
				flag = " /F"
			}
			setApi := ""
			if apiEnv := resolveHarnessAPIURL(); apiEnv != "" {
				setApi = fmt.Sprintf("set IAZIO_HARNESS_API_URL=%s&& ", apiEnv)
			}
			return fmt.Sprintf(`schtasks /Create /TN iazio-agent /SC ONLOGON%s /TR "cmd /C set IAZIO_AGENT_NONINTERACTIVE=1&& %s\"%s\" run"`, flag, setApi, bin)
		default:
			return linuxUnit(bin)
		}
	}
	switch goos {
	case "darwin":
		return fmt.Sprintf("launchctl %s gui/%d/%s", action, os.Getuid(), agentLabel)
	case "windows":
		switch action {
		case "start", "restart":
			return "schtasks /Run /TN iazio-agent"
		case "stop":
			return "schtasks /End /TN iazio-agent"
		case "uninstall":
			return "schtasks /Delete /TN iazio-agent /F"
		default:
			return "schtasks /Query /TN iazio-agent"
		}
	default:
		return fmt.Sprintf("systemctl --user %s iazio-agent.service", action)
	}
}

// Apply returns the service text. A dry run does not call execFn.
func Apply(goos, action, bin string, dry, force bool, execFn func(name string, args ...string) error) (string, error) {
	text := ActionText(goos, action, bin, force)
	if dry || execFn == nil {
		return text, nil
	}
	argv := controlArgv(goos, action, force)
	if len(argv) == 0 {
		return text, nil
	}
	if err := execFn(argv[0], argv[1:]...); err != nil {
		return text, err
	}
	return text, nil
}

// SuiteAllowed reports whether update --suite may start.
// A running harness blocks the suite unless onTimeout is abort and the drain timeout has expired.
func SuiteAllowed(running bool, onTimeout string, expired bool) bool {
	if !running {
		return true
	}
	return onTimeout == "abort" && expired
}

func controlArgv(goos, action string, force bool) []string {
	switch goos {
	case "darwin":
		switch action {
		case "install":
			return []string{"launchctl", "bootstrap", "gui/iazio-agent"}
		case "uninstall":
			return []string{"launchctl", "bootout", "gui/iazio-agent"}
		default:
			return []string{"launchctl", action, "gui/iazio-agent"}
		}
	case "windows":
		switch action {
		case "install":
			args := []string{"schtasks", "/Create", "/TN", "iazio-agent"}
			if force {
				args = append(args, "/F")
			}
			return args
		case "start", "restart":
			return []string{"schtasks", "/Run", "/TN", "iazio-agent"}
		case "stop":
			return []string{"schtasks", "/End", "/TN", "iazio-agent"}
		case "uninstall":
			return []string{"schtasks", "/Delete", "/TN", "iazio-agent", "/F"}
		default:
			return []string{"schtasks", "/Query", "/TN", "iazio-agent"}
		}
	default:
		if action == "install" {
			args := []string{"systemctl", "--user", "enable", "--now"}
			if force {
				args = append(args, "--force")
			}
			return append(args, "iazio-agent.service")
		}
		return []string{"systemctl", "--user", action, "iazio-agent.service"}
	}
}

func darwinPlist(bin, logPath string) string {
	apiXml := ""
	if apiEnv := resolveHarnessAPIURL(); apiEnv != "" {
		apiXml = fmt.Sprintf("\n\t\t<key>IAZIO_HARNESS_API_URL</key>\n\t\t<string>%s</string>", apiEnv)
	}
	homeXml := ""
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		homeXml = fmt.Sprintf("\n\t\t<key>HOME</key>\n\t\t<string>%s</string>", home)
	}
	userXml := ""
	if u := os.Getenv("USER"); u != "" {
		userXml = fmt.Sprintf("\n\t\t<key>USER</key>\n\t\t<string>%s</string>", u)
	} else if curr, err := user.Current(); err == nil && curr.Username != "" {
		userXml = fmt.Sprintf("\n\t\t<key>USER</key>\n\t\t<string>%s</string>", curr.Username)
	}
	pathXml := ""
	if p := os.Getenv("PATH"); p != "" {
		pathXml = fmt.Sprintf("\n\t\t<key>PATH</key>\n\t\t<string>%s</string>", p)
	}
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>%s</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>run</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<true/>
	<key>ThrottleInterval</key>
	<integer>60</integer>
	<key>ProcessType</key>
	<string>Background</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>IAZIO_AGENT_NONINTERACTIVE</key>
		<string>1</string>%s%s%s%s
	</dict>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`, agentLabel, bin, apiXml, homeXml, userXml, pathXml, logPath, logPath)
}

func agentLogPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return filepath.Join(".iazio", "logs", "agent.log")
	}
	return filepath.Join(home, ".iazio", "logs", "agent.log")
}

// InstallDarwin writes the user LaunchAgent and bootstraps it into gui/<uid>.
func InstallDarwin(bin string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home: %w", err)
	}
	return InstallDarwinAt(home, bin, execCommand)
}

// InstallDarwinAt writes the plist under home and bootstraps it. run executes launchctl.
func InstallDarwinAt(home, bin string, run func(name string, args ...string) error) error {
	if run == nil {
		return errors.New("launchctl runner is required")
	}
	if bin == "" {
		return errors.New("binary path is required")
	}
	plistPath := filepath.Join(home, "Library", "LaunchAgents", agentLabel+".plist")
	logPath := filepath.Join(home, ".iazio", "logs", "agent.log")
	if err := os.MkdirAll(filepath.Dir(plistPath), 0o755); err != nil {
		return fmt.Errorf("create LaunchAgents directory: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}
	if err := os.WriteFile(plistPath, []byte(darwinPlist(bin, logPath)), 0o644); err != nil {
		return fmt.Errorf("write plist: %w", err)
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	target := domain + "/" + agentLabel
	_ = run("launchctl", "bootout", target)
	if err := run("launchctl", "bootstrap", domain, plistPath); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w", err)
	}
	if err := run("launchctl", "enable", target); err != nil {
		return fmt.Errorf("launchctl enable: %w", err)
	}
	return nil
}

func linuxUnit(bin string) string {
	apiLine := ""
	if apiEnv := resolveHarnessAPIURL(); apiEnv != "" {
		apiLine = fmt.Sprintf("Environment=IAZIO_HARNESS_API_URL=%s\n", apiEnv)
	}
	return fmt.Sprintf(`[Unit]
Description=iazio-agent user supervisor
After=network.target

[Service]
Type=simple
ExecStart=%s run
Restart=on-failure
RestartSec=60s
Environment=IAZIO_AGENT_NONINTERACTIVE=1
%s
[Install]
WantedBy=default.target
`, bin, apiLine)
}

// IsWSL reports whether the current environment is Windows Subsystem for Linux (WSL).
func IsWSL() bool {
	return IsWSLWith(os.Getenv, func() ([]byte, error) {
		return os.ReadFile("/proc/sys/kernel/osrelease")
	})
}

// IsWSLWith allows injecting getenv and readOSRelease for testing.
func IsWSLWith(getenv func(string) string, readOSRelease func() ([]byte, error)) bool {
	if getenv != nil {
		if getenv("WSL_DISTRO_NAME") != "" || getenv("WSL_INTEROP") != "" {
			return true
		}
	}
	if readOSRelease != nil {
		if data, err := readOSRelease(); err == nil {
			if strings.Contains(strings.ToLower(string(data)), "microsoft") {
				return true
			}
		}
	}
	return false
}

// WSLSystemdBlock reports when this process is WSL without systemd.
func WSLSystemdBlock() string {
	data, err := os.ReadFile("/proc/sys/kernel/osrelease")
	_, statErr := os.Stat("/run/systemd/system")
	return wslSystemdBlock(string(data), err == nil, statErr == nil)
}

func wslSystemdBlock(release string, haveRelease, systemd bool) string {
	isWSL := IsWSLWith(nil, func() ([]byte, error) {
		if !haveRelease {
			return nil, os.ErrNotExist
		}
		return []byte(release), nil
	})
	if !isWSL {
		return ""
	}
	if systemd {
		return ""
	}
	return "WSL is not running systemd. Set systemd=true under [boot] in /etc/wsl.conf, run `wsl --shutdown` from Windows, then retry service install"
}

// IsSystemdAvailable reports whether user-level systemd is available and running.
func IsSystemdAvailable() bool {
	if WSLSystemdBlock() != "" {
		return false
	}
	if _, err := os.Stat("/run/systemd/system"); err != nil {
		return false
	}
	return true
}

// InstallLinux writes the systemd user unit, enables it, and requests linger.
// If systemd is not active (e.g. WSL without systemd or container), it installs a resilient user daemon.
func InstallLinux(bin string) error {
	if !IsSystemdAvailable() {
		return InstallUserDaemon(bin)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home: %w", err)
	}
	err = InstallLinuxAt(home, bin, execCommand)
	if err != nil && (strings.Contains(err.Error(), "Failed to connect to bus") || strings.Contains(err.Error(), "No such file or directory")) {
		return InstallUserDaemon(bin)
	}
	return err
}

// InstallUserDaemon installs a resilient non-root, non-systemd user daemon.
func InstallUserDaemon(bin string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home: %w", err)
	}
	return InstallUserDaemonAt(home, bin, execCommand)
}

// InstallUserDaemonAt writes the daemon runner script and starts it immediately.
func InstallUserDaemonAt(home, bin string, run func(name string, args ...string) error) error {
	daemonDir := filepath.Join(home, ".iazio", "daemon")
	logDir := filepath.Join(home, ".iazio", "logs")
	if err := os.MkdirAll(daemonDir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return err
	}

	runnerPath := filepath.Join(daemonDir, "runner.sh")
	logPath := filepath.Join(logDir, "agent.log")
	pidPath := filepath.Join(home, ".iazio", "agent.pid")
	apiURL := resolveHarnessAPIURL()

	runnerScript := fmt.Sprintf("#!/bin/sh\n"+
		"# iazio-agent user-space supervisor runner\n"+
		"export IAZIO_AGENT_NONINTERACTIVE=1\n"+
		"export IAZIO_HARNESS_API_URL=\"%s\"\n"+
		"echo $$ > \"%s\"\n"+
		"exec \"%s\" run >> \"%s\" 2>&1\n", apiURL, pidPath, bin, logPath)

	if err := os.WriteFile(runnerPath, []byte(runnerScript), 0o755); err != nil {
		return err
	}

	setupUserCrontab(runnerPath, run)
	return StartUserDaemonAt(home, bin, run)
}

func setupUserCrontab(runnerPath string, run func(name string, args ...string) error) {
	if run != nil {
		_ = run("crontab", "-")
		return
	}
	out, err := exec.Command("crontab", "-l").Output()
	var lines []string
	if err == nil {
		for _, l := range strings.Split(string(out), "\n") {
			l = strings.TrimSpace(l)
			if l != "" && !strings.Contains(l, "iazio-agent") {
				lines = append(lines, l)
			}
		}
	}
	cronEntry := fmt.Sprintf(`* * * * * pgrep -f "iazio-agent run" >/dev/null || nohup /bin/sh "%s" >/dev/null 2>&1 &`, runnerPath)
	rebootEntry := fmt.Sprintf(`@reboot nohup /bin/sh "%s" >/dev/null 2>&1 &`, runnerPath)
	lines = append(lines, cronEntry, rebootEntry)
	newCron := strings.Join(lines, "\n") + "\n"

	cmd := exec.Command("crontab", "-")
	cmd.Stdin = strings.NewReader(newCron)
	_ = cmd.Run()
}

// StartUserDaemonAt starts the runner in background if not already alive.
func StartUserDaemonAt(home, bin string, run func(name string, args ...string) error) error {
	pidPath := filepath.Join(home, ".iazio", "agent.pid")
	if isDaemonPIDAlive(pidPath) {
		return nil
	}
	runnerPath := filepath.Join(home, ".iazio", "daemon", "runner.sh")
	if _, err := os.Stat(runnerPath); os.IsNotExist(err) {
		return InstallUserDaemonAt(home, bin, run)
	}
	if run != nil {
		return run("/bin/sh", runnerPath)
	}
	cmd := exec.Command("/bin/sh", runnerPath)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start user daemon: %w", err)
	}
	_ = cmd.Process.Release()
	return nil
}

// StopUserDaemonAt terminates the user daemon process if running.
func StopUserDaemonAt(home string) error {
	pidPath := filepath.Join(home, ".iazio", "agent.pid")
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return nil
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		_ = os.Remove(pidPath)
		return nil
	}
	proc, err := os.FindProcess(pid)
	if err == nil {
		_ = proc.Signal(syscall.SIGTERM)
		time.Sleep(200 * time.Millisecond)
	}
	_ = os.Remove(pidPath)
	return nil
}

// StatusUserDaemonAt reports whether the daemon process is alive.
func StatusUserDaemonAt(home string) (bool, int) {
	pidPath := filepath.Join(home, ".iazio", "agent.pid")
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return false, 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return false, 0
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false, 0
	}
	if err := proc.Signal(syscall.Signal(0)); err == nil {
		return true, pid
	}
	return false, 0
}

// UninstallUserDaemonAt stops the daemon, cleans crontab, and removes runner files.
func UninstallUserDaemonAt(home string, run ...func(name string, args ...string) error) error {
	_ = StopUserDaemonAt(home)
	if len(run) > 0 && run[0] != nil {
		_ = run[0]("crontab", "-")
	} else {
		out, err := exec.Command("crontab", "-l").Output()
		if err == nil {
			var lines []string
			for _, l := range strings.Split(string(out), "\n") {
				l = strings.TrimSpace(l)
				if l != "" && !strings.Contains(l, "iazio-agent") {
					lines = append(lines, l)
				}
			}
			newCron := strings.Join(lines, "\n") + "\n"
			cmd := exec.Command("crontab", "-")
			cmd.Stdin = strings.NewReader(newCron)
			_ = cmd.Run()
		}
	}
	_ = os.RemoveAll(filepath.Join(home, ".iazio", "daemon"))
	_ = os.Remove(filepath.Join(home, ".iazio", "agent.pid"))
	return nil
}

func isDaemonPIDAlive(pidPath string) bool {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}

// InstallLinuxAt writes the unit under home and runs the supplied commands.
func InstallLinuxAt(home, bin string, run func(name string, args ...string) error) error {
	if run == nil {
		return errors.New("systemctl runner is required")
	}
	unitPath := filepath.Join(home, ".config", "systemd", "user", "iazio-agent.service")
	if err := os.MkdirAll(filepath.Dir(unitPath), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(unitPath, []byte(linuxUnit(bin)), 0o644); err != nil {
		return err
	}
	if err := run("systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	if err := run("systemctl", "--user", "enable", "--now", "iazio-agent.service"); err != nil {
		return err
	}
	return run("loginctl", "enable-linger")
}

// InstallWindows registers an ONLOGON task for the current user.
func InstallWindows(bin string) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home: %w", err)
	}
	return InstallWindowsAt(home, bin, windowsLogonUser(), execCommand)
}

// InstallWindowsAt writes the launcher and task XML, then registers the task.
func InstallWindowsAt(home, bin, user string, run func(name string, args ...string) error) error {
	if run == nil {
		return errors.New("schtasks runner is required")
	}
	dir := filepath.Join(home, ".iazio", "tasks")
	logDir := filepath.Join(home, ".iazio", "logs")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return err
	}
	batPath := filepath.Join(dir, "iazio-agent.bat")
	xmlPath := filepath.Join(dir, "iazio-agent.xml")
	logPath := filepath.Join(logDir, "agent.log")

	apiLine := ""
	if apiEnv := resolveHarnessAPIURL(); apiEnv != "" {
		apiLine = fmt.Sprintf("set \"IAZIO_HARNESS_API_URL=%s\"\r\n", apiEnv)
	}
	bat := fmt.Sprintf("@echo off\r\nset \"IAZIO_AGENT_NONINTERACTIVE=1\"\r\n%s\"%s\" run >> \"%s\" 2>&1\r\n", apiLine, bin, logPath)
	xmlDoc := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>%s</UserId>
    </LogonTrigger>
    <TimeTrigger>
      <Enabled>true</Enabled>
      <StartBoundary>2026-01-01T00:00:00</StartBoundary>
      <Repetition>
        <Interval>PT5M</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
    </TimeTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <StartWhenAvailable>true</StartWhenAvailable>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
  </Settings>
  <Actions Context="Author"><Exec><Command>%s</Command></Exec></Actions>
</Task>
`, user, batPath)
	if err := os.WriteFile(batPath, []byte(bat), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(xmlPath, encodeUTF16LEWithBOM(xmlDoc), 0o644); err != nil {
		return err
	}
	if err := run("schtasks", "/Create", "/TN", "iazio-agent", "/XML", xmlPath, "/F"); err != nil {
		return err
	}
	_ = run("schtasks", "/Run", "/TN", "iazio-agent")
	return nil
}

func encodeUTF16LEWithBOM(s string) []byte {
	u16 := utf16.Encode([]rune(s))
	out := make([]byte, 2+len(u16)*2)
	out[0], out[1] = 0xFF, 0xFE
	for i, v := range u16 {
		out[2+i*2] = byte(v)
		out[2+i*2+1] = byte(v >> 8)
	}
	return out
}

func windowsLogonUser() string {
	if v := strings.TrimSpace(os.Getenv("USERNAME")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv("USER")); v != "" {
		return v
	}
	current, err := user.Current()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(current.Username)
}

func execCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, string(out))
	}
	return nil
}
