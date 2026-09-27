// Package service renders and installs the user-level LaunchAgent for the agent.
package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf16"
)

const agentLabel = "io.iazio.iazio-agent"

// ErrSuiteBusy is returned when a suite update cannot start.
var ErrSuiteBusy = errors.New("suite update refused while a harness is running")

// UnitText returns the launchd, systemd, or scheduler text for a dry run.
func UnitText(goos, bin string) string {
	return ActionText(goos, "install", bin, false)
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
			return fmt.Sprintf(`schtasks /Create /TN iazio-agent /SC ONLOGON%s /TR "cmd /C set IAZIO_AGENT_NONINTERACTIVE=1&& \"%s\" run"`, flag, bin)
		default:
			return linuxUnit(bin)
		}
	}
	switch goos {
	case "darwin":
		return fmt.Sprintf("launchctl %s gui/%d/%s", action, os.Getuid(), agentLabel)
	case "windows":
		return "schtasks /TN iazio-agent /Query"
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
		if action == "install" {
			args := []string{"schtasks", "/Create", "/TN", "iazio-agent"}
			if force {
				args = append(args, "/F")
			}
			return args
		}
		return []string{"schtasks", "/Query", "/TN", "iazio-agent"}
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
		<string>1</string>
	</dict>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`, agentLabel, bin, logPath, logPath)
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
	_ = run("launchctl", "enable", target)
	return nil
}

func linuxUnit(bin string) string {
	return fmt.Sprintf(`[Unit]
Description=iazio-agent user supervisor
After=network.target

[Service]
Type=simple
ExecStart=%s run
Restart=on-failure
RestartSec=60s
Environment=IAZIO_AGENT_NONINTERACTIVE=1

[Install]
WantedBy=default.target
`, bin)
}

// WSLSystemdBlock reports when this process is WSL without systemd.
func WSLSystemdBlock() string {
	data, err := os.ReadFile("/proc/sys/kernel/osrelease")
	_, statErr := os.Stat("/run/systemd/system")
	return wslSystemdBlock(string(data), err == nil, statErr == nil)
}

func wslSystemdBlock(release string, haveRelease, systemd bool) string {
	if !haveRelease || !strings.Contains(strings.ToLower(release), "microsoft") {
		return ""
	}
	if systemd {
		return ""
	}
	return "WSL is not running systemd. Set systemd=true under [boot] in /etc/wsl.conf, run `wsl --shutdown` from Windows, then retry service install"
}

// InstallLinux writes the systemd user unit, enables it, and requests linger.
func InstallLinux(bin string) error {
	if msg := WSLSystemdBlock(); msg != "" {
		return errors.New(msg)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve home: %w", err)
	}
	return InstallLinuxAt(home, bin, execCommand)
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
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	batPath := filepath.Join(dir, "iazio-agent.bat")
	xmlPath := filepath.Join(dir, "iazio-agent.xml")
	bat := fmt.Sprintf("@echo off\r\nset \"IAZIO_AGENT_NONINTERACTIVE=1\"\r\n\"%s\" run\r\n", bin)
	xmlDoc := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers><LogonTrigger><Enabled>true</Enabled><UserId>%s</UserId></LogonTrigger></Triggers>
  <Settings>
    <StartWhenAvailable>true</StartWhenAvailable>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
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
	return run("schtasks", "/Create", "/TN", "iazio-agent", "/XML", xmlPath, "/F")
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
