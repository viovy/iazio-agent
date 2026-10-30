// Package service renders and installs the user-level LaunchAgent for the agent.
package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf16"
)

const DarwinLabel = "io.iazio.iazio-agent"
const DarwinWatcherLabel = "io.iazio.iazio-agent-watcher"
const DarwinWatchdogLabel = "io.iazio.iazio-agent-watchdog"

const agentLabel = DarwinLabel
const watcherLabel = DarwinWatcherLabel
const watchdogLabel = DarwinWatchdogLabel

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

func resolveElasticsearchURL() string {
	if esEnv := strings.TrimSpace(os.Getenv("IAZIO_ELASTICSEARCH_URL")); esEnv != "" {
		return esEnv
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		cfgPath := filepath.Join(home, ".iazio", "agent.json")
		if data, err := os.ReadFile(cfgPath); err == nil {
			var cfg struct {
				ElasticsearchURL string `json:"elasticsearch_url"`
			}
			if err := json.Unmarshal(data, &cfg); err == nil && strings.TrimSpace(cfg.ElasticsearchURL) != "" {
				return strings.TrimSpace(cfg.ElasticsearchURL)
			}
		}
		cfgPath2 := filepath.Join(home, ".iazio", "config.json")
		if data, err := os.ReadFile(cfgPath2); err == nil {
			var cfg struct {
				Endpoints map[string]string `json:"endpoints"`
			}
			if err := json.Unmarshal(data, &cfg); err == nil && cfg.Endpoints != nil {
				if v, ok := cfg.Endpoints["elasticsearch"]; ok && strings.TrimSpace(v) != "" {
					return strings.TrimSpace(v)
				}
			}
		}
	}
	return ""
}

// ActionText returns the dry-run text for a service action.
// Install yields a launchd plist, a systemd user unit, or a Task Scheduler command.
func ActionText(goos, action, bin string, force bool) string {
	if action == "" {
		action = "install"
	}
	switch action {
	case "dev-install":
		return fmt.Sprintf("hot-swap %s and restart supervisor", bin)
	case "reconcile":
		return "reconcile user service configuration and watchdog units"
	case "pause":
		return "pause supervisor and place maintenance lock"
	case "resume":
		return "resume supervisor and remove maintenance lock"
	case "revive-check":
		return "check supervisor health and auto-revive if inactive"
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
	esXml := ""
	if esEnv := resolveElasticsearchURL(); esEnv != "" {
		esXml = fmt.Sprintf("\n\t\t<key>IAZIO_ELASTICSEARCH_URL</key>\n\t\t<string>%s</string>", esEnv)
	}
	maintenancePath := ""
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		maintenancePath = filepath.Join(home, ".iazio", "maintenance.lock")
	} else {
		maintenancePath = filepath.Join(".iazio", "maintenance.lock")
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
	<key>AbandonProcessGroup</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>PathState</key>
		<dict>
			<key>%s</key>
			<false/>
		</dict>
	</dict>
	<key>ThrottleInterval</key>
	<integer>3</integer>
	<key>SoftResourceLimits</key>
	<dict>
		<key>NumberOfFiles</key>
		<integer>65536</integer>
	</dict>
	<key>ProcessType</key>
	<string>Standard</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>IAZIO_AGENT_NONINTERACTIVE</key>
		<string>1</string>%s%s%s%s%s
	</dict>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`, agentLabel, bin, maintenancePath, apiXml, homeXml, userXml, pathXml, esXml, logPath, logPath)
}

// DarwinWatcherPlist returns the LaunchAgent plist definition to monitor binary replacements.
func DarwinWatcherPlist(bin, logPath string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>%s</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>service</string>
		<string>revive-check</string>
	</array>
	<key>WatchPaths</key>
	<array>
		<string>%s</string>
	</array>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`, watcherLabel, bin, bin, logPath, logPath)
}

// DarwinWatchdogPlist returns the periodic LaunchAgent plist definition that triggers revive-check.
func DarwinWatchdogPlist(bin, logPath string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>%s</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>service</string>
		<string>revive-check</string>
	</array>
	<key>StartInterval</key>
	<integer>120</integer>
	<key>RunAtLoad</key>
	<true/>
	<key>AbandonProcessGroup</key>
	<true/>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`, watchdogLabel, bin, logPath, logPath)
}

// InstallDarwinResilience installs and enables the watcher and watchdog LaunchAgents.
func InstallDarwinResilience(home, bin string, run func(name string, args ...string) error) error {
	if run == nil {
		run = execCommand
	}
	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	logDir := filepath.Join(home, ".iazio", "logs")
	logPath := filepath.Join(logDir, "agent.log")
	if err := os.MkdirAll(launchAgentsDir, 0o755); err != nil {
		return fmt.Errorf("create LaunchAgents directory: %w", err)
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}

	domain := "gui/" + strconv.Itoa(os.Getuid())
	watcherTarget := domain + "/" + watcherLabel
	watchdogTarget := domain + "/" + watchdogLabel

	watcherPlistPath := filepath.Join(launchAgentsDir, watcherLabel+".plist")
	expectedWatcher := DarwinWatcherPlist(bin, logPath)
	existingWatcher, wErr := os.ReadFile(watcherPlistPath)
	watcherChanged := wErr != nil || string(existingWatcher) != expectedWatcher
	if watcherChanged {
		if err := os.WriteFile(watcherPlistPath, []byte(expectedWatcher), 0o644); err != nil {
			return fmt.Errorf("write watcher plist: %w", err)
		}
	}
	_, watcherLoadedErr := statusCheckCommand("launchctl", "print", watcherTarget)
	watcherLoaded := watcherLoadedErr == nil
	if watcherChanged || !watcherLoaded {
		if watcherChanged {
			_ = run("launchctl", "bootout", watcherTarget)
		}
		if err := run("launchctl", "bootstrap", domain, watcherPlistPath); err != nil {
			_ = os.Remove(watcherPlistPath)
			return fmt.Errorf("launchctl bootstrap watcher: %w", err)
		}
		if err := run("launchctl", "enable", watcherTarget); err != nil {
			_ = os.Remove(watcherPlistPath)
			return fmt.Errorf("launchctl enable watcher: %w", err)
		}
	}

	watchdogPlistPath := filepath.Join(launchAgentsDir, watchdogLabel+".plist")
	expectedWatchdog := DarwinWatchdogPlist(bin, logPath)
	existingWatchdog, wdErr := os.ReadFile(watchdogPlistPath)
	watchdogChanged := wdErr != nil || string(existingWatchdog) != expectedWatchdog
	if watchdogChanged {
		if err := os.WriteFile(watchdogPlistPath, []byte(expectedWatchdog), 0o644); err != nil {
			return fmt.Errorf("write watchdog plist: %w", err)
		}
	}
	insideWatchdog := os.Getenv("XPC_SERVICE_NAME") == DarwinWatchdogLabel
	_, watchdogLoadedErr := statusCheckCommand("launchctl", "print", watchdogTarget)
	watchdogLoaded := watchdogLoadedErr == nil
	if insideWatchdog {
		if !watchdogLoaded {
			if err := run("launchctl", "bootstrap", domain, watchdogPlistPath); err != nil {
				_ = os.Remove(watchdogPlistPath)
				return fmt.Errorf("launchctl bootstrap watchdog: %w", err)
			}
			if err := run("launchctl", "enable", watchdogTarget); err != nil {
				_ = os.Remove(watchdogPlistPath)
				return fmt.Errorf("launchctl enable watchdog: %w", err)
			}
		}
	} else if watchdogChanged || !watchdogLoaded {
		if watchdogChanged {
			_ = run("launchctl", "bootout", watchdogTarget)
		}
		if err := run("launchctl", "bootstrap", domain, watchdogPlistPath); err != nil {
			_ = os.Remove(watchdogPlistPath)
			return fmt.Errorf("launchctl bootstrap watchdog: %w", err)
		}
		if err := run("launchctl", "enable", watchdogTarget); err != nil {
			_ = os.Remove(watchdogPlistPath)
			return fmt.Errorf("launchctl enable watchdog: %w", err)
		}
	}
	return nil
}

// UninstallDarwinResilience unloads and deletes the watcher and watchdog LaunchAgents.
func UninstallDarwinResilience(home string, run func(name string, args ...string) error) error {
	if run == nil {
		run = execCommand
	}
	domain := "gui/" + strconv.Itoa(os.Getuid())
	watcherTarget := domain + "/" + watcherLabel
	watchdogTarget := domain + "/" + watchdogLabel

	_ = run("launchctl", "bootout", watcherTarget)
	_ = run("launchctl", "bootout", watchdogTarget)

	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	_ = os.Remove(filepath.Join(launchAgentsDir, watcherLabel+".plist"))
	_ = os.Remove(filepath.Join(launchAgentsDir, watchdogLabel+".plist"))
	return nil
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
	if err := InstallDarwinResilience(home, bin, run); err != nil {
		return fmt.Errorf("install darwin resilience: %w", err)
	}
	return nil
}

func linuxUnit(bin string) string {
	apiLine := ""
	if apiEnv := resolveHarnessAPIURL(); apiEnv != "" {
		apiLine = fmt.Sprintf("Environment=IAZIO_HARNESS_API_URL=%s\n", apiEnv)
	}
	esLine := ""
	if esEnv := resolveElasticsearchURL(); esEnv != "" {
		esLine = fmt.Sprintf("Environment=IAZIO_ELASTICSEARCH_URL=%s\n", esEnv)
	}
	return fmt.Sprintf(`[Unit]
Description=iazio-agent user supervisor
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
ExecStart=%s run
Restart=always
RestartSec=3s
KillMode=process
WatchdogSec=30s
OOMScoreAdjust=-500
# Low intrusion: yield CPU/IO to interactive sessions (STORY-GOV-3198 / STORY-GOV-3597).
Nice=10
IOSchedulingClass=idle
IOSchedulingPriority=7
EnvironmentFile=-%%h/.iazio/agent.env
StandardOutput=journal
StandardError=journal
Environment=IAZIO_AGENT_NONINTERACTIVE=1
%s%s
[Install]
WantedBy=default.target
`, bin, apiLine, esLine)
}

// LinuxWatcherPath returns the systemd .path unit definition to monitor binary replacements.
func LinuxWatcherPath(bin string) string {
	return fmt.Sprintf(`[Unit]
Description=Watch iazio-agent binary for updates and dev replacements
After=network.target

[Path]
PathModified=%s
PathChanged=%s
Unit=iazio-agent-watcher.service

[Install]
WantedBy=default.target
`, bin, bin)
}

// LinuxWatcherService returns the oneshot service that restarts the agent on binary changes.
func LinuxWatcherService() string {
	return `[Unit]
Description=Auto-restart iazio-agent on binary replacement
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/bin/systemctl --user restart iazio-agent.service
`
}

// LinuxWatchdogTimer returns the periodic timer definition that triggers revive-check.
func LinuxWatchdogTimer() string {
	return `[Unit]
Description=iazio-agent auto-revive watchdog timer
After=network.target

[Timer]
OnBootSec=1m
OnUnitActiveSec=2m
AccuracySec=10s
Unit=iazio-agent-watchdog.service

[Install]
WantedBy=timers.target default.target
`
}

// LinuxWatchdogService returns the watchdog service definition executing revive-check.
func LinuxWatchdogService(bin string) string {
	return fmt.Sprintf(`[Unit]
Description=iazio-agent auto-revive watchdog
After=network.target

[Service]
Type=oneshot
ExecStart=%s service revive-check
`, bin)
}

// InstallLinuxResilience installs and enables the .path watcher and .timer watchdog units.
func InstallLinuxResilience(home, bin string, run func(name string, args ...string) error) error {
	if run == nil {
		run = execCommand
	}
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return err
	}

	pathUnitFile := filepath.Join(unitDir, "iazio-agent-watcher.path")
	if err := os.WriteFile(pathUnitFile, []byte(LinuxWatcherPath(bin)), 0o644); err != nil {
		return err
	}

	watcherServiceFile := filepath.Join(unitDir, "iazio-agent-watcher.service")
	if err := os.WriteFile(watcherServiceFile, []byte(LinuxWatcherService()), 0o644); err != nil {
		return err
	}

	timerUnitFile := filepath.Join(unitDir, "iazio-agent-watchdog.timer")
	if err := os.WriteFile(timerUnitFile, []byte(LinuxWatchdogTimer()), 0o644); err != nil {
		return err
	}

	watchdogServiceFile := filepath.Join(unitDir, "iazio-agent-watchdog.service")
	if err := os.WriteFile(watchdogServiceFile, []byte(LinuxWatchdogService(bin)), 0o644); err != nil {
		return err
	}

	if err := run("systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	_ = run("systemctl", "--user", "enable", "--now", "iazio-agent-watcher.path")
	_ = run("systemctl", "--user", "enable", "--now", "iazio-agent-watchdog.timer")
	return nil
}

// copyBinaryAtomic copies a binary to dest using a temporary file and atomic rename,
// which avoids ETXTBSY (text file busy) if dest is currently running.
func copyBinaryAtomic(src, dest string) error {
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

	tmp := fmt.Sprintf("%s.tmp.%d", dest, time.Now().UnixNano())
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		if runtime.GOOS == "windows" {
			oldPath := dest + ".old"
			_ = os.Remove(oldPath)
			if renErr := os.Rename(dest, oldPath); renErr == nil {
				if err2 := os.Rename(tmp, dest); err2 == nil {
					_ = os.Remove(oldPath)
					return nil
				}
			}
		}
		return err
	}
	return nil
}

func validateCandidateBinary(src string, info os.FileInfo) error {
	if info.Size() == 0 {
		return errors.New("candidate binary is empty")
	}
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open candidate binary: %w", err)
	}
	defer f.Close()

	header := make([]byte, 512)
	n, err := f.Read(header)
	if err != nil && err != io.EOF {
		return fmt.Errorf("read candidate header: %w", err)
	}
	if n < 4 {
		return errors.New("candidate file too small to be an executable")
	}

	// Reject empty scripts (e.g. #!/bin/sh with no executable command lines)
	if bytes.HasPrefix(header, []byte("#!")) {
		content, _ := os.ReadFile(src)
		lines := strings.Split(strings.TrimSpace(string(content)), "\n")
		hasCommand := false
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l != "" && !strings.HasPrefix(l, "#") {
				hasCommand = true
				break
			}
		}
		if !hasCommand {
			return errors.New("candidate binary is an empty script without executable commands")
		}
	}
	return nil
}

// ReconcileServiceConfiguration inspects user service units and corrects missing,
// outdated, or drifted configuration (e.g. missing WatchdogSec, wrong ExecStart path).
func ReconcileServiceConfiguration(home, bin string, run func(name string, args ...string) error) error {
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		home = h
	}
	if run == nil {
		run = execCommand
	}
	if bin == "" || bin == "iazio-agent" || !filepath.IsAbs(bin) || strings.Contains(bin, filepath.Join(".iazio", "bin")) {
		if runtime.GOOS == "windows" {
			bin = filepath.Join(home, "bin", "iazio-agent.exe")
		} else {
			bin = filepath.Join(home, ".local", "bin", "iazio-agent")
		}
	}

	if runtime.GOOS == "linux" {
		return reconcileLinuxServiceConfiguration(home, bin, run)
	} else if runtime.GOOS == "darwin" {
		return reconcileDarwinServiceConfiguration(home, bin, run)
	} else if runtime.GOOS == "windows" {
		tasksDir := filepath.Join(home, ".iazio", "tasks")
		cleanStaleBackups(tasksDir)
		cleanStaleBackups(filepath.Join(home, ".local", "bin"))
		cleanStaleBackups(filepath.Join(home, "bin"))
		binDir := filepath.Join(home, ".iazio", "bin")
		cleanStaleBackups(binDir)
		for _, legacyBin := range []string{"iazio-agent", "iazio-harvester", "iazio-harness"} {
			_ = os.Remove(filepath.Join(binDir, legacyBin+".exe"))
			_ = os.Remove(filepath.Join(binDir, legacyBin+".old"))
			_ = os.Remove(filepath.Join(binDir, legacyBin+".version"))
		}

		// Per ADR-002, iazio-harness must NEVER be installed as a user or system service.
		harnessBat := filepath.Join(tasksDir, "iazio-harness.bat")
		harnessXML := filepath.Join(tasksDir, "iazio-harness.xml")
		if _, err := os.Stat(harnessBat); err == nil {
			_ = run("schtasks", "/End", "/TN", "iazio-harness")
			_ = run("schtasks", "/Delete", "/TN", "iazio-harness", "/F")
			_ = os.Remove(harnessBat)
			_ = os.Remove(harnessXML)
		}

		batPath := filepath.Join(tasksDir, "iazio-agent.bat")
		xmlPath := filepath.Join(tasksDir, "iazio-agent.xml")
		needsInstall := false
		if data, err := os.ReadFile(batPath); err != nil || !strings.Contains(string(data), bin) {
			needsInstall = true
		} else if xData, err := os.ReadFile(xmlPath); err != nil || (!strings.Contains(string(xData), "PT1M") && !bytes.Contains(xData, []byte{'P', 0, 'T', 0, '1', 0, 'M', 0})) {
			needsInstall = true
		} else {
			if qOut, qErr := statusCheckCommand("schtasks", "/Query", "/TN", "iazio-agent", "/V", "/FO", "LIST"); qErr != nil {
				needsInstall = true
			} else if len(qOut) > 0 && strings.Contains(string(qOut), "Task To Run:") && !strings.Contains(strings.ToLower(string(qOut)), strings.ToLower(batPath)) {
				needsInstall = true
			}
		}
		if needsInstall {
			if err := InstallWindowsAt(home, bin, windowsLogonUser(), run); err != nil {
				return err
			}
		} else {
			lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
			inMaintenance := false
			if info, err := os.Stat(lockFile); err == nil && time.Since(info.ModTime()) <= 2*time.Hour {
				inMaintenance = true
			}
			if !inMaintenance {
				if out, err := statusCheckCommand("schtasks", "/Query", "/TN", "iazio-agent", "/FO", "LIST"); err == nil {
					if running, _, ok := ParseSchtasksStatus(string(out)); ok && !running {
						_ = run("schtasks", "/Run", "/TN", "iazio-agent")
					}
				}
			}
		}

		// Reconcile companion iazio-harvester Windows user service if installed in canonical ~/bin
		harvesterBin := filepath.Join(home, "bin", "iazio-harvester.exe")
		if _, err := os.Stat(harvesterBin); err == nil && !isTestProcess() {
			_ = exec.Command(harvesterBin, "service", "reconcile").Run()
		}
		return nil
	}
	return nil
}

func reconcileLinuxServiceConfiguration(home, bin string, run func(name string, args ...string) error) error {
	if run == nil {
		run = execCommand
	}
	if bin == "" || bin == "iazio-agent" || !filepath.IsAbs(bin) || strings.Contains(bin, filepath.Join(".iazio", "bin")) {
		bin = filepath.Join(home, ".local", "bin", "iazio-agent")
	}
	cleanStaleBackups(filepath.Join(home, ".iazio", "bin"))
	cleanStaleBackups(filepath.Join(home, ".local", "bin"))
	// Purge secondary binary from ~/.iazio/bin/
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-agent"))
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-agent.old"))
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-agent.version"))
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return err
	}

	serviceFile := filepath.Join(unitDir, "iazio-agent.service")
	needsReload := false

	// Check and reconcile primary service unit
	expectedService := linuxUnit(bin)
	data, err := os.ReadFile(serviceFile)
	expectedExec := fmt.Sprintf("ExecStart=%s run", bin)
	if err != nil || !strings.Contains(string(data), "WatchdogSec=") || !strings.Contains(string(data), expectedExec) || !strings.Contains(string(data), "IAZIO_AGENT_NONINTERACTIVE=1") {
		if err := os.WriteFile(serviceFile, []byte(expectedService), 0o644); err != nil {
			return fmt.Errorf("write iazio-agent.service: %w", err)
		}
		needsReload = true
	}

	// Check and reconcile watcher .path and .service
	pathFile := filepath.Join(unitDir, "iazio-agent-watcher.path")
	expectedPath := LinuxWatcherPath(bin)
	if pData, err := os.ReadFile(pathFile); err != nil || !strings.Contains(string(pData), bin) {
		if err := os.WriteFile(pathFile, []byte(expectedPath), 0o644); err != nil {
			return fmt.Errorf("write iazio-agent-watcher.path: %w", err)
		}
		needsReload = true
	}

	watcherSvcFile := filepath.Join(unitDir, "iazio-agent-watcher.service")
	if _, err := os.Stat(watcherSvcFile); err != nil {
		if err := os.WriteFile(watcherSvcFile, []byte(LinuxWatcherService()), 0o644); err != nil {
			return fmt.Errorf("write iazio-agent-watcher.service: %w", err)
		}
		needsReload = true
	}

	// Check and reconcile watchdog .timer and .service
	timerFile := filepath.Join(unitDir, "iazio-agent-watchdog.timer")
	if _, err := os.Stat(timerFile); err != nil {
		if err := os.WriteFile(timerFile, []byte(LinuxWatchdogTimer()), 0o644); err != nil {
			return fmt.Errorf("write iazio-agent-watchdog.timer: %w", err)
		}
		needsReload = true
	}

	watchdogSvcFile := filepath.Join(unitDir, "iazio-agent-watchdog.service")
	expectedWatchdogSvc := LinuxWatchdogService(bin)
	if wData, err := os.ReadFile(watchdogSvcFile); err != nil || !strings.Contains(string(wData), bin) {
		if err := os.WriteFile(watchdogSvcFile, []byte(expectedWatchdogSvc), 0o644); err != nil {
			return fmt.Errorf("write iazio-agent-watchdog.service: %w", err)
		}
		needsReload = true
	}

	if needsReload {
		_ = run("systemctl", "--user", "daemon-reload")
	}
	_ = run("systemctl", "--user", "enable", "iazio-agent.service")
	_ = run("systemctl", "--user", "enable", "--now", "iazio-agent-watcher.path")
	_ = run("systemctl", "--user", "enable", "--now", "iazio-agent-watchdog.timer")
	_ = run("loginctl", "enable-linger")

	// Per ADR-002, iazio-harness must NEVER be installed as a user or system service.
	// Purge any legacy iazio-harness systemd user units if present.
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-harness"))
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-harvester"))
	legacyHarnessUnits := []string{
		"iazio-harness.service",
		"iazio-harness-watcher.path",
		"iazio-harness-watcher.service",
		"iazio-harness-watchdog.timer",
		"iazio-harness-watchdog.service",
	}
	purgedHarness := false
	for _, u := range legacyHarnessUnits {
		uPath := filepath.Join(unitDir, u)
		if _, err := os.Stat(uPath); err == nil {
			_ = run("systemctl", "--user", "disable", "--now", u)
			_ = os.Remove(uPath)
			purgedHarness = true
		}
	}
	if purgedHarness {
		_ = run("systemctl", "--user", "daemon-reload")
	}

	// Reconcile companion iazio-harvester user service if harvester binary exists in canonical ~/.local/bin
	harvesterBin := filepath.Join(home, ".local", "bin", "iazio-harvester")
	if _, err := os.Stat(harvesterBin); err == nil && !isTestProcess() {
		_ = exec.Command(harvesterBin, "service", "reconcile").Run()
	}

	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	inMaintenance := false
	if info, err := os.Stat(lockFile); err == nil && time.Since(info.ModTime()) <= 2*time.Hour {
		inMaintenance = true
	}
	if !inMaintenance {
		_, isActErr := statusCheckCommand("systemctl", "--user", "is-active", "--quiet", "iazio-agent.service")
		if isActErr != nil {
			_ = run("systemctl", "--user", "reset-failed", "iazio-agent.service", "iazio-agent-watcher.service", "iazio-agent-watchdog.service")
			_ = run("systemctl", "--user", "restart", "iazio-agent.service")
		}
	}
	return nil
}

func isRunningInsideDarwinAgentService(target string) bool {
	if os.Getenv("XPC_SERVICE_NAME") == DarwinLabel || os.Getenv("IAZIO_AGENT_SERVICE") == "1" {
		return true
	}
	if out, err := statusCheckCommand("launchctl", "print", target); err == nil {
		outStr := string(out)
		pidStr := fmt.Sprintf("pid = %d", os.Getpid())
		if strings.Contains(outStr, "state = running") && strings.Contains(outStr, pidStr) {
			return true
		}
	}
	return false
}

func reconcileDarwinServiceConfiguration(home, bin string, run func(name string, args ...string) error) error {
	if run == nil {
		run = execCommand
	}
	if bin == "" || bin == "iazio-agent" || !filepath.IsAbs(bin) || strings.Contains(bin, filepath.Join(".iazio", "bin")) {
		bin = filepath.Join(home, ".local", "bin", "iazio-agent")
	}
	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	_ = os.MkdirAll(launchAgentsDir, 0o755)
	logDir := filepath.Join(home, ".iazio", "logs")
	_ = os.MkdirAll(logDir, 0o755)
	logPath := filepath.Join(logDir, "agent.log")

	domain := "gui/" + strconv.Itoa(os.Getuid())
	target := domain + "/" + agentLabel
	insideService := isRunningInsideDarwinAgentService(target)

	// Reconcile primary iazio-agent LaunchAgent
	agentPlist := filepath.Join(launchAgentsDir, agentLabel+".plist")
	expectedPlist := darwinPlist(bin, logPath)
	data, err := os.ReadFile(agentPlist)
	existingStr := string(data)
	if err != nil || existingStr != expectedPlist {
		_ = os.WriteFile(agentPlist, []byte(expectedPlist), 0o644)
		if !insideService {
			_ = run("launchctl", "bootout", target)
			_ = run("launchctl", "bootstrap", domain, agentPlist)
			_ = run("launchctl", "enable", target)
		}
	}
	if err := InstallDarwinResilience(home, bin, run); err != nil {
		return err
	}

	// Per ADR-002, iazio-harness must NEVER be installed as a user or system service.
	// Purge any legacy iazio-harness LaunchAgent plists if present.
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-harness"))
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", "iazio-harvester"))
	for _, legacyLabel := range []string{"io.iazio.iazio-harness", "io.iazio.iazio-harness.watcher", "io.iazio.iazio-harness.watchdog", "io.iazio.iazio-harness-watcher", "io.iazio.iazio-harness-watchdog"} {
		legacyPlist := filepath.Join(launchAgentsDir, legacyLabel+".plist")
		if _, err := os.Stat(legacyPlist); err == nil {
			_ = run("launchctl", "bootout", domain+"/"+legacyLabel)
			_ = os.Remove(legacyPlist)
		}
	}

	// Reconcile companion iazio-harvester LaunchAgent if harvester binary exists in canonical ~/.local/bin
	harvesterBin := filepath.Join(home, ".local", "bin", "iazio-harvester")
	if _, err := os.Stat(harvesterBin); err == nil && !isTestProcess() {
		_ = exec.Command(harvesterBin, "service", "reconcile").Run()
	}

	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	inMaintenance := false
	if info, err := os.Stat(lockFile); err == nil && time.Since(info.ModTime()) <= 2*time.Hour {
		inMaintenance = true
	}
	if !inMaintenance && !insideService {
		out, err := statusCheckCommand("launchctl", "print", target)
		outStr := string(out)
		if err != nil || strings.Contains(outStr, "job state = exited") || !strings.Contains(outStr, "pid =") {
			if err != nil {
				_ = run("launchctl", "bootstrap", domain, agentPlist)
			}
			_ = run("launchctl", "kickstart", "-k", target)
		}
	}
	return nil
}

// DevInstall safely hot-swaps the active binary with srcBinary and ensures the service is running.
func DevInstall(srcBinary, home string, run func(name string, args ...string) error) error {
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		home = h
	}
	if run == nil {
		run = execCommand
	}
	if srcBinary == "" {
		exe, err := os.Executable()
		if err != nil {
			return fmt.Errorf("resolve executable: %w", err)
		}
		if resolved, err := filepath.EvalSymlinks(exe); err == nil {
			srcBinary = resolved
		} else {
			srcBinary = exe
		}
	}
	srcBinary, err := filepath.Abs(srcBinary)
	if err != nil {
		return err
	}
	info, err := os.Stat(srcBinary)
	if err != nil {
		return fmt.Errorf("source binary not found: %w", err)
	}
	if info.IsDir() {
		return fmt.Errorf("source binary is a directory: %s", srcBinary)
	}
	if err := validateCandidateBinary(srcBinary, info); err != nil {
		return fmt.Errorf("invalid candidate binary: %w", err)
	}

	destDir := filepath.Join(home, ".local", "bin")
	if runtime.GOOS == "windows" {
		destDir = filepath.Join(home, "bin")
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	binName := "iazio-agent"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	dest := filepath.Join(destDir, binName)

	if err := copyBinaryAtomic(srcBinary, dest); err != nil {
		return fmt.Errorf("atomic binary swap failed: %w", err)
	}
	// Purge secondary binary from ~/.iazio/bin/
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", binName))
	_ = os.Remove(filepath.Join(home, ".iazio", "bin", binName+".old"))

	// Remove maintenance lock if present
	_ = os.Remove(filepath.Join(home, ".iazio", "maintenance.lock"))

	// Ensure resilience units and configuration are active and reconciled
	if runtime.GOOS == "linux" && IsSystemdAvailable() {
		_ = ReconcileServiceConfiguration(home, dest, run)
		_ = run("systemctl", "--user", "reset-failed", "iazio-agent.service")
		if err := run("systemctl", "--user", "restart", "iazio-agent.service"); err != nil {
			return fmt.Errorf("restart service: %w", err)
		}
	} else if runtime.GOOS == "darwin" {
		if err := ReconcileServiceConfiguration(home, dest, run); err != nil {
			return fmt.Errorf("install darwin resilience: %w", err)
		}
		if err := run("launchctl", "kickstart", "-k", fmt.Sprintf("gui/%d/%s", os.Getuid(), agentLabel)); err != nil {
			return fmt.Errorf("kickstart service: %w", err)
		}
	} else if runtime.GOOS == "windows" {
		_ = ReconcileServiceConfiguration(home, dest, run)
		_ = run("schtasks", "/Run", "/TN", "iazio-agent")
	}
	return nil
}

// ReviveCheck checks if the agent service is active, and if inactive and not locked, starts it.
func ReviveCheck(home string, run func(name string, args ...string) error) error {
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		home = h
	}
	if run == nil {
		run = execCommand
	}
	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	if info, err := os.Stat(lockFile); err == nil {
		// Maintenance lock exists. Check if it expired (> 2 hours max safety window)
		if time.Since(info.ModTime()) > 2*time.Hour {
			_ = os.Remove(lockFile)
		} else {
			return nil // Maintenance pause intentionally active
		}
	}
	bin := filepath.Join(home, ".local", "bin", "iazio-agent")
	if runtime.GOOS == "windows" {
		bin = filepath.Join(home, "bin", "iazio-agent.exe")
	}
	_ = ReconcileServiceConfiguration(home, bin, run)
	if runtime.GOOS == "linux" && IsSystemdAvailable() {
		_, err := statusCheckCommand("systemctl", "--user", "is-active", "--quiet", "iazio-agent.service")
		if err != nil {
			// Inactive! Reset failed and restart it.
			_ = run("systemctl", "--user", "reset-failed", "iazio-agent.service")
			return run("systemctl", "--user", "restart", "iazio-agent.service")
		}
	} else if runtime.GOOS == "darwin" {
		domain := "gui/" + strconv.Itoa(os.Getuid())
		target := domain + "/" + agentLabel
		out, err := statusCheckCommand("launchctl", "print", target)
		outStr := string(out)
		isDead := err != nil || strings.Contains(outStr, "job state = exited") || !strings.Contains(outStr, "pid =")
		if isDead {
			plistPath := filepath.Join(home, "Library", "LaunchAgents", agentLabel+".plist")
			_ = run("launchctl", "bootout", target)
			if err := run("launchctl", "bootstrap", domain, plistPath); err != nil {
				return fmt.Errorf("launchctl bootstrap: %w", err)
			}
			return run("launchctl", "kickstart", "-k", target)
		}
	} else if runtime.GOOS == "windows" {
		out, err := statusCheckCommand("schtasks", "/Query", "/TN", "iazio-agent", "/FO", "LIST")
		if err == nil {
			running, _, ok := ParseSchtasksStatus(string(out))
			if ok && !running {
				return run("schtasks", "/Run", "/TN", "iazio-agent")
			}
		}
	}
	return nil
}

// PauseService sets maintenance mode and cleanly stops the supervisor.
func PauseService(home string, run func(name string, args ...string) error) error {
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		home = h
	}
	if run == nil {
		run = execCommand
	}
	lockDir := filepath.Join(home, ".iazio")
	_ = os.MkdirAll(lockDir, 0o755)
	lockFile := filepath.Join(lockDir, "maintenance.lock")
	now := time.Now().Format(time.RFC3339)
	_ = os.WriteFile(lockFile, []byte(fmt.Sprintf("paused_at=%s\n", now)), 0o644)
	if runtime.GOOS == "linux" && IsSystemdAvailable() {
		return run("systemctl", "--user", "stop", "iazio-agent.service")
	} else if runtime.GOOS == "darwin" {
		return run("launchctl", "stop", fmt.Sprintf("gui/%d/%s", os.Getuid(), agentLabel))
	} else if runtime.GOOS == "windows" {
		return run("schtasks", "/End", "/TN", "iazio-agent")
	}
	return nil
}

// ResumeService removes maintenance mode and starts the supervisor.
func ResumeService(home string, run func(name string, args ...string) error) error {
	if home == "" {
		h, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		home = h
	}
	if run == nil {
		run = execCommand
	}
	lockFile := filepath.Join(home, ".iazio", "maintenance.lock")
	_ = os.Remove(lockFile)
	if runtime.GOOS == "linux" && IsSystemdAvailable() {
		return run("systemctl", "--user", "start", "iazio-agent.service")
	} else if runtime.GOOS == "darwin" {
		return run("launchctl", "kickstart", "-k", fmt.Sprintf("gui/%d/%s", os.Getuid(), agentLabel))
	} else if runtime.GOOS == "windows" {
		return run("schtasks", "/Run", "/TN", "iazio-agent")
	}
	return nil
}

// ParseSchtasksStatus extracts the Status field from `schtasks /Query /FO LIST` output.
// ok is false when the Status: line is absent so callers fail closed instead of
// treating unparseable output as a successful status probe.
func ParseSchtasksStatus(out string) (running bool, status string, ok bool) {
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if strings.HasPrefix(trimmed, "Status:") {
			statusVal := strings.TrimSpace(strings.TrimPrefix(trimmed, "Status:"))
			return strings.EqualFold(statusVal, "Running"), statusVal, true
		}
	}
	return false, "", false
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
	if err == nil {
		_ = InstallLinuxResilience(home, bin, execCommand)
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

	if err := setupUserCrontab(runnerPath, run); err != nil {
		return fmt.Errorf("setup crontab: %w", err)
	}
	return StartUserDaemonAt(home, bin, run)
}

// BuildInstallCrontab preserves unrelated crontab lines while configuring iazio-agent runner entries.
func BuildInstallCrontab(existing, runnerPath string) string {
	var lines []string
	for _, l := range strings.Split(existing, "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !strings.Contains(l, "iazio-agent") {
			lines = append(lines, l)
		}
	}
	cronEntry := fmt.Sprintf(`* * * * * pgrep -f "iazio-agent run" >/dev/null || nohup /bin/sh "%s" >/dev/null 2>&1 & # iazio-agent`, runnerPath)
	rebootEntry := fmt.Sprintf(`@reboot nohup /bin/sh "%s" >/dev/null 2>&1 & # iazio-agent`, runnerPath)
	lines = append(lines, cronEntry, rebootEntry)
	return strings.Join(lines, "\n") + "\n"
}

// BuildUninstallCrontab strips iazio-agent runner entries from existing crontab.
func BuildUninstallCrontab(existing string) string {
	var lines []string
	for _, l := range strings.Split(existing, "\n") {
		l = strings.TrimSpace(l)
		if l != "" && !strings.Contains(l, "iazio-agent") {
			lines = append(lines, l)
		}
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

var crontabListCommand = func() ([]byte, error) {
	return exec.Command("crontab", "-l").Output()
}

var crontabApplyCommand = func(stdin string) error {
	cmd := exec.Command("crontab", "-")
	cmd.Stdin = strings.NewReader(stdin)
	return cmd.Run()
}

var crontabRemoveCommand = func() error {
	return exec.Command("crontab", "-r").Run()
}

func setupUserCrontab(runnerPath string, run func(name string, args ...string) error) error {
	var existing string
	if run != nil {
		_ = BuildInstallCrontab(existing, runnerPath)
		return run("crontab", "-")
	}
	out, err := crontabListCommand()
	if err == nil {
		existing = string(out)
	}
	newCron := BuildInstallCrontab(existing, runnerPath)

	if err := crontabApplyCommand(newCron); err != nil {
		return fmt.Errorf("update crontab: %w", err)
	}
	return nil
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
	if checkPIDAlive(pid) {
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
		out, err := crontabListCommand()
		if err == nil {
			newCron := BuildUninstallCrontab(string(out))
			if newCron == "" {
				_ = crontabRemoveCommand()
			} else {
				_ = crontabApplyCommand(newCron)
			}
		}
	}
	var errs []error
	if err := os.RemoveAll(filepath.Join(home, ".iazio", "daemon")); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if err := os.Remove(filepath.Join(home, ".iazio", "agent.pid")); err != nil && !os.IsNotExist(err) {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("cleanup user daemon: %v", errs)
	}
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
	return checkPIDAlive(pid)
}

func checkPIDAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if pid == os.Getpid() {
		return true
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	if runtime.GOOS == "windows" {
		out, err := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", pid), "/NH").Output()
		if err != nil {
			return false
		}
		return strings.Contains(string(out), strconv.Itoa(pid))
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
	u := os.Getenv("USER")
	if u == "" {
		if curr, err := user.Current(); err == nil && curr.Username != "" {
			u = curr.Username
		}
	}
	if u != "" {
		if err := run("loginctl", "enable-linger", u); err == nil {
			return nil
		}
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
	cleanStaleBackups(dir)
	cleanStaleBackups(filepath.Join(home, ".iazio", "bin"))
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
        <Interval>PT1M</Interval>
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

func isTestProcess() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.") {
			return true
		}
	}
	exe, err := os.Executable()
	if err == nil {
		base := strings.ToLower(filepath.Base(exe))
		if strings.HasSuffix(base, ".test") || strings.HasSuffix(base, ".test.exe") {
			return true
		}
	}
	return false
}

func isServiceControlCommand(name string) bool {
	base := strings.ToLower(strings.TrimSuffix(filepath.Base(name), ".exe"))
	switch base {
	case "schtasks", "systemctl", "launchctl", "loginctl", "crontab", "taskkill":
		return true
	default:
		return false
	}
}

var execCommand = func(name string, args ...string) error {
	if isTestProcess() && isServiceControlCommand(name) {
		return nil
	}
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %w: %s", name, args, err, string(out))
	}
	return nil
}

var statusCheckCommand = func(name string, args ...string) ([]byte, error) {
	if isTestProcess() && isServiceControlCommand(name) {
		return nil, errors.New("service control command disabled in test process")
	}
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

// SetExecCommandForTest sets the command runner and optional status checker for testing,
// returning a cleanup function to restore originals.
func SetExecCommandForTest(runner func(name string, args ...string) error, statusChecker ...func(name string, args ...string) ([]byte, error)) func() {
	origExec := execCommand
	origStatus := statusCheckCommand
	if runner != nil {
		execCommand = runner
	}
	if len(statusChecker) > 0 && statusChecker[0] != nil {
		statusCheckCommand = statusChecker[0]
	} else if runner != nil {
		statusCheckCommand = func(name string, args ...string) ([]byte, error) {
			err := runner(name, args...)
			return nil, err
		}
	}
	return func() {
		execCommand = origExec
		statusCheckCommand = origStatus
	}
}

// SetCrontabCommandForTest allows mocking crontab subcommands in tests.
func SetCrontabCommandForTest(list func() ([]byte, error), apply func(string) error, remove func() error) func() {
	origList := crontabListCommand
	origApply := crontabApplyCommand
	origRemove := crontabRemoveCommand
	if list != nil {
		crontabListCommand = list
	}
	if apply != nil {
		crontabApplyCommand = apply
	}
	if remove != nil {
		crontabRemoveCommand = remove
	}
	return func() {
		crontabListCommand = origList
		crontabApplyCommand = origApply
		crontabRemoveCommand = origRemove
	}
}

// cleanStaleBackups purges backup files matching .old. or .tmp in dir.
func cleanStaleBackups(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.Contains(name, ".old.") || strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
}

// RestartToolService restarts the specified tool's daemon service across Darwin (launchd),
// Linux (systemd --user), or Windows (schtasks).
// Per ADR-002, iazio-harness is strictly an on-demand CLI and must never be restarted as an OS service.
func RestartToolService(toolName string, runner ...func(string, ...string) error) error {
	if toolName == "iazio-harness" {
		return nil
	}
	run := execCommand
	if len(runner) > 0 && runner[0] != nil {
		run = runner[0]
	}

	label := "io.iazio." + toolName
	if toolName == "autopilot" {
		label = "io.autopilot"
	}

	switch runtime.GOOS {
	case "darwin":
		target := fmt.Sprintf("gui/%d/%s", os.Getuid(), label)
		return run("launchctl", "kickstart", "-k", target)
	case "linux":
		if IsSystemdAvailable() {
			return run("systemctl", "--user", "restart", toolName+".service")
		}
	case "windows":
		_ = run("schtasks", "/End", "/TN", toolName)
		return run("schtasks", "/Run", "/TN", toolName)
	}
	return nil
}
