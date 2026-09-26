// Package service renders the user-level unit for the agent.
package service

import (
	"errors"
	"fmt"
)

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
			return fmt.Sprintf(`<?xml version="1.0"?><plist><key>Label</key><string>iazio-agent</string><key>ProgramArguments</key><array><string>%s</string><string>run</string></array><key>RunAtLoad</key><true/><key>EnvironmentVariables</key><dict><key>IAZIO_AGENT_NONINTERACTIVE</key><string>1</string></dict></plist>`, bin)
		case "windows":
			flag := ""
			if force {
				flag = " /F"
			}
			return fmt.Sprintf(`schtasks /Create /TN iazio-agent /SC ONLOGON%s /TR "cmd /C set IAZIO_AGENT_NONINTERACTIVE=1&& \"%s\" run"`, flag, bin)
		default:
			return fmt.Sprintf("[Service]\nExecStart=%s run\nEnvironment=IAZIO_AGENT_NONINTERACTIVE=1\n", bin)
		}
	}
	switch goos {
	case "darwin":
		return fmt.Sprintf("launchctl %s gui/iazio-agent", action)
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
