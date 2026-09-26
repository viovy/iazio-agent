// Package service renders the user-level unit for the agent.
package service

import "fmt"

// UnitText returns the launchd, systemd, or scheduler text for a dry run.
func UnitText(goos, bin string) string {
	switch goos {
	case "darwin":
		return fmt.Sprintf(`<?xml version="1.0"?><plist><key>Label</key><string>iazio-agent</string><key>ProgramArguments</key><array><string>%s</string><string>run</string></array><key>RunAtLoad</key><true/><key>EnvironmentVariables</key><dict><key>IAZIO_AGENT_NONINTERACTIVE</key><string>1</string></dict></plist>`, bin)
	case "windows":
		return fmt.Sprintf(`schtasks /Create /TN iazio-agent /TR "%s run"`, bin)
	default:
		return fmt.Sprintf("[Service]\nExecStart=%s run\nEnvironment=IAZIO_AGENT_NONINTERACTIVE=1\n", bin)
	}
}

// SuiteAllowed reports whether update --suite may start.
func SuiteAllowed(running bool, onTimeout string, expired bool) bool {
	if !running {
		return true
	}
	return onTimeout == "abort" && expired
}
