// Package service manages platform-specific background daemon life cycles,
// including the Windows-to-WSL companion supervisor.
package service

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// WSLServiceStatus describes the operational state of WSL services supervised from Windows native.
type WSLServiceStatus struct {
	WSLPresent      bool   `json:"wsl_present"`
	Distro          string `json:"distro,omitempty"`
	SystemdActive   bool   `json:"systemd_active"`
	AgentActive     bool   `json:"agent_active"`
	HarvesterActive bool   `json:"harvester_active"`
	LingerActive    bool   `json:"linger_active"`
	AutoHealed      bool   `json:"auto_healed"`
	LastCheckError  string `json:"last_check_error,omitempty"`
}

// WSLCompanionSupervisor supervises and automatically heals WSL systemd user services from the Windows native host.
type WSLCompanionSupervisor struct {
	distro string
	runner func(name string, args ...string) ([]byte, error)
}

// NewWSLCompanionSupervisor creates a supervisor for WSL companion daemons.
func NewWSLCompanionSupervisor(distro string, runner ...func(name string, args ...string) ([]byte, error)) *WSLCompanionSupervisor {
	r := defaultExecRunner
	if len(runner) > 0 && runner[0] != nil {
		r = runner[0]
	}
	return &WSLCompanionSupervisor{
		distro: strings.TrimSpace(distro),
		runner: r,
	}
}

func defaultExecRunner(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

// CheckAndHeal inspects WSL2 liveness, user systemd state, iazio-agent, and iazio-harvester services.
// If inactive or missing, it automatically installs, enables linger, and starts them.
func (s *WSLCompanionSupervisor) CheckAndHeal(ctx context.Context) WSLServiceStatus {
	status := WSLServiceStatus{
		Distro: s.distro,
	}

	// 1. Verify WSL availability
	out, err := s.runner("wsl.exe", "-l", "-q")
	if err != nil {
		status.LastCheckError = fmt.Sprintf("probe wsl.exe: %v", err)
		return status
	}
	raw := strings.ReplaceAll(string(out), "\x00", "")
	lines := strings.Split(raw, "\n")
	var distros []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed != "" {
			distros = append(distros, trimmed)
		}
	}
	if len(distros) == 0 {
		status.LastCheckError = "no WSL distributions registered"
		return status
	}
	status.WSLPresent = true
	if status.Distro == "" {
		status.Distro = distros[0]
	}

	wslExec := func(cmdArgs ...string) ([]byte, error) {
		args := []string{}
		if status.Distro != "" {
			args = append(args, "-d", status.Distro)
		}
		args = append(args, "-e")
		args = append(args, cmdArgs...)
		return s.runner("wsl.exe", args...)
	}

	// 2. Check systemd active in WSL
	sdOut, sdErr := wslExec("systemctl", "is-system-running")
	sdStr := strings.TrimSpace(string(sdOut))
	if sdErr == nil || sdStr == "running" || sdStr == "degraded" {
		status.SystemdActive = true
	} else {
		// Attempt fallback check
		if _, statErr := wslExec("test", "-d", "/run/systemd/system"); statErr == nil {
			status.SystemdActive = true
		}
	}

	// 3. Keepalive ping to ensure WSL VM stays running
	_, _ = wslExec("true")

	// 4. Inspect iazio-agent and iazio-harvester user service status
	agentOut, _ := wslExec("systemctl", "--user", "is-active", "iazio-agent.service")
	if strings.TrimSpace(string(agentOut)) == "active" {
		status.AgentActive = true
	}

	harvesterOut, _ := wslExec("systemctl", "--user", "is-active", "iazio-harvester.service")
	if strings.TrimSpace(string(harvesterOut)) == "active" {
		status.HarvesterActive = true
	}

	// 5. Inspect user linger status
	lingerOut, _ := wslExec("loginctl", "show-user", "-p", "Linger")
	if strings.Contains(strings.ToLower(string(lingerOut)), "linger=yes") {
		status.LingerActive = true
	}

	// 6. Heal if services are inactive or linger is disabled
	needsHealing := !status.AgentActive || !status.HarvesterActive || !status.LingerActive
	if needsHealing && status.SystemdActive {
		// Ensure linger is active so user systemd survives logout/reboot
		_, _ = wslExec("loginctl", "enable-linger")

		// Re-install or enable iazio-agent service if inactive
		if !status.AgentActive {
			_, _ = wslExec("iazio-agent", "service", "install")
			_, _ = wslExec("systemctl", "--user", "enable", "--now", "iazio-agent.service")
			chk, _ := wslExec("systemctl", "--user", "is-active", "iazio-agent.service")
			if strings.TrimSpace(string(chk)) == "active" {
				status.AgentActive = true
				status.AutoHealed = true
			}
		}

		// Re-install or enable iazio-harvester service if inactive
		if !status.HarvesterActive {
			_, _ = wslExec("iazio-harvester", "service", "install")
			_, _ = wslExec("systemctl", "--user", "enable", "--now", "iazio-harvester.service")
			chk, _ := wslExec("systemctl", "--user", "is-active", "iazio-harvester.service")
			if strings.TrimSpace(string(chk)) == "active" {
				status.HarvesterActive = true
				status.AutoHealed = true
			}
		}
	}

	return status
}

// StartSupervisor begins periodic supervision in the background.
func (s *WSLCompanionSupervisor) StartSupervisor(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	go func() {
		// Initial check
		_ = s.CheckAndHeal(ctx)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = s.CheckAndHeal(ctx)
			}
		}
	}()
}
