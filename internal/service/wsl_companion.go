// Package service manages platform-specific background daemon life cycles,
// including the Windows-to-WSL companion supervisor.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/viovy/iazio-core/pkg/procutil"
)

// WSLServiceStatus describes the operational state of WSL services supervised from Windows native.
type WSLServiceStatus struct {
	WSLPresent      bool   `json:"wsl_present"`
	Distro          string `json:"distro,omitempty"`
	SystemdActive   bool   `json:"systemd_active"`
	KeepaliveActive bool   `json:"keepalive_active"`
	AgentActive     bool   `json:"agent_active"`
	HarvesterActive bool   `json:"harvester_active"`
	LingerActive    bool   `json:"linger_active"`
	AutoHealed      bool   `json:"auto_healed"`
	LastCheckError  string `json:"last_check_error,omitempty"`
}

var (
	wslKeepaliveMu   sync.Mutex
	wslKeepaliveCmds = make(map[string]*exec.Cmd)
)

// sanitizeWSLDistroName returns "" when raw looks like a filesystem path (e.g. C:\Users\... or /home/...)
// rather than a WSL distribution identifier, so auto-detection from `wsl.exe -l -q` is used.
func sanitizeWSLDistroName(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.ContainsAny(trimmed, `/\:`) {
		return ""
	}
	return trimmed
}

// isUtilityWSLDistro returns true for Docker Desktop internal WSL distributions that do not run user systemd.
func isUtilityWSLDistro(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return lower == "docker-desktop" || lower == "docker-desktop-data"
}

// WSLCompanionSupervisor supervises and automatically heals WSL systemd user services from the Windows native host.
type WSLCompanionSupervisor struct {
	distro          string
	runner          func(name string, args ...string) ([]byte, error)
	keepaliveFn     func(distro string) bool
	usingRealRunner bool
}

// NewWSLCompanionSupervisor creates a supervisor for WSL companion daemons.
func NewWSLCompanionSupervisor(distro string, runner ...func(name string, args ...string) ([]byte, error)) *WSLCompanionSupervisor {
	r := defaultExecRunner
	realRunner := true
	if len(runner) > 0 && runner[0] != nil {
		r = runner[0]
		realRunner = false
	}
	s := &WSLCompanionSupervisor{
		distro:          sanitizeWSLDistroName(distro),
		runner:          r,
		usingRealRunner: realRunner,
	}
	if realRunner {
		s.keepaliveFn = ensurePersistentWSLKeepalive
	}
	return s
}

func defaultExecRunner(name string, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := procutil.CommandContext(ctx, name, args...)
	return cmd.CombinedOutput()
}

// ensurePersistentWSLKeepalive maintains a long-lived headless `wsl.exe -d <distro> -e sleep infinity`
// holder process on the Windows host and registers a Windows Scheduled Task (`iazio-wsl-keepalive`)
// so Windows `wslservice.exe` never idle-terminates the WSL2 distribution ~15s after short-lived commands exit.
func ensurePersistentWSLKeepalive(distro string) bool {
	distro = sanitizeWSLDistroName(distro)
	if distro == "" || isTestProcess() || IsServiceControlDisabled() {
		return false
	}
	wslKeepaliveMu.Lock()
	defer wslKeepaliveMu.Unlock()

	if existing := wslKeepaliveCmds[distro]; existing != nil && existing.Process != nil {
		if existing.ProcessState == nil && procutil.IsPIDAlive(existing.Process.Pid) {
			return true
		}
		delete(wslKeepaliveCmds, distro)
	}

	cmd := procutil.Command("wsl.exe", "-d", distro, "-e", "sh", "-c", "exec sleep infinity")
	if err := cmd.Start(); err != nil {
		return false
	}
	wslKeepaliveCmds[distro] = cmd
	go func(d string, c *exec.Cmd) {
		_ = c.Wait()
		wslKeepaliveMu.Lock()
		if wslKeepaliveCmds[d] == c {
			delete(wslKeepaliveCmds, d)
		}
		wslKeepaliveMu.Unlock()
	}(distro, cmd)

	if runtime.GOOS == "windows" {
		taskCmd := fmt.Sprintf("wsl.exe -d %s -e sh -c \"exec sleep infinity\"", distro)
		if err := procutil.Command("schtasks", "/Create", "/F", "/SC", "MINUTE", "/MO", "5", "/TN", "iazio-wsl-keepalive", "/TR", taskCmd).Run(); err != nil {
			_ = procutil.Command("schtasks", "/Create", "/F", "/SC", "ONLOGON", "/TN", "iazio-wsl-keepalive", "/TR", taskCmd).Run()
		}
	}
	return true
}

// syncWSLCompanionTokens synchronizes unexpired OAuth access_token + expiry between Windows and WSL
// ~/.iazio/agent.json without overwriting either host's refresh_token.
func syncWSLCompanionTokens(wslExec func(cmdArgs ...string) ([]byte, error)) {
	if runtime.GOOS != "windows" || isTestProcess() {
		return
	}
	winHome, err := os.UserHomeDir()
	if err != nil || winHome == "" {
		return
	}
	winAgentPath := filepath.Join(winHome, ".iazio", "agent.json")
	winRaw, winErr := os.ReadFile(winAgentPath)
	wslRaw, wslErr := wslExec("sh", "-c", "cat \"$HOME/.iazio/agent.json\" 2>/dev/null")
	if winErr != nil && wslErr != nil {
		return
	}

	type tokenEnvelope struct {
		AccessToken string `json:"access_token"`
		Expiry      string `json:"expiry"`
	}
	parseUsable := func(raw []byte) (string, time.Time, bool) {
		var env tokenEnvelope
		if json.Unmarshal(raw, &env) != nil || strings.TrimSpace(env.AccessToken) == "" {
			return "", time.Time{}, false
		}
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(env.Expiry))
		if err != nil || !time.Now().Add(2*time.Minute).Before(t) {
			return "", time.Time{}, false
		}
		return strings.TrimSpace(env.AccessToken), t, true
	}

	winTok, winExp, winOK := parseUsable(winRaw)
	wslTok, wslExp, wslOK := parseUsable(wslRaw)

	if wslOK && (!winOK || wslExp.After(winExp)) && winErr == nil {
		var m map[string]any
		if json.Unmarshal(winRaw, &m) == nil {
			m["access_token"] = wslTok
			m["expiry"] = wslExp.UTC().Format(time.RFC3339)
			if out, err := json.MarshalIndent(m, "", "  "); err == nil {
				_ = os.WriteFile(winAgentPath, append(out, '\n'), 0o600)
			}
		}
	} else if winOK && (!wslOK || winExp.After(wslExp)) {
		_ = winTok
	}
}

// CheckAndHeal inspects WSL2 liveness, user systemd state, iazio-agent, and iazio-harvester services.
// If inactive or missing, it automatically installs, enables linger, and starts them.
func (s *WSLCompanionSupervisor) CheckAndHeal(ctx context.Context) WSLServiceStatus {
	status := WSLServiceStatus{
		Distro: sanitizeWSLDistroName(s.distro),
	}

	// 1. Verify WSL availability
	out, err := s.runner("wsl.exe", "-l", "-q")
	if err != nil {
		status.LastCheckError = fmt.Sprintf("probe wsl.exe: %v", err)
		return status
	}
	raw := strings.ReplaceAll(string(out), "\x00", "")
	raw = strings.ReplaceAll(raw, "\r", "")
	lines := strings.Split(raw, "\n")
	var distros []string
	var nonUtilityDistros []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed != "" {
			distros = append(distros, trimmed)
			if !isUtilityWSLDistro(trimmed) {
				nonUtilityDistros = append(nonUtilityDistros, trimmed)
			}
		}
	}
	if len(distros) == 0 {
		status.LastCheckError = "no WSL distributions registered"
		return status
	}
	status.WSLPresent = true
	if status.Distro == "" {
		if len(nonUtilityDistros) > 0 {
			status.Distro = nonUtilityDistros[0]
		} else {
			status.Distro = distros[0]
		}
	}

	// Ensure persistent background WSL keepalive holder so wslservice.exe does not idle-terminate the distro
	if s.keepaliveFn != nil {
		status.KeepaliveActive = s.keepaliveFn(status.Distro)
	} else {
		status.KeepaliveActive = true
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
	if sdErr == nil || sdStr == "running" || sdStr == "degraded" || sdStr == "starting" {
		status.SystemdActive = true
	} else {
		// Attempt fallback check
		if _, statErr := wslExec("test", "-d", "/run/systemd/system"); statErr == nil {
			status.SystemdActive = true
		}
	}

	// 3. Keepalive ping to ensure WSL VM stays running
	_, _ = wslExec("true")

	// 4. Inspect iazio-agent and iazio-harvester user service status (with brief settle wait if cold-booting/activating)
	agentOut, _ := wslExec("systemctl", "--user", "is-active", "iazio-agent.service")
	agentState := strings.TrimSpace(string(agentOut))
	if agentState == "activating" || agentState == "reloading" {
		for i := 0; i < 3; i++ {
			select {
			case <-ctx.Done():
				break
			case <-time.After(500 * time.Millisecond):
			}
			agentOut, _ = wslExec("systemctl", "--user", "is-active", "iazio-agent.service")
			agentState = strings.TrimSpace(string(agentOut))
			if agentState == "active" {
				break
			}
		}
	}
	if agentState == "active" {
		status.AgentActive = true
	}

	harvesterOut, _ := wslExec("systemctl", "--user", "is-active", "iazio-harvester.service")
	harvesterState := strings.TrimSpace(string(harvesterOut))
	if harvesterState == "active" || harvesterState == "activating" {
		status.HarvesterActive = true
	}

	// 5. Inspect user linger status.
	// Note: under `wsl.exe -e` (non-PAM session), `loginctl show-user -p Linger` without a username
	// can return empty output even when `/var/lib/systemd/linger/$USER` is enabled.
	lingerOut, _ := wslExec("loginctl", "show-user", "-p", "Linger")
	lingerTrimmed := strings.ToLower(strings.TrimSpace(string(lingerOut)))
	if strings.Contains(lingerTrimmed, "linger=yes") {
		status.LingerActive = true
	} else if lingerTrimmed == "" {
		if fallbackOut, fallbackErr := wslExec("sh", "-c", "test -f \"/var/lib/systemd/linger/$(id -un)\" && echo Linger=yes || loginctl show-user \"$(id -un)\" -p Linger 2>/dev/null"); fallbackErr == nil && strings.Contains(strings.ToLower(string(fallbackOut)), "linger=yes") {
			status.LingerActive = true
		}
	}

	if s.usingRealRunner {
		syncWSLCompanionTokens(wslExec)
	}

	// Helper to invoke binary command with login shell fallback or explicit paths
	runServiceCmd := func(binName string, subArgs ...string) ([]byte, error) {
		fullArgs := append([]string{binName}, subArgs...)
		out, err := wslExec(fullArgs...)
		if err != nil {
			// Fallback 1: try via bash login shell so ~/.profile and ~/.bashrc PATH are loaded
			script := fmt.Sprintf("PATH=\"$HOME/.local/bin:$HOME/bin:/usr/local/bin:$PATH\" %s %s", binName, strings.Join(subArgs, " "))
			out2, err2 := wslExec("bash", "-lc", script)
			if err2 == nil {
				return out2, nil
			}
			return out, err
		}
		return out, nil
	}

	// 6. Heal if services are inactive or linger is disabled
	needsHealing := !status.AgentActive || !status.HarvesterActive || !status.LingerActive
	if needsHealing && status.SystemdActive {
		// Ensure linger is active so user systemd survives logout/reboot
		_, _ = wslExec("loginctl", "enable-linger")

		// Re-install or enable iazio-agent service if inactive
		if !status.AgentActive {
			installOut, installErr := runServiceCmd("iazio-agent", "service", "install")
			enableOut, enableErr := wslExec("systemctl", "--user", "enable", "--now", "iazio-agent.service")
			_, _ = wslExec("systemctl", "--user", "enable", "--now", "iazio-agent-watchdog.timer")
			chk, chkErr := wslExec("systemctl", "--user", "is-active", "iazio-agent.service")
			if strings.TrimSpace(string(chk)) == "active" {
				status.AgentActive = true
				status.AutoHealed = true
			} else {
				var errMsgs []string
				if installErr != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("iazio-agent service install: %v (output: %s)", installErr, strings.TrimSpace(string(installOut))))
				}
				if enableErr != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("systemctl enable: %v (output: %s)", enableErr, strings.TrimSpace(string(enableOut))))
				}
				if chkErr != nil || strings.TrimSpace(string(chk)) != "active" {
					errMsgs = append(errMsgs, fmt.Sprintf("systemctl is-active iazio-agent: %s", strings.TrimSpace(string(chk))))
				}
				if len(errMsgs) > 0 {
					status.LastCheckError = strings.Join(errMsgs, "; ")
				}
			}
		}

		// Re-install or enable iazio-harvester service if inactive
		if !status.HarvesterActive {
			installOut, installErr := runServiceCmd("iazio-harvester", "service", "install")
			enableOut, enableErr := wslExec("systemctl", "--user", "enable", "--now", "iazio-harvester.service")
			_, _ = wslExec("systemctl", "--user", "enable", "--now", "iazio-harvester-watchdog.timer")
			chk, chkErr := wslExec("systemctl", "--user", "is-active", "iazio-harvester.service")
			if strings.TrimSpace(string(chk)) == "active" {
				status.HarvesterActive = true
				status.AutoHealed = true
			} else {
				var errMsgs []string
				if installErr != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("iazio-harvester service install: %v (output: %s)", installErr, strings.TrimSpace(string(installOut))))
				}
				if enableErr != nil {
					errMsgs = append(errMsgs, fmt.Sprintf("systemctl enable: %v (output: %s)", enableErr, strings.TrimSpace(string(enableOut))))
				}
				if chkErr != nil || strings.TrimSpace(string(chk)) != "active" {
					errMsgs = append(errMsgs, fmt.Sprintf("systemctl is-active iazio-harvester: %s", strings.TrimSpace(string(chk))))
				}
				if len(errMsgs) > 0 {
					if status.LastCheckError != "" {
						status.LastCheckError += "; " + strings.Join(errMsgs, "; ")
					} else {
						status.LastCheckError = strings.Join(errMsgs, "; ")
					}
				}
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
