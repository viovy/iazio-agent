package service

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestWSLCompanionSupervisor_CheckAndHeal(t *testing.T) {
	commandsRun := make([]string, 0)
	mockRunner := func(name string, args ...string) ([]byte, error) {
		fullCmd := name + " " + strings.Join(args, " ")
		commandsRun = append(commandsRun, fullCmd)

		if strings.Contains(fullCmd, "-l -q") {
			return []byte("Ubuntu\nDebian\n"), nil
		}
		if strings.Contains(fullCmd, "is-system-running") {
			return []byte("running\n"), nil
		}
		if strings.Contains(fullCmd, "is-active iazio-agent.service") {
			// Initially inactive, becomes active after enable
			for _, cmd := range commandsRun {
				if strings.Contains(cmd, "enable --now iazio-agent.service") {
					return []byte("active\n"), nil
				}
			}
			return []byte("inactive\n"), nil
		}
		if strings.Contains(fullCmd, "is-active iazio-harvester.service") {
			for _, cmd := range commandsRun {
				if strings.Contains(cmd, "enable --now iazio-harvester.service") {
					return []byte("active\n"), nil
				}
			}
			return []byte("inactive\n"), nil
		}
		if strings.Contains(fullCmd, "show-user -p Linger") {
			return []byte("Linger=no\n"), nil
		}
		return []byte("ok\n"), nil
	}

	supervisor := NewWSLCompanionSupervisor("Ubuntu", mockRunner)
	ctx := context.Background()
	status := supervisor.CheckAndHeal(ctx)

	if !status.WSLPresent {
		t.Fatalf("expected WSLPresent true")
	}
	if status.Distro != "Ubuntu" {
		t.Fatalf("expected distro Ubuntu, got %s", status.Distro)
	}
	if !status.SystemdActive {
		t.Fatalf("expected SystemdActive true")
	}
	if !status.AutoHealed {
		t.Fatalf("expected AutoHealed true")
	}
	if !status.AgentActive {
		t.Fatalf("expected AgentActive true after healing")
	}
	if !status.HarvesterActive {
		t.Fatalf("expected HarvesterActive true after healing")
	}
}

func TestWSLCompanionSupervisor_NoWSL(t *testing.T) {
	mockRunner := func(name string, args ...string) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}

	supervisor := NewWSLCompanionSupervisor("", mockRunner)
	status := supervisor.CheckAndHeal(context.Background())

	if status.WSLPresent {
		t.Fatalf("expected WSLPresent false")
	}
	if status.LastCheckError == "" {
		t.Fatalf("expected LastCheckError populated")
	}
}

func TestWSLCompanionSupervisor_HealingFailure_Diagnostics(t *testing.T) {
	mockRunner := func(name string, args ...string) ([]byte, error) {
		fullCmd := name + " " + strings.Join(args, " ")
		if strings.Contains(fullCmd, "-l -q") {
			return []byte("Ubuntu\n"), nil
		}
		if strings.Contains(fullCmd, "is-system-running") {
			return []byte("running\n"), nil
		}
		if strings.Contains(fullCmd, "is-active") {
			return []byte("failed\n"), nil
		}
		if strings.Contains(fullCmd, "service install") {
			return []byte("command not found: iazio-agent\n"), context.DeadlineExceeded
		}
		return []byte("ok\n"), nil
	}

	supervisor := NewWSLCompanionSupervisor("Ubuntu", mockRunner)
	status := supervisor.CheckAndHeal(context.Background())

	if !status.WSLPresent {
		t.Fatalf("expected WSLPresent true")
	}
	if status.AgentActive {
		t.Fatalf("expected AgentActive false")
	}
	if !strings.Contains(status.LastCheckError, "service install") {
		t.Fatalf("expected LastCheckError to contain diagnostic info, got: %s", status.LastCheckError)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	supervisor.StartSupervisor(ctx, 5*time.Millisecond)
	<-ctx.Done()
	time.Sleep(10 * time.Millisecond)

	sDefault := NewWSLCompanionSupervisor("Ubuntu", nil)
	_, _ = sDefault.runner("true")
}

func TestWSLCompanionSupervisor_PathSanitizationAndLingerFallback(t *testing.T) {
	pollCount := 0
	mockRunner := func(name string, args ...string) ([]byte, error) {
		fullCmd := name + " " + strings.Join(args, " ")
		if strings.Contains(fullCmd, "-l -q") {
			return []byte("docker-desktop\r\nDebian\r\n"), nil
		}
		if strings.Contains(fullCmd, "is-system-running") {
			return []byte("starting\n"), nil
		}
		if strings.Contains(fullCmd, "is-active iazio-agent.service") {
			pollCount++
			if pollCount == 1 {
				return []byte("activating\n"), nil
			}
			return []byte("active\n"), nil
		}
		if strings.Contains(fullCmd, "is-active iazio-harvester.service") {
			return []byte("activating\n"), nil
		}
		if strings.Contains(fullCmd, "loginctl show-user -p Linger") {
			// Simulate wsl.exe -e non-PAM empty output
			return []byte(""), nil
		}
		if strings.Contains(fullCmd, "/var/lib/systemd/linger/") {
			return []byte("Linger=yes\n"), nil
		}
		return []byte("ok\n"), nil
	}

	// Pass a Windows home path to verify sanitizeWSLDistroName strips paths and selects Debian over docker-desktop
	sup := NewWSLCompanionSupervisor(`C:\Users\romeo`, mockRunner)
	keepaliveCalled := ""
	sup.keepaliveFn = func(distro string) bool {
		keepaliveCalled = distro
		return true
	}

	status := sup.CheckAndHeal(context.Background())
	if status.Distro != "Debian" {
		t.Fatalf("expected sanitized distro to resolve to Debian (skipping docker-desktop), got %q", status.Distro)
	}
	if keepaliveCalled != "Debian" || !status.KeepaliveActive {
		t.Fatalf("expected keepaliveFn called for Debian, got %q active=%v", keepaliveCalled, status.KeepaliveActive)
	}
	if !status.AgentActive {
		t.Fatalf("expected AgentActive true after activating -> active poll")
	}
	if !status.HarvesterActive {
		t.Fatalf("expected HarvesterActive true when activating")
	}
	if !status.LingerActive {
		t.Fatalf("expected LingerActive true via /var/lib/systemd/linger fallback")
	}
	if status.AutoHealed {
		t.Fatalf("expected AutoHealed false when services were already active/activating and linger present")
	}
}


