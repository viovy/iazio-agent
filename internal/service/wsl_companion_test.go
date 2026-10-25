package service

import (
	"context"
	"strings"
	"testing"
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
}
