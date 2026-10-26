//go:build !windows
// +build !windows

package supervisor

import (
	"os/exec"
	"testing"
)

func TestPostStartProc(t *testing.T) {
	cmd := exec.Command("echo", "test")
	setProcGroup(cmd)
	postStartProc(cmd)
	if cmd.SysProcAttr == nil || (!cmd.SysProcAttr.Setsid && !cmd.SysProcAttr.Setpgid) {
		t.Fatalf("expected Setsid=true or Setpgid=true, got %+v", cmd.SysProcAttr)
	}
}
