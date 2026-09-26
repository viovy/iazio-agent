// Command iazio-agent is the user-level runner supervisor.
package main

import (
	"fmt"
	"os"

	"github.com/viovy/iazio-agent/internal/auth"
	"github.com/viovy/iazio-agent/internal/service"
	"github.com/viovy/iazio-agent/internal/supervisor"
)

var (
	version = "0.1.0-dev"
	commit  = "unknown"
	branch  = "unknown"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: iazio-agent version|auth|service|update|run")
	}
	switch args[0] {
	case "version":
		fmt.Println(formatVersion())
		return nil
	case "auth":
		return runAuth(args[1:])
	case "service":
		fmt.Println(service.UnitText(os.Getenv("GOOS_OVERRIDE"), "iazio-agent"))
		return nil
	case "update":
		return nil
	case "run":
		fmt.Println(supervisor.HostKind(os.Getenv("IAZIO_AGENT_HOST_KIND")))
		return nil
	default:
		return fmt.Errorf("unknown command %s", args[0])
	}
}

func formatVersion() string {
	if commit == "" || commit == "unknown" {
		return "iazio-agent " + version
	}
	sha := commit
	if len(sha) > 12 {
		sha = sha[:12]
	}
	return "iazio-agent " + version + "+" + sha
}

func runAuth(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("auth status|login")
	}
	st, err := auth.ReadStatus(auth.ConfigPath())
	if err != nil {
		return err
	}
	switch args[0] {
	case "status":
		fmt.Println(auth.FormatStatus(st))
		return nil
	case "login":
		noninteractive := os.Getenv("IAZIO_AGENT_NONINTERACTIVE") == "1" || os.Getenv("INVOCATION_ID") != ""
		return auth.LoginAllowed(noninteractive, st.HasRefreshToken)
	default:
		return fmt.Errorf("unknown auth command")
	}
}
