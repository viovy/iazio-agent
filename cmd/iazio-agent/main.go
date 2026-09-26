// Command iazio-agent is the user-level runner supervisor.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/viovy/iazio-agent/internal/auth"
	"github.com/viovy/iazio-agent/internal/controlplane"
	"github.com/viovy/iazio-agent/internal/service"
	"github.com/viovy/iazio-agent/internal/supervisor"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := execute(ctx, os.Args[1:], os.Stdout, os.Stderr, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, getenv func(string) string) error {
	if getenv == nil {
		getenv = os.Getenv
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: iazio-agent version|auth|service|update|run")
	}
	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, formatVersion())
		return nil
	case "auth":
		return runAuth(ctx, args[1:], stdout, getenv)
	case "service":
		return runService(args[1:], stdout, getenv)
	case "update":
		return runUpdate(args[1:], stdout)
	case "run":
		fmt.Fprintln(stdout, supervisor.HostKind(getenv("IAZIO_AGENT_HOST_KIND")))
		if err := dialControlPlane(ctx, getenv); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		sup := supervisor.New(time.Now)
		_ = sup.Lock("run")
		<-ctx.Done()
		return nil
	default:
		return fmt.Errorf("unknown command %s", args[0])
	}
}

func dialControlPlane(ctx context.Context, getenv func(string) string) error {
	api := getenv("IAZIO_HARNESS_API_URL")
	host := getenv("IAZIO_AGENT_HOST_ID")
	if api == "" || host == "" {
		return nil
	}
	client := controlplane.Client{BaseURL: api}
	kind := supervisor.HostKind(getenv("IAZIO_AGENT_HOST_KIND"))
	if err := client.Register(ctx, host, kind); err != nil {
		return err
	}
	return client.Heartbeat(ctx, host, nil, false)
}

func runAuth(ctx context.Context, args []string, stdout io.Writer, getenv func(string) string) error {
	if len(args) == 0 {
		return fmt.Errorf("auth status|login")
	}
	switch args[0] {
	case "status":
		st, err := auth.ReadStatus(auth.ConfigPathFrom("", getenv))
		if err != nil {
			return err
		}
		fmt.Fprintln(stdout, auth.FormatStatus(st))
		return nil
	case "login":
		if auth.Noninteractive(getenv) {
			return fmt.Errorf("auth login refused: noninteractive session")
		}
		return auth.Login(ctx, auth.LoginOptions{Getenv: getenv, Stdout: stdout})
	default:
		return fmt.Errorf("unknown auth command")
	}
}

func runService(args []string, stdout io.Writer, getenv func(string) string) error {
	action, dry, force, err := parseService(args)
	if err != nil {
		return err
	}
	goos := runtime.GOOS
	if v := getenv("GOOS_OVERRIDE"); v != "" {
		goos = v
	}
	text := service.ActionText(goos, action, "iazio-agent", force)
	fmt.Fprintln(stdout, text)
	if dry {
		return nil
	}
	_, err = service.Apply(goos, action, "iazio-agent", false, force, nil)
	return err
}

func runUpdate(args []string, stdout io.Writer) error {
	suite := false
	onTimeout := "reject"
	expired := false
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--suite":
			suite = true
		case args[i] == "--on-timeout" && i+1 < len(args):
			i++
			onTimeout = args[i]
		case strings.HasPrefix(args[i], "--on-timeout="):
			onTimeout = strings.TrimPrefix(args[i], "--on-timeout=")
		case args[i] == "--drain-expired":
			expired = true
		default:
			return fmt.Errorf("unknown update flag %s", args[i])
		}
	}
	if suite {
		if !service.SuiteAllowed(false, onTimeout, expired) {
			return service.ErrSuiteBusy
		}
		fmt.Fprintln(stdout, "update suite")
		return nil
	}
	fmt.Fprintln(stdout, "update")
	return nil
}

func parseService(args []string) (action string, dry, force bool, err error) {
	if len(args) == 0 {
		return "", false, false, fmt.Errorf("service install|uninstall|start|stop|restart|status")
	}
	action = args[0]
	switch action {
	case "install", "uninstall", "start", "stop", "restart", "status":
	default:
		return "", false, false, fmt.Errorf("unknown service action %s", action)
	}
	for _, arg := range args[1:] {
		switch arg {
		case "--dry-run":
			dry = true
		case "--force":
			force = true
		default:
			return "", false, false, fmt.Errorf("unknown service flag %s", arg)
		}
	}
	return action, dry, force, nil
}
