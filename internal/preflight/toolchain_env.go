package preflight

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// HydrateToolchainEnv ensures system and user toolchain directories are present in the
// current process's PATH environment variable, ensuring git, go, make, npm, codesign, etc.
// are resolvable under daemon/service runners (launchd, systemd --user, Task Scheduler).
func HydrateToolchainEnv() string {
	_ = os.Setenv("GIT_TERMINAL_PROMPT", "0")
	return HydrateToolchainEnvWith(runtime.GOOS, os.Getenv("PATH"), os.UserHomeDir, os.Getenv)
}

// HydrateToolchainEnvWith is the testable implementation of HydrateToolchainEnv.
func HydrateToolchainEnvWith(
	goos string,
	currentPath string,
	homeDirFn func() (string, error),
	getenvFn func(string) string,
) string {
	home := ""
	if homeDirFn != nil {
		home, _ = homeDirFn()
	}
	if getenvFn == nil {
		getenvFn = os.Getenv
	}

	join := func(parts ...string) string {
		if goos == "windows" {
			var nonEmp []string
			for _, p := range parts {
				p = strings.ReplaceAll(p, "/", "\\")
				p = strings.Trim(p, "\\")
				if p != "" {
					nonEmp = append(nonEmp, p)
				}
			}
			res := strings.Join(nonEmp, "\\")
			if len(parts) > 0 && strings.HasPrefix(parts[0], "\\\\") {
				res = "\\\\" + res
			}
			return res
		}
		return filepath.Join(parts...)
	}

	var prependDirs []string

	switch goos {
	case "darwin":
		if home != "" {
			prependDirs = append(prependDirs,
				join(home, ".local", "bin"),
				join(home, "bin"),
				join(home, "go", "bin"),
			)
		}
		prependDirs = append(prependDirs,
			"/opt/homebrew/bin",
			"/opt/homebrew/sbin",
			"/usr/local/go/bin",
			"/usr/local/bin",
		)

	case "windows":
		userProfile := getenvFn("USERPROFILE")
		if userProfile == "" {
			userProfile = home
		}
		localAppData := getenvFn("LOCALAPPDATA")
		appData := getenvFn("APPDATA")

		if localAppData != "" {
			prependDirs = append(prependDirs, join(localAppData, "Programs", "iazio", "bin"))
		}
		if userProfile != "" {
			prependDirs = append(prependDirs,
				join(userProfile, "bin"),
				join(userProfile, ".local", "bin"),
				join(userProfile, "go", "bin"),
			)
		}
		if appData != "" {
			prependDirs = append(prependDirs, join(appData, "npm"))
		}
		prependDirs = append(prependDirs,
			`C:\Program Files\Go\bin`,
			`C:\Program Files\Git\cmd`,
			`C:\Program Files\Git\bin`,
			`C:\Program Files\nodejs`,
		)

	default: // linux / wsl
		if home != "" {
			prependDirs = append(prependDirs,
				join(home, ".local", "bin"),
				join(home, "bin"),
				join(home, "go", "bin"),
			)
		}
		prependDirs = append(prependDirs,
			"/usr/local/go/bin",
			"/usr/local/bin",
		)
	}

	sep := string(filepath.ListSeparator)
	if goos == "windows" {
		sep = ";"
	}

	splitList := func(pathStr string) []string {
		if goos == "windows" {
			return strings.Split(pathStr, ";")
		}
		return filepath.SplitList(pathStr)
	}

	existingParts := splitList(currentPath)

	seen := make(map[string]bool)
	var finalParts []string

	addDir := func(d string) {
		clean := strings.TrimSpace(d)
		if clean == "" || clean == "." {
			return
		}
		if goos == "windows" {
			clean = strings.ReplaceAll(clean, "/", "\\")
		} else {
			clean = filepath.Clean(clean)
		}
		key := clean
		if goos == "windows" {
			key = strings.ToLower(clean)
		}
		if !seen[key] {
			seen[key] = true
			finalParts = append(finalParts, clean)
		}
	}

	for _, d := range prependDirs {
		addDir(d)
	}
	for _, d := range existingParts {
		addDir(d)
	}

	hydrated := strings.Join(finalParts, sep)
	_ = os.Setenv("PATH", hydrated)
	return hydrated
}
