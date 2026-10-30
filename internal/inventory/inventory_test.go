package inventory

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestClassifyBehindAndMissing(t *testing.T) {
	found := []Tool{{Name: "autopilot", Version: "dev", Path: "/bin/autopilot"}}
	tools, ok := Classify(found, map[string]string{"autopilot": "1.2.3"})
	if ok {
		t.Fatal("expected unschedulable")
	}
	var missing, behind int
	for _, tool := range tools {
		switch tool.Status {
		case "MISSING":
			missing++
		case "BEHIND":
			behind++
		}
	}
	if missing == 0 || behind == 0 {
		t.Fatalf("missing=%d behind=%d", missing, behind)
	}
}

func TestScanFirstHit(t *testing.T) {
	a := t.TempDir()
	b := t.TempDir()
	binName := "autopilot"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	writeExe(t, filepath.Join(a, binName))
	writeExe(t, filepath.Join(b, binName))
	got := Scan([]string{a, b})
	if len(got) != 1 || got[0].Path != filepath.Join(a, binName) {
		t.Fatalf("%+v", got)
	}
}

func TestClassifyVersions(t *testing.T) {
	const sha = "abc123"
	tests := []struct {
		name string
		ver  string
		exp  string
		st   string
	}{
		{name: "dev", ver: "dev", exp: "1.2.3+" + sha, st: "BEHIND"},
		{name: "dev build", ver: "0.1.0-dev", exp: "1.2.3", st: "BEHIND"},
		{name: "same revision", ver: "1.2.3+" + sha, exp: "1.2.3+" + sha, st: "OK"},
		{name: "other revision", ver: "1.2.3+fff", exp: "1.2.3+" + sha, st: "BEHIND"},
		{name: "older", ver: "1.2.2+" + sha, exp: "1.2.3+" + sha, st: "BEHIND"},
		{name: "newer", ver: "1.2.4+" + sha, exp: "1.2.3+" + sha, st: "OK"},
		{name: "empty pin", ver: "1.2.3", exp: "", st: "OK"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tools, ok := Classify([]Tool{{Name: "autopilot", Version: tt.ver, Path: "/bin/autopilot", Executable: true}}, map[string]string{"autopilot": tt.exp})
			var got string
			for _, tool := range tools {
				if tool.Name == "autopilot" {
					got = tool.Status
				}
			}
			if got != tt.st {
				t.Fatalf("status %s, want %s", got, tt.st)
			}
			if ok {
				t.Fatal("a partial baseline must be unschedulable")
			}
		})
	}
}

func TestDefaultDirsOrder(t *testing.T) {
	home := t.TempDir()
	first := t.TempDir()
	second := t.TempDir()
	pathEnv := first + string(os.PathListSeparator) + second
	got := DefaultDirs(pathEnv, home, "linux")
	wantPrefix := []string{
		first,
		second,
		filepath.Join(home, ".local", "bin"),
		filepath.Join(home, ".gemini", "antigravity-cli", "bin"),
	}
	if len(got) != len(wantPrefix) {
		t.Fatalf("%v", got)
	}
	for i := range wantPrefix {
		if got[i] != wantPrefix[i] {
			t.Fatalf("dir %d = %s, want %s", i, got[i], wantPrefix[i])
		}
	}
	win := DefaultDirs("", home, "windows")
	if win[0] != filepath.Join(home, "bin") {
		t.Fatalf("%v", win)
	}
}

func TestScanMatchesAgyAndMockAgy(t *testing.T) {
	dir := t.TempDir()
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	agyBin := filepath.Join(dir, "agy"+ext)
	mockAgyBin := filepath.Join(dir, "mock-agy"+ext)
	_ = os.WriteFile(agyBin, []byte("#!/bin/sh\necho 1.4.8\n"), 0755)
	_ = os.WriteFile(mockAgyBin, []byte("#!/bin/sh\necho 0.1.0\n"), 0755)

	tools := Scan([]string{dir})
	foundAgy := false
	foundMockAgy := false
	for _, tool := range tools {
		if tool.Name == "agy" {
			foundAgy = true
			if !tool.Installed {
				t.Errorf("expected agy tool.Installed to be true")
			}
		}
		if tool.Name == "mock-agy" {
			foundMockAgy = true
			if !tool.Installed {
				t.Errorf("expected mock-agy tool.Installed to be true")
			}
		}
	}
	if !foundAgy {
		t.Errorf("expected Scan to discover agy binary")
	}
	if !foundMockAgy {
		t.Errorf("expected Scan to discover mock-agy binary")
	}
}

func TestInspectInstalledVersion(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "mytool")
	if runtime.GOOS == "windows" {
		binPath = filepath.Join(dir, "mytool.cmd")
		if err := os.WriteFile(binPath, []byte("@echo mytool version v1.4.2\r\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := os.WriteFile(binPath, []byte("#!/bin/sh\necho 'mytool version v1.4.2'\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	// Without .version file, should execute and parse
	ver := InspectInstalledVersion(binPath)
	if ver != "1.4.2" {
		t.Fatalf("expected parsed version 1.4.2, got: %s", ver)
	}

	// With .version file, should read immediately
	if err := os.WriteFile(binPath+".version", []byte("2.0.0"), 0o644); err != nil {
		t.Fatal(err)
	}
	ver2 := InspectInstalledVersion(binPath)
	if ver2 != "2.0.0" {
		t.Fatalf("expected version file 2.0.0, got: %s", ver2)
	}

	// With raw hex SHA in .version, should execute binary --version and preserve +<sha> in .version
	InvalidateVersionCache(binPath)
	rawSHA := "d979f6fc623a6dcdf3fa70b0f712f2db2c179ff7"
	if err := os.WriteFile(binPath+".version", []byte(rawSHA), 0o644); err != nil {
		t.Fatal(err)
	}
	ver3 := InspectInstalledVersion(binPath)
	if ver3 != "1.4.2" {
		t.Fatalf("expected version 1.4.2 when .version held raw SHA, got: %s", ver3)
	}
	sha3 := InspectToolCommitSHA(binPath)
	if sha3 != rawSHA[:7] {
		t.Fatalf("expected 7-char short commit SHA %s to be preserved in .version, got: %s", rawSHA[:7], sha3)
	}
}

func TestScanWindowsUserBin(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "bin", "autopilot.exe")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := ScanOS(DefaultDirs("", home, "windows"), "windows")
	if len(got) != 1 || got[0].Name != "autopilot" || !got[0].Executable {
		t.Fatalf("%+v", got)
	}
}

func writeExe(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestInspectToolService(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	// 1. Not installed
	installed, svcType, svcStatus, caps := InspectToolService("iazio-agent", "linux")
	if installed || svcType != "none" || svcStatus != "not_installed" {
		t.Fatalf("expected not installed, got: %v, %s, %s", installed, svcType, svcStatus)
	}

	// 2. Installed with systemd user unit
	unitDir := filepath.Join(home, ".config", "systemd", "user")
	_ = os.MkdirAll(unitDir, 0755)
	unitContent := `[Unit]
Description=iazio-agent user supervisor
[Service]
ExecStart=/path/to/iazio-agent run
Restart=always
WatchdogSec=30s
`
	_ = os.WriteFile(filepath.Join(unitDir, "iazio-agent.service"), []byte(unitContent), 0644)
	_ = os.WriteFile(filepath.Join(unitDir, "iazio-agent-watcher.path"), []byte("[Path]\n"), 0644)

	installed, svcType, _, caps = InspectToolService("iazio-agent", "linux")
	if !installed || svcType != "user_service" {
		t.Fatalf("expected user_service installed, got: %v, %s", installed, svcType)
	}
	hasWatchdog := false
	hasRestart := false
	hasWatcher := false
	for _, c := range caps {
		if c == "watchdog" {
			hasWatchdog = true
		}
		if c == "auto_restart" {
			hasRestart = true
		}
		if c == "path_watcher" {
			hasWatcher = true
		}
	}
	if !hasWatchdog || !hasRestart || !hasWatcher {
		t.Fatalf("missing expected capabilities: %+v", caps)
	}
}

func TestInvalidateVersionCache(t *testing.T) {
	verCacheMu.Lock()
	verCache["/bin/fake-tool-bin"] = cachedVersion{version: "1.0.0"}
	verCacheMu.Unlock()

	InvalidateVersionCache("/bin/fake-tool-bin")

	verCacheMu.RLock()
	_, exists := verCache["/bin/fake-tool-bin"]
	verCacheMu.RUnlock()
	if exists {
		t.Fatalf("expected /bin/fake-tool-bin to be evicted from version cache")
	}
}

func TestCompareCoreAndDedupe(t *testing.T) {
	s1 := semver{major: 1, minor: 2, patch: 3}
	s2 := semver{major: 2, minor: 0, patch: 0}
	s3 := semver{major: 1, minor: 3, patch: 0}
	s4 := semver{major: 1, minor: 2, patch: 4}
	s5 := semver{major: 1, minor: 2, patch: 3}

	if compareCore(s1, s2) >= 0 {
		t.Errorf("expected s1 < s2")
	}
	if compareCore(s2, s1) <= 0 {
		t.Errorf("expected s2 > s1")
	}
	if compareCore(s1, s3) >= 0 {
		t.Errorf("expected s1 < s3")
	}
	if compareCore(s3, s1) <= 0 {
		t.Errorf("expected s3 > s1")
	}
	if compareCore(s1, s4) >= 0 {
		t.Errorf("expected s1 < s4")
	}
	if compareCore(s4, s1) <= 0 {
		t.Errorf("expected s4 > s1")
	}
	if compareCore(s1, s5) != 0 {
		t.Errorf("expected s1 == s5")
	}

	expA := filepath.Clean("/dir/a")
	expB := filepath.Clean("/dir/b")
	expC := filepath.Clean("/dir/c")
	dirs := []string{"/dir/a", "/dir/b", "/dir/a", "/dir/c", "/dir/b"}
	deduped := dedupe(dirs)
	if len(deduped) != 3 || deduped[0] != expA || deduped[1] != expB || deduped[2] != expC {
		t.Fatalf("unexpected dedupe result: %+v", deduped)
	}

	// parseSemver edge cases
	if _, ok := parseSemver(""); ok {
		t.Errorf("expected empty string to fail parseSemver")
	}
	if _, ok := parseSemver("not.a.number"); ok {
		t.Errorf("expected invalid semver to fail")
	}
	if _, ok := parseSemver("1.2"); ok {
		t.Errorf("expected missing patch to fail")
	}
	if _, ok := parseSemver("1.2.-3"); ok {
		t.Errorf("expected negative number to fail")
	}
	parsed, ok := parseSemver("v2.4.6+abcdef-dirty")
	if !ok || parsed.major != 2 || parsed.minor != 4 || parsed.patch != 6 {
		t.Fatalf("unexpected parsed semver: %+v", parsed)
	}
}

func TestInspectToolServiceDarwin(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	launchAgentsDir := filepath.Join(home, "Library", "LaunchAgents")
	_ = os.MkdirAll(launchAgentsDir, 0755)
	plistPath := filepath.Join(launchAgentsDir, "io.iazio.iazio-agent.plist")
	_ = os.WriteFile(plistPath, []byte("<plist></plist>"), 0644)

	harvesterPlistPath := filepath.Join(launchAgentsDir, "io.iazio.iazio-harvester.plist")
	_ = os.WriteFile(harvesterPlistPath, []byte("<plist></plist>"), 0644)

	harnessPlistPath := filepath.Join(launchAgentsDir, "io.iazio.iazio-harness.plist")
	_ = os.WriteFile(harnessPlistPath, []byte("<plist></plist>"), 0644)

	installed, svcType, svcStatus, caps := InspectToolService("iazio-agent", "darwin")
	if !installed || svcType != "user_service" || svcStatus != "installed" {
		t.Fatalf("expected user_service, got: %v, %s, %s", installed, svcType, svcStatus)
	}
	if len(caps) == 0 {
		t.Fatalf("expected non-empty caps on darwin launchd")
	}

	hvInstalled, hvType, hvStatus, _ := InspectToolService("iazio-harvester", "darwin")
	if !hvInstalled || hvType != "user_service" || hvStatus != "installed" {
		t.Fatalf("expected iazio-harvester user_service, got: %v, %s, %s", hvInstalled, hvType, hvStatus)
	}

	// ADR-002: iazio-harness must never be reported as an OS service even if a legacy plist exists on disk.
	hInstalled, hType, hStatus, _ := InspectToolService("iazio-harness", "darwin")
	if hInstalled || hType != "none" || hStatus != "not_installed" {
		t.Fatalf("expected iazio-harness never to be a service per ADR-002, got: %v, %s, %s", hInstalled, hType, hStatus)
	}
}

func TestInspectToolServiceWindows(t *testing.T) {
	installed, svcType, svcStatus, _ := InspectToolService("iazio-agent", "windows")
	if !installed {
		if svcType != "none" || svcStatus != "not_installed" {
			t.Fatalf("unexpected windows status: %s, %s", svcType, svcStatus)
		}
	}
}

func TestScanSymlinkDetectionAndRejection(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping symlink test on windows")
	}
	dir := t.TempDir()
	realBin := filepath.Join(dir, "real-autopilot")
	writeExe(t, realBin)

	symlinkBin := filepath.Join(dir, "autopilot")
	if err := os.Symlink(realBin, symlinkBin); err != nil {
		t.Fatal(err)
	}

	iazioBin := filepath.Join(dir, "iazio-agent")
	writeExe(t, iazioBin)

	tools := Scan([]string{dir})
	var foundAutopilot, foundIazio bool
	for _, tool := range tools {
		if tool.Name == "autopilot" {
			foundAutopilot = true
			if !tool.IsSymlink {
				t.Errorf("expected autopilot to be marked as symlink")
			}
			if tool.Status != "SYMLINK_REJECTED" {
				t.Errorf("expected autopilot status SYMLINK_REJECTED, got %s", tool.Status)
			}
			if tool.Executable {
				t.Errorf("expected autopilot Executable to be false")
			}
		}
		if tool.Name == "iazio-agent" {
			foundIazio = true
			if tool.IsSymlink {
				t.Errorf("expected iazio-agent not to be marked as symlink")
			}
			if tool.Status != "OK" {
				t.Errorf("expected iazio-agent status OK, got %s", tool.Status)
			}
		}
	}
	if !foundAutopilot {
		t.Errorf("autopilot tool not found in scan")
	}
	if !foundIazio {
		t.Errorf("iazio-agent tool not found in scan")
	}

	// Test InspectInstalledVersion rejects symlink
	ver := InspectInstalledVersion(symlinkBin)
	if ver != "" {
		t.Errorf("expected empty version for symlink, got %s", ver)
	}
}

func TestInspectToolCommitSHAFallbacks(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "tool-with-sha")
	writeExe(t, binPath)

	// Sidecar with valid hex SHA (normalized to 7-char short SHA)
	hexSHA := "abcdef1234567890abcdef1234567890abcdef12"
	expectedShortSHA := "abcdef1"
	if err := os.WriteFile(binPath+".version", []byte(hexSHA), 0o644); err != nil {
		t.Fatal(err)
	}
	sha := InspectToolCommitSHA(binPath)
	if sha != expectedShortSHA {
		t.Fatalf("expected sha %s, got %s", expectedShortSHA, sha)
	}

	// Cached hit
	shaCached := InspectToolCommitSHA(binPath)
	if shaCached != expectedShortSHA {
		t.Fatalf("expected cached sha %s, got %s", expectedShortSHA, shaCached)
	}

	// Missing binary
	if shaNone := InspectToolCommitSHA(filepath.Join(dir, "nonexistent")); shaNone != "" {
		t.Fatalf("expected empty sha for missing file, got %s", shaNone)
	}
}

func TestFindPIDsExecutingFile(t *testing.T) {
	pids := FindPIDsExecutingFile("/nonexistent/file/path")
	if len(pids) != 0 {
		t.Fatalf("expected 0 pids for nonexistent file, got %v", pids)
	}
}

func TestExecutableVariations(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "test.exe")
	if err := os.WriteFile(binPath, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if !executable(fi, "test.exe", "windows") {
		t.Errorf("expected test.exe to be executable on windows")
	}
	if !executable(fi, "test.bat", "windows") {
		t.Errorf("expected test.bat to be executable on windows")
	}
	if !executable(fi, "test.cmd", "windows") {
		t.Errorf("expected test.cmd to be executable on windows")
	}
	if executable(fi, "test.txt", "windows") {
		t.Errorf("expected test.txt not to be executable on windows")
	}

	dirFi, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if executable(dirFi, "dir", "linux") {
		t.Errorf("expected directory not to be executable")
	}
}

func TestScanSymlinkPrecedence_RealBinaryTakesPrecedenceOverPrecedingSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping symlink test on windows")
	}
	dir1 := t.TempDir() // Represents PATH e.g. /usr/local/bin
	dir2 := t.TempDir() // Represents ~/.iazio/bin

	realBin := filepath.Join(dir2, "iazio-harness")
	writeExe(t, realBin)

	symlinkBin := filepath.Join(dir1, "iazio-harness")
	if err := os.Symlink(realBin, symlinkBin); err != nil {
		t.Fatal(err)
	}

	tools := Scan([]string{dir1, dir2})
	var harnessTool *Tool
	for _, tool := range tools {
		if tool.Name == "iazio-harness" {
			h := tool
			harnessTool = &h
			break
		}
	}

	if harnessTool == nil {
		t.Fatal("expected iazio-harness to be discovered in scan")
	}
	if harnessTool.Status != "OK" {
		t.Errorf("expected iazio-harness Status OK, got %s", harnessTool.Status)
	}
	if !harnessTool.Executable {
		t.Errorf("expected iazio-harness Executable to be true")
	}
	if !harnessTool.Installed {
		t.Errorf("expected iazio-harness Installed to be true")
	}
	if harnessTool.Path != realBin {
		t.Errorf("expected iazio-harness Path to resolve to real binary %s, got %s", realBin, harnessTool.Path)
	}
}

func TestMatch_RejectsStagingAndSidecarSuffixes(t *testing.T) {
	rejected := []string{
		"iazio-harvester.next",
		"iazio-agent.next",
		"iazio-agent.old",
		"iazio-agent.tmp",
		"iazio-agent.bak",
		"iazio-agent.version",
		"iazio-agent.sha256",
		"iazio-agent.sig",
	}
	for _, name := range rejected {
		if match(name) {
			t.Errorf("expected match(%q) to be false, got true", name)
		}
	}
	if !match("iazio-harvester") {
		t.Errorf("expected match(\"iazio-harvester\") to be true")
	}
}

func TestInspectToolService_DarwinExitCodesAndRunningState(t *testing.T) {
	// 1. state = running -> active
	installed, _, status, _ := InspectToolService("iazio-harvester", "darwin", func(name string, args ...string) ([]byte, error) {
		return []byte("{\n\tstate = running\n\tpid = 4321\n}\n"), nil
	})
	if !installed || status != "active" {
		t.Fatalf("expected installed=true, status=active, got installed=%v, status=%q", installed, status)
	}

	// 2. non-zero last exit code (e.g. 78 EX_CONFIG) -> failed
	installed, _, status, _ = InspectToolService("iazio-harvester", "darwin", func(name string, args ...string) ([]byte, error) {
		return []byte("{\n\tstate = spawn scheduled\n\tlast exit code = 78: EX_CONFIG\n}\n"), nil
	})
	if !installed || status != "failed" {
		t.Fatalf("expected installed=true, status=failed for exit code 78, got installed=%v, status=%q", installed, status)
	}

	// 3. last exit code = 0 -> installed
	installed, _, status, _ = InspectToolService("iazio-harvester", "darwin", func(name string, args ...string) ([]byte, error) {
		return []byte("{\n\tstate = not running\n\tlast exit code = 0\n}\n"), nil
	})
	if !installed || status != "installed" {
		t.Fatalf("expected installed=true, status=installed for exit code 0, got installed=%v, status=%q", installed, status)
	}

	// 4. last exit code = (never exited) -> installed
	installed, _, status, _ = InspectToolService("iazio-harvester", "darwin", func(name string, args ...string) ([]byte, error) {
		return []byte("{\n\tstate = not running\n\tlast exit code = (never exited)\n}\n"), nil
	})
	if !installed || status != "installed" {
		t.Fatalf("expected installed=true, status=installed for (never exited), got installed=%v, status=%q", installed, status)
	}
}
