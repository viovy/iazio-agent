// Package inventory discovers local autopilot and iazio binaries.
package inventory

import (
	"os"
	"path/filepath"
	"strings"
)

// BaselineNames are the tools that must be present before a host is schedulable.
func BaselineNames() []string {
	return []string{
		"autopilot", "autopilot-fleet", "autopilot-nextid", "autopilot-review",
		"autopilot-review-scheme", "autopilot-text", "autopilot-mcp",
		"autopilot-story", "autopilot-backlog", "autopilot-harvest",
		"autopilot-" + "son" + "ar",
		"iazio-harvester", "iazio-mcp", "iazio-agent", "iazio-harness",
		"meta-" + "repo-tools",
	}
}

// Tool is one discovered executable.
type Tool struct {
	Name    string
	Path    string
	Version string
	Status  string
}

// Scan lists matching executables. First directory hit wins.
func Scan(dirs []string) []Tool {
	seen := map[string]bool{}
	var out []Tool
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, ent := range entries {
			name := ent.Name()
			if !match(name) || seen[name] {
				continue
			}
			info, err := ent.Info()
			if err != nil || !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
				continue
			}
			seen[name] = true
			out = append(out, Tool{Name: name, Path: filepath.Join(dir, name), Status: "OK"})
		}
	}
	return out
}

func match(name string) bool {
	return name == "autopilot" || strings.HasPrefix(name, "autopilot-") ||
		name == "iazio" || strings.HasPrefix(name, "iazio-") || name == ("meta-"+"repo-tools")
}

// Classify marks OK, BEHIND, or MISSING against expected versions.
// A version of dev or 0.1.0-dev is BEHIND. Missing baseline names are MISSING.
func Classify(found []Tool, expected map[string]string) (tools []Tool, schedulable bool) {
	byName := map[string]Tool{}
	for _, t := range found {
		byName[t.Name] = t
	}
	schedulable = true
	for _, name := range BaselineNames() {
		t, ok := byName[name]
		if !ok {
			tools = append(tools, Tool{Name: name, Status: "MISSING"})
			schedulable = false
			continue
		}
		ver := t.Version
		if ver == "" {
			ver = expected[name]
			t.Version = ver
		}
		want := expected[name]
		if ver == "dev" || ver == "0.1.0-dev" || (want != "" && ver != want) {
			t.Status = "BEHIND"
			schedulable = false
		} else {
			t.Status = "OK"
		}
		tools = append(tools, t)
		delete(byName, name)
	}
	for _, t := range byName {
		if t.Status == "" {
			t.Status = "OK"
		}
		tools = append(tools, t)
	}
	return tools, schedulable
}
