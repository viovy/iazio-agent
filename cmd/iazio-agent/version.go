package main

import "strings"

// version is the semantic version stamped at link time.
var version = "dev"

// commit is the source revision stamped at link time.
var commit = "unknown"

// branch is the source branch stamped at link time.
var branch = "unknown"

func formatVersion() string {
	if sha, ok := shortCommit(commit); ok {
		return "iazio-agent " + version + "+" + sha
	}
	return "iazio-agent " + version
}

func shortCommit(value string) (string, bool) {
	c := strings.TrimSpace(value)
	if c == "" || c == "unknown" {
		return "", false
	}
	if len(c) > 7 {
		c = c[:7]
	}
	return c, true
}

func branchName() string {
	if branch == "" {
		return "unknown"
	}
	return branch
}
