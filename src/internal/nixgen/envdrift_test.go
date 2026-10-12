package nixgen

import (
	"regexp"
	"strings"
	"testing"
)

// TestEveryEnvVarTheShimNamesIsRead.
//
// PIPEDPEER_PANDAS and PIPEDPEER_TORCH were set by tests and documented in a
// comment for months, and nothing ever read either. A knob that does nothing
// is worse than no knob: someone sets it, sees the behaviour they wanted for
// an unrelated reason, and believes it works.
//
// Names appearing only in a comment are excluded — prose about a variable is
// not a claim that it exists — but a comment saying "opt-in via X=1" when
// nothing reads X is exactly the drift this catches, so the check runs over
// code with comments stripped.
func TestEveryEnvVarTheShimNamesIsRead(t *testing.T) {
	// The constant, not the file. Reading shim.go from disk made this test
	// depend on the source tree being present, which it is not when the
	// binaries are built here and run on the machine that has the Python
	// dependencies - it failed there with "cannot find shim.go" while passing
	// locally, which is the exact shape of problem test-on-host.sh exists to
	// surface.
	code := stripPyComments(ShimSitecustomize)

	named := regexp.MustCompile(`"(PIPEDPEER_[A-Z0-9_]+)"`)
	read := regexp.MustCompile(`(?:environ\.get|getenv|environ\[)\s*\(?\s*"(PIPEDPEER_[A-Z0-9_]+)"`)

	isRead := map[string]bool{}
	for _, m := range read.FindAllStringSubmatch(code, -1) {
		isRead[m[1]] = true
	}
	// Assignment counts as use: the shim sets a few markers for the worker
	// process to read on the other side.
	assigned := regexp.MustCompile(`environ\[\s*"(PIPEDPEER_[A-Z0-9_]+)"\s*\]\s*=`)
	for _, m := range assigned.FindAllStringSubmatch(code, -1) {
		isRead[m[1]] = true
	}

	seen := map[string]bool{}
	for _, m := range named.FindAllStringSubmatch(code, -1) {
		name := m[1]
		if seen[name] || isRead[name] {
			continue
		}
		seen[name] = true
		t.Errorf("%s is named in the shim but never read; a knob that does "+
			"nothing is worse than no knob", name)
	}
}

// stripPyComments removes whole-line and trailing # comments, leaving string
// literals alone well enough for this check.
func stripPyComments(s string) string {
	var out strings.Builder
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
			continue
		}
		out.WriteString(line)
		out.WriteString("\n")
	}
	return out.String()
}
