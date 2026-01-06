package version

import (
	"runtime/debug"
	"strings"
	"time"
)

const defaultModule = "pkt.systems/lockd"

// buildVersion is set via -ldflags "-X pkt.systems/lockd/internal/version.buildVersion=...".
var buildVersion = ""

// Current returns the best available version string.
func Current() string {
	if strings.TrimSpace(buildVersion) != "" {
		return buildVersion
	}
	info, ok := debug.ReadBuildInfo()
	if ok {
		if v := strings.TrimSpace(info.Main.Version); v != "" && v != "(devel)" {
			return v
		}
		if v := pseudoFromBuildInfo(info); v != "" {
			return v
		}
	}
	return "v0.0.0-unknown"
}

// Module returns the module path from build info when available.
func Module() string {
	info, ok := debug.ReadBuildInfo()
	if ok {
		if path := strings.TrimSpace(info.Main.Path); path != "" {
			return path
		}
	}
	return defaultModule
}

func pseudoFromBuildInfo(info *debug.BuildInfo) string {
	if info == nil {
		return ""
	}
	var revision string
	var vcsTime string
	var modified bool
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.time":
			vcsTime = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if revision == "" || vcsTime == "" {
		return ""
	}
	parsed, err := time.Parse(time.RFC3339, vcsTime)
	if err != nil {
		return ""
	}
	rev := revision
	if len(rev) > 12 {
		rev = rev[:12]
	}
	ver := "v0.0.0-" + parsed.UTC().Format("20060102150405") + "-" + rev
	if modified {
		ver += "+dirty"
	}
	return ver
}
