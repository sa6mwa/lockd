package benchenv

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// LoadEnvFile sources the env file using bash and applies any changed variables
// to the current process environment.
func LoadEnvFile(tb testing.TB, filename string) {
	tb.Helper()
	if filename == "" {
		tb.Fatalf("env file name required")
	}
	path := resolveEnvFile(tb, filename)
	applyEnvFile(tb, path)
}

func resolveEnvFile(tb testing.TB, filename string) string {
	tb.Helper()
	if fileExists(filename) {
		path, err := filepath.Abs(filename)
		if err == nil {
			return path
		}
		return filename
	}

	cwd, err := os.Getwd()
	if err == nil {
		dir := cwd
		for {
			if fileExists(filepath.Join(dir, "go.mod")) {
				candidate := filepath.Join(dir, filename)
				if fileExists(candidate) {
					return candidate
				}
				break
			}
			parent := filepath.Dir(dir)
			if parent == dir {
				break
			}
			dir = parent
		}
	}

	tb.Fatalf("missing environment file: %s", filename)
	return filename
}

func applyEnvFile(tb testing.TB, path string) {
	tb.Helper()
	if _, err := os.Stat(path); err != nil {
		tb.Fatalf("missing environment file: %s", path)
	}

	cmd := exec.Command("bash", "-c", "set -a; source \"$1\"; env -0", "bash", path)
	cmd.Env = os.Environ()
	cmd.Dir = filepath.Dir(path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		tb.Fatalf("source env file %s: %v\n%s", path, err, strings.TrimSpace(string(out)))
	}

	before := envMap(os.Environ())
	after := envMapFromOutput(out)
	for key, val := range after {
		if shouldIgnoreEnvKey(key) {
			continue
		}
		if prev, ok := before[key]; ok && prev == val {
			continue
		}
		if err := os.Setenv(key, val); err != nil {
			tb.Fatalf("setenv %s: %v", key, err)
		}
	}
}

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		if entry == "" {
			continue
		}
		if idx := strings.Index(entry, "="); idx >= 0 {
			out[entry[:idx]] = entry[idx+1:]
		}
	}
	return out
}

func envMapFromOutput(out []byte) map[string]string {
	res := map[string]string{}
	for _, entry := range bytes.Split(out, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		if idx := bytes.IndexByte(entry, '='); idx >= 0 {
			key := string(entry[:idx])
			val := string(entry[idx+1:])
			res[key] = val
		}
	}
	return res
}

func shouldIgnoreEnvKey(key string) bool {
	switch key {
	case "PWD", "OLDPWD", "SHLVL", "_", "SHELLOPTS", "BASHOPTS", "PS1", "PS2", "PS4", "PROMPT_COMMAND", "TERM", "COLORTERM":
		return true
	}
	return strings.HasPrefix(key, "BASH_")
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
