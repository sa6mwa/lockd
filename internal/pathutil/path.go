package pathutil

import (
	"os"
	"path/filepath"
	"strings"
)

// ExpandUserAndEnv expands shell-style path components in p.
// It supports:
//   - environment variable tokens via os.ExpandEnv (for example $HOME, ${HOME})
//   - leading "~/" or "~\" to the current user's home directory
//
// The returned path is not normalized to absolute form; callers retain control
// over relative-path handling.
func ExpandUserAndEnv(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", nil
	}
	p = os.ExpandEnv(p)
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if len(p) == 1 {
			p = home
		} else if p[1] == '/' || p[1] == '\\' {
			p = filepath.Join(home, p[2:])
		}
	}
	return p, nil
}
