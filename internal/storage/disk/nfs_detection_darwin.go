//go:build darwin

package disk

import (
	"strings"
	"syscall"
)

func isNFS(root string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return false
	}
	return isFSTypeNFS(int8ToString(st.Fstypename[:]))
}

func int8ToString(buf []int8) string {
	var out []byte
	for _, b := range buf {
		if b == 0 {
			break
		}
		out = append(out, byte(b))
	}
	return string(out)
}

func isFSTypeNFS(fsType string) bool {
	fsType = strings.ToLower(strings.TrimSpace(fsType))
	return fsType == "nfs" || fsType == "nfs4"
}
