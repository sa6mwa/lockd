//go:build aix

package disk

import (
	"strings"

	"golang.org/x/sys/unix"
)

func isNFS(root string) bool {
	var st unix.Statfs_t
	if err := unix.Statfs(root, &st); err != nil {
		return false
	}
	fsType := uint8ToString(st.Fname[:])
	if fsType == "" {
		fsType = uint8ToString(st.Fpack[:])
	}
	return isFSTypeNFS(fsType)
}

func uint8ToString(buf []uint8) string {
	end := 0
	for ; end < len(buf); end++ {
		if buf[end] == 0 {
			break
		}
	}
	return string(buf[:end])
}

func isFSTypeNFS(fsType string) bool {
	fsType = strings.ToLower(strings.TrimSpace(fsType))
	return fsType == "nfs" || fsType == "nfs4"
}
