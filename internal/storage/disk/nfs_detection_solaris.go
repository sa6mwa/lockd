//go:build solaris

package disk

import (
	"strings"

	"golang.org/x/sys/unix"
)

func isNFS(root string) bool {
	var st unix.Statvfs_t
	if err := unix.Statvfs(root, &st); err != nil {
		return false
	}
	fsType := int8ToString(st.Basetype[:])
	if fsType == "" {
		fsType = int8ToString(st.Fstr[:])
	}
	return isFSTypeNFS(fsType)
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
