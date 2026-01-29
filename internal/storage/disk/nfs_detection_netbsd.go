//go:build netbsd

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
	return isFSTypeNFS(byteToString(st.Fstypename[:]))
}

func byteToString(buf []byte) string {
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
