//go:build linux

package disk

import "syscall"

func isNFS(root string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(root, &st); err != nil {
		return false
	}
	return st.Type == nfsSuperMagic
}
