//go:build !linux && !darwin && !freebsd && !openbsd && !dragonfly && !netbsd && !solaris && !aix && !windows

package disk

func isNFS(_ string) bool {
	return false
}
