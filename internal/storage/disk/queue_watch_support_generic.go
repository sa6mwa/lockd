//go:build !linux

package disk

func queueWatchSupported(root string) bool {
	return false
}
