//go:build linux

package disk

import (
	"os"
	"syscall"
)

func syncFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return syscall.Fdatasync(int(file.Fd()))
}
