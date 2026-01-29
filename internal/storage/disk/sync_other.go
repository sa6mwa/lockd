//go:build !linux

package disk

import "os"

func syncFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return file.Sync()
}
