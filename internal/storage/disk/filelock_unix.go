//go:build unix

package disk

import (
	"os"

	"golang.org/x/sys/unix"
)

// lockFile obtains an exclusive advisory lock on the provided file handle.
func lockFile(f *os.File) error {
	flock := unix.Flock_t{Type: unix.F_WRLCK, Whence: int16(0)}
	return unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &flock)
}

// unlockFile releases any advisory lock held on the provided file handle.
func unlockFile(f *os.File) error {
	flock := unix.Flock_t{Type: unix.F_UNLCK, Whence: int16(0)}
	return unix.FcntlFlock(f.Fd(), unix.F_SETLK, &flock)
}
