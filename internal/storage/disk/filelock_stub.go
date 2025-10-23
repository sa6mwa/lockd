//go:build !unix

package disk

import "os"

// lockFile is a stub on non-Unix platforms; the underlying filesystem is
// expected to provide its own serialization semantics.
func lockFile(f *os.File) error { return nil }

// unlockFile is a stub counterpart to lockFile on non-Unix platforms.
func unlockFile(f *os.File) error { return nil }
