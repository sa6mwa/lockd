//go:build windows

package disk

import (
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func isNFS(root string) bool {
	volume := filepath.VolumeName(root)
	if volume == "" {
		return false
	}
	if !strings.HasSuffix(volume, `\\`) {
		volume += `\\`
	}
	volPtr, err := windows.UTF16PtrFromString(volume)
	if err != nil {
		return false
	}
	var fsName [256]uint16
	if err := windows.GetVolumeInformation(volPtr, nil, 0, nil, nil, nil, &fsName[0], uint32(len(fsName))); err != nil {
		return false
	}
	return isFSTypeNFS(windows.UTF16ToString(fsName[:]))
}

func isFSTypeNFS(fsType string) bool {
	fsType = strings.ToLower(strings.TrimSpace(fsType))
	return strings.HasPrefix(fsType, "nfs")
}
