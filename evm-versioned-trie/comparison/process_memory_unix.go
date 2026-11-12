//go:build darwin || linux

package comparison

import (
	"golang.org/x/sys/unix"
	"runtime"
)

func peakRSS() (*uint64, error) {
	var r unix.Rusage
	if e := unix.Getrusage(unix.RUSAGE_SELF, &r); e != nil {
		return nil, e
	}
	value := uint64(r.Maxrss)
	if runtime.GOOS == "linux" {
		value *= 1024
	}
	return &value, nil
}
