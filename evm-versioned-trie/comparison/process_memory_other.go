//go:build !darwin && !linux

package comparison

func peakRSS() (*uint64, error) { return nil, nil }
