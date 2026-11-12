package comparison

import "runtime"

type ProcessMemory struct {
	GoHeapAllocBytes, GoHeapSysBytes, GoStackSysBytes, GoRuntimeSysBytes uint64
	PeakRSSBytes                                                         *uint64
	PeakRSSScope                                                         string
}

// CaptureProcessMemory is an out-of-timing diagnostic. RSS includes native
// allocations, and its peak covers this process's lifetime, not one method.
func CaptureProcessMemory() (ProcessMemory, error) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	out := ProcessMemory{GoHeapAllocBytes: mem.HeapAlloc, GoHeapSysBytes: mem.HeapSys, GoStackSysBytes: mem.StackSys, GoRuntimeSysBytes: mem.Sys, PeakRSSScope: "process-lifetime"}
	var e error
	out.PeakRSSBytes, e = peakRSS()
	return out, e
}
