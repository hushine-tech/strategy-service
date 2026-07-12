//go:build !windows

package runtimeagent

import (
	"os"
	"syscall"
)

func requestWorkerStop(process *os.Process) (bool, error) {
	return false, process.Signal(syscall.SIGTERM)
}
