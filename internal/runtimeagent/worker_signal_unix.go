//go:build !windows

package runtimeagent

import (
	"os"
	"syscall"
)

func requestWorkerStop(process *os.Process) error {
	return process.Signal(syscall.SIGTERM)
}
