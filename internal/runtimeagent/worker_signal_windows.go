//go:build windows

package runtimeagent

import "os"

func requestWorkerStop(process *os.Process) error {
	return process.Kill()
}
