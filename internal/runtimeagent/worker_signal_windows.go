//go:build windows

package runtimeagent

import "os"

func requestWorkerStop(process *os.Process) (bool, error) {
	return true, process.Kill()
}
