//go:build windows

package runtimeagent

import (
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/windows"
)

func configureRuntimeProbeCommand(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP}
}

func terminateRuntimeProbeProcess(process *os.Process) error {
	if process == nil {
		return nil
	}
	return process.Kill()
}

func killRuntimeProbeProcess(process *os.Process) error {
	if process == nil {
		return nil
	}
	return process.Kill()
}
