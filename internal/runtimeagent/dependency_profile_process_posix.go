//go:build !windows

package runtimeagent

import (
	"os"
	"os/exec"
	"syscall"
)

func configureRuntimeProbeCommand(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateRuntimeProbeProcess(process *os.Process) error {
	if process == nil {
		return nil
	}
	return syscall.Kill(-process.Pid, syscall.SIGTERM)
}

func killRuntimeProbeProcess(process *os.Process) error {
	if process == nil {
		return nil
	}
	return syscall.Kill(-process.Pid, syscall.SIGKILL)
}
