//go:build !windows

package runtimeagent

import (
	"fmt"
	"os"
)

func secureTerminalRetryPath(path string, directory bool) error {
	mode := os.FileMode(0o600)
	if directory {
		mode = 0o700
	}
	return os.Chmod(path, mode)
}

func validateTerminalRetryPathSecurity(
	_ string,
	mode os.FileMode,
	want os.FileMode,
) error {
	if mode.Perm() != want {
		return fmt.Errorf("mode is %04o, want %04o", mode.Perm(), want)
	}
	return nil
}

func replaceTerminalRetryFile(source string, destination string) error {
	return os.Rename(source, destination)
}

func syncTerminalRetryDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
