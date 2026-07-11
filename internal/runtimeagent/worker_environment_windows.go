//go:build windows

package runtimeagent

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func trustedWorkerPlatformEnvironment(resolvedExecutable string) (map[string]string, error) {
	windowsDir, err := windows.GetWindowsDirectory()
	if err != nil {
		return nil, fmt.Errorf("get native Windows directory: %w", err)
	}
	systemDir, err := windows.GetSystemDirectory()
	if err != nil {
		return nil, fmt.Errorf("get native Windows system directory: %w", err)
	}

	paths := []string{filepath.Dir(resolvedExecutable), systemDir}
	unique := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = filepath.Clean(path)
		key := strings.ToLower(path)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, path)
	}
	return map[string]string{
		"PATH":       strings.Join(unique, string(os.PathListSeparator)),
		"SYSTEMROOT": windowsDir,
		"WINDIR":     windowsDir,
		"COMSPEC":    filepath.Join(systemDir, "cmd.exe"),
		"PATHEXT":    ".COM;.EXE;.BAT;.CMD",
	}, nil
}
