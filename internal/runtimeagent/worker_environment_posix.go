//go:build !windows

package runtimeagent

import (
	"os"
	"path/filepath"
	"strings"
)

func trustedWorkerPlatformEnvironment(resolvedExecutable string) (map[string]string, error) {
	paths := []string{
		filepath.Dir(resolvedExecutable),
		"/usr/local/bin",
		"/usr/bin",
		"/bin",
	}
	unique := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		path = filepath.Clean(path)
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		unique = append(unique, path)
	}
	return map[string]string{
		"PATH": strings.Join(unique, string(os.PathListSeparator)),
	}, nil
}
