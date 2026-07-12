package runtimeagent

import (
	"fmt"
	"path/filepath"
	"runtime/coverage"
)

type CoverageConfig struct {
	RootDir string
}

func (c CoverageConfig) PythonArgsPrefix() []string {
	if c.RootDir == "" {
		return nil
	}
	return []string{
		"-m",
		"coverage",
		"run",
		"--parallel-mode",
		fmt.Sprintf("--data-file=%s", filepath.Join(c.RootDir, "python", ".coverage")),
		"--source=strategy_service",
	}
}

func WriteGoCoverageSnapshot(dir string) error {
	return coverage.WriteCountersDir(dir)
}
