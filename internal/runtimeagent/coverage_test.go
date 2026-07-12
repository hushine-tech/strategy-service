package runtimeagent_test

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/hushine-tech/strategy-service/internal/runtimeagent"
)

func TestCoverageConfigPythonArgs(t *testing.T) {
	cfg := runtimeagent.CoverageConfig{RootDir: "/coverage"}
	got := cfg.PythonArgsPrefix()
	want := []string{
		"-m", "coverage", "run", "--parallel-mode",
		"--data-file=/coverage/python/.coverage", "--source=strategy_service",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("PythonArgsPrefix() = %v, want %v", got, want)
	}
}

func TestCoverageConfigDisabledHasNoPythonWrapper(t *testing.T) {
	if got := (runtimeagent.CoverageConfig{}).PythonArgsPrefix(); len(got) != 0 {
		t.Fatalf("PythonArgsPrefix() = %v, want no arguments", got)
	}
}

func TestWriteGoCoverageSnapshotReturnsRuntimeError(t *testing.T) {
	missingDir := filepath.Join(t.TempDir(), "missing")
	if err := runtimeagent.WriteGoCoverageSnapshot(missingDir); err == nil {
		t.Fatal("WriteGoCoverageSnapshot() error = nil for a missing output directory")
	}
}
