package runtimeagent_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

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

func TestInitializeCoverageFinalizationReplacesStaleMarker(t *testing.T) {
	root := t.TempDir()
	marker := filepath.Join(root, runtimeagent.CoverageFinalizationFile)
	if err := os.WriteFile(marker, []byte(`{"schema_version":1,"runtime_id":"rt-old","boot_id":"old","state":"complete"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	bootID, err := runtimeagent.InitializeCoverageFinalization(root, "rt-new")
	if err != nil {
		t.Fatalf("InitializeCoverageFinalization: %v", err)
	}
	if bootID == "" || bootID == "old" {
		t.Fatalf("boot_id = %q, want a fresh value", bootID)
	}
	var record runtimeagent.CoverageFinalization
	body, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatal(err)
	}
	if record.SchemaVersion != 1 || record.RuntimeID != "rt-new" || record.BootID != bootID || record.State != "running" {
		t.Fatalf("record = %+v", record)
	}
	if record.WorkerShutdown != "pending" || record.GoSnapshot != "pending" || record.CompletedAt != "" {
		t.Fatalf("running record exposes final state: %+v", record)
	}
}

func TestWriteCoverageFinalizationUsesApprovedSchemaOnly(t *testing.T) {
	root := t.TempDir()
	record := runtimeagent.CoverageFinalization{
		SchemaVersion:  1,
		RuntimeID:      "rt-1",
		BootID:         "boot-1",
		State:          "complete",
		WorkerShutdown: "ok",
		ForcedWorkers:  0,
		GoSnapshot:     "ok",
		CompletedAt:    time.Date(2026, 7, 12, 1, 2, 3, 0, time.UTC).Format(time.RFC3339Nano),
	}
	if err := runtimeagent.WriteCoverageFinalization(root, record); err != nil {
		t.Fatalf("WriteCoverageFinalization: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(root, runtimeagent.CoverageFinalizationFile))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	want := []string{"boot_id", "completed_at", "forced_workers", "go_snapshot", "runtime_id", "schema_version", "state", "worker_shutdown"}
	got := make([]string, 0, len(fields))
	for key := range fields {
		got = append(got, key)
	}
	slices.Sort(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("marker fields = %v, want %v", got, want)
	}
	for _, forbidden := range []string{"error", "message", "credential", "token", "tls", "address"} {
		if _, ok := fields[forbidden]; ok {
			t.Fatalf("marker contains forbidden field %q", forbidden)
		}
	}
}

func TestWriteCoverageFinalizationRejectsUnapprovedStatusText(t *testing.T) {
	record := runtimeagent.CoverageFinalization{
		SchemaVersion:  1,
		RuntimeID:      "rt-1",
		BootID:         "boot-1",
		State:          "incomplete",
		WorkerShutdown: "secret diagnostic text",
		ForcedWorkers:  0,
		GoSnapshot:     "ok",
		CompletedAt:    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err := runtimeagent.WriteCoverageFinalization(t.TempDir(), record); err == nil {
		t.Fatal("WriteCoverageFinalization accepted arbitrary worker status text")
	}
}

func TestWriteCoverageFinalizationRejectsInconsistentFacts(t *testing.T) {
	validTime := time.Now().UTC().Format(time.RFC3339Nano)
	for _, record := range []runtimeagent.CoverageFinalization{
		{
			SchemaVersion: 1, RuntimeID: "rt-1", BootID: "boot-1",
			State: "running", WorkerShutdown: "pending", ForcedWorkers: 1, GoSnapshot: "pending",
		},
		{
			SchemaVersion: 1, RuntimeID: "rt-1", BootID: "boot-1",
			State: "incomplete", WorkerShutdown: "forced", ForcedWorkers: 0, GoSnapshot: "ok", CompletedAt: validTime,
		},
		{
			SchemaVersion: 1, RuntimeID: "rt-1", BootID: "boot-1",
			State: "incomplete", WorkerShutdown: "ok", ForcedWorkers: 1, GoSnapshot: "ok", CompletedAt: validTime,
		},
		{
			SchemaVersion: 1, RuntimeID: "rt-1", BootID: "boot-1",
			State: "incomplete", WorkerShutdown: "error", GoSnapshot: "ok", CompletedAt: "not-a-time",
		},
	} {
		if err := runtimeagent.WriteCoverageFinalization(t.TempDir(), record); err == nil {
			t.Fatalf("WriteCoverageFinalization accepted inconsistent record: %+v", record)
		}
	}
}
