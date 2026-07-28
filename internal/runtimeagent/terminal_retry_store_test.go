package runtimeagent

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestTerminalRetryStoreCreatesMissingFreshStateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fresh-runtime-state")

	store, err := NewTerminalRetryStore(root)
	if err != nil {
		t.Fatalf("NewTerminalRetryStore: %v", err)
	}
	if store.root != root {
		t.Fatalf("store root = %q, want %q", store.root, root)
	}
	for _, path := range []string{
		root,
		filepath.Join(root, terminalRetryDirectoryName),
	} {
		info, err := os.Lstat(path)
		if err != nil {
			t.Fatalf("Lstat %s: %v", path, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("%s mode = %v, want real directory", path, info.Mode())
		}
		if err := validateTerminalRetryPathSecurity(
			path,
			info.Mode(),
			0o700,
		); err != nil {
			t.Fatalf("%s permissions are not private: %v", path, err)
		}
	}
	records, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("fresh retry records = %+v, want empty", records)
	}
}

func TestTerminalRetryStoreRoundTripsAndDeletesAtomicRecord(t *testing.T) {
	root := t.TempDir()
	store, err := NewTerminalRetryStore(root)
	if err != nil {
		t.Fatalf("NewTerminalRetryStore: %v", err)
	}
	record := TerminalRetryRecord{
		SchemaVersion:   indicatorTerminalRetrySchemaVersion,
		SessionID:       "sess-retry",
		Generation:      7,
		DesiredStatus:   "finished",
		EffectiveStatus: "recoverable",
		BarsProcessed:   17,
		Reason:          "indicator finalization failed",
	}
	if err := store.Save(record); err != nil {
		t.Fatalf("Save: %v", err)
	}

	entries, err := os.ReadDir(filepath.Join(root, terminalRetryDirectoryName))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("retry files = %d, want 1", len(entries))
	}
	info, err := entries[0].Info()
	if err != nil {
		t.Fatalf("retry file info: %v", err)
	}
	recordPath := filepath.Join(
		root,
		terminalRetryDirectoryName,
		entries[0].Name(),
	)
	if !info.Mode().IsRegular() {
		t.Fatalf("retry file mode = %v, want regular 0600", info.Mode())
	}
	if err := validateTerminalRetryPathSecurity(
		recordPath,
		info.Mode(),
		0o600,
	); err != nil {
		t.Fatalf("retry file permissions are not private: %v", err)
	}
	raw, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if strings.Contains(string(raw), "worker-token") {
		t.Fatalf("retry record persisted worker token: %s", raw)
	}

	loaded, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if len(loaded) != 1 || !reflect.DeepEqual(loaded[0], record) {
		t.Fatalf("loaded = %+v, want %+v", loaded, record)
	}
	if err := store.Delete(record.SessionID, record.Generation); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	loaded, err = store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll after delete: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("records after delete = %+v", loaded)
	}
}

func TestTerminalRetryStoreFailsClosedOnCorruptOrUnsafeRecord(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "truncated",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(`{"schema_version":1`), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "unsafe mode",
			mutate: func(t *testing.T, path string) {
				t.Helper()
				if runtime.GOOS == "windows" {
					t.Skip("Windows does not expose POSIX permission bits")
				}
				if err := os.Chmod(path, 0o644); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewTerminalRetryStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			record := TerminalRetryRecord{
				SchemaVersion:   indicatorTerminalRetrySchemaVersion,
				SessionID:       "sess-corrupt",
				Generation:      3,
				DesiredStatus:   "recoverable",
				EffectiveStatus: "recoverable",
				Reason:          "retry",
			}
			if err := store.Save(record); err != nil {
				t.Fatal(err)
			}
			path := store.recordPath(record.SessionID, record.Generation)
			test.mutate(t, path)

			if _, err := store.LoadAll(); err == nil {
				t.Fatal("LoadAll accepted unsafe retry record")
			}
		})
	}
}

func TestTerminalRetryStoreRejectsRecordTooLargeToReload(t *testing.T) {
	store, err := NewTerminalRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record := TerminalRetryRecord{
		SchemaVersion:   indicatorTerminalRetrySchemaVersion,
		SessionID:       "sess-oversized",
		Generation:      3,
		DesiredStatus:   "recoverable",
		EffectiveStatus: "recoverable",
		Indicators: &IndicatorSessionCheckpointV2{
			SchemaVersion: indicatorSessionCheckpointSchemaV2,
			SessionID:     "sess-oversized",
			Streams: []indicatorStreamCheckpointV2{{
				Definitions: []indicatorDefinitionCheckpointV2{{
					ConfigJSON: strings.Repeat(
						"x",
						terminalRetryMaximumBytes,
					),
				}},
			}},
		},
	}

	err = store.Save(record)

	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("Save error = %v, want oversized record rejection", err)
	}
	if entries, readErr := os.ReadDir(store.dir); readErr != nil {
		t.Fatal(readErr)
	} else if len(entries) != 0 {
		t.Fatalf("oversized record left files behind: %+v", entries)
	}
}
