package runtimeagent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/coverage"
	"strings"
)

const (
	CoverageFinalizationFile       = "finalization.json"
	CoverageFinalizationRunning    = "running"
	CoverageFinalizationComplete   = "complete"
	CoverageFinalizationIncomplete = "incomplete"
	CoverageFinalizationPending    = "pending"
	CoverageFinalizationOK         = "ok"
	CoverageFinalizationError      = "error"
	CoverageFinalizationForced     = "forced"
)

type CoverageFinalization struct {
	SchemaVersion  int    `json:"schema_version"`
	RuntimeID      string `json:"runtime_id"`
	BootID         string `json:"boot_id"`
	State          string `json:"state"`
	WorkerShutdown string `json:"worker_shutdown"`
	ForcedWorkers  int    `json:"forced_workers"`
	GoSnapshot     string `json:"go_snapshot"`
	CompletedAt    string `json:"completed_at,omitempty"`
}

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

func InitializeCoverageFinalization(root, runtimeID string) (string, error) {
	runtimeID = strings.TrimSpace(runtimeID)
	if runtimeID == "" {
		return "", fmt.Errorf("runtime_id is required for coverage finalization")
	}
	bootID, err := randomCoverageBootID()
	if err != nil {
		return "", err
	}
	record := CoverageFinalization{
		SchemaVersion:  1,
		RuntimeID:      runtimeID,
		BootID:         bootID,
		State:          CoverageFinalizationRunning,
		WorkerShutdown: CoverageFinalizationPending,
		GoSnapshot:     CoverageFinalizationPending,
	}
	if err := WriteCoverageFinalization(root, record); err != nil {
		return "", err
	}
	return bootID, nil
}

func WriteCoverageFinalization(root string, record CoverageFinalization) error {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "." || !filepath.IsAbs(root) {
		return fmt.Errorf("coverage finalization root must be absolute")
	}
	if err := validateCoverageFinalization(record); err != nil {
		return err
	}
	temp, err := os.CreateTemp(root, ".finalization-*.tmp")
	if err != nil {
		return fmt.Errorf("create coverage finalization temp file: %w", err)
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		_ = temp.Close()
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure coverage finalization temp file: %w", err)
	}
	encoder := json.NewEncoder(temp)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(record); err != nil {
		return fmt.Errorf("encode coverage finalization: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync coverage finalization: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close coverage finalization: %w", err)
	}
	finalPath := filepath.Join(root, CoverageFinalizationFile)
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("publish coverage finalization: %w", err)
	}
	removeTemp = false
	return nil
}

func validateCoverageFinalization(record CoverageFinalization) error {
	if record.SchemaVersion != 1 {
		return fmt.Errorf("coverage finalization schema_version must be 1")
	}
	if strings.TrimSpace(record.RuntimeID) == "" || strings.TrimSpace(record.BootID) == "" {
		return fmt.Errorf("coverage finalization runtime_id and boot_id are required")
	}
	switch record.State {
	case CoverageFinalizationRunning, CoverageFinalizationComplete, CoverageFinalizationIncomplete:
	default:
		return fmt.Errorf("invalid coverage finalization state")
	}
	if record.ForcedWorkers < 0 {
		return fmt.Errorf("coverage finalization forced_workers cannot be negative")
	}
	if record.State == CoverageFinalizationRunning {
		if record.WorkerShutdown != CoverageFinalizationPending || record.GoSnapshot != CoverageFinalizationPending || record.CompletedAt != "" {
			return fmt.Errorf("running coverage finalization must remain pending")
		}
		return nil
	}
	if record.CompletedAt == "" {
		return fmt.Errorf("completed_at is required for final coverage state")
	}
	switch record.WorkerShutdown {
	case CoverageFinalizationOK, CoverageFinalizationError, CoverageFinalizationForced:
	default:
		return fmt.Errorf("invalid coverage finalization worker_shutdown")
	}
	switch record.GoSnapshot {
	case CoverageFinalizationOK, CoverageFinalizationError:
	default:
		return fmt.Errorf("invalid coverage finalization go_snapshot")
	}
	completeFacts := record.WorkerShutdown == CoverageFinalizationOK && record.ForcedWorkers == 0 && record.GoSnapshot == CoverageFinalizationOK
	if record.State == CoverageFinalizationComplete && !completeFacts {
		return fmt.Errorf("complete coverage finalization requires successful facts")
	}
	if record.State == CoverageFinalizationIncomplete && completeFacts {
		return fmt.Errorf("incomplete coverage finalization requires a failed fact")
	}
	return nil
}

func randomCoverageBootID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate coverage boot id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}
