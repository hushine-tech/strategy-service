package runtimeagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	indicatorTerminalRetrySchemaVersion = 1
	terminalRetryDirectoryName          = "terminal-retries"
	terminalRetryMaximumBytes           = 16 << 20
)

type TerminalRetryRecord struct {
	SchemaVersion   int                           `json:"schema_version"`
	SessionID       string                        `json:"session_id"`
	Generation      uint64                        `json:"generation"`
	DesiredStatus   string                        `json:"desired_status"`
	EffectiveStatus string                        `json:"effective_status"`
	BarsProcessed   int64                         `json:"bars_processed"`
	Reason          string                        `json:"reason"`
	ExpectedStatus  string                        `json:"expected_status,omitempty"`
	Indicators      *IndicatorSessionCheckpointV2 `json:"indicators,omitempty"`
}

type terminalRetryEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	Record        json.RawMessage `json:"record"`
	RecordSHA256  string          `json:"record_sha256"`
}

type TerminalRetryStore struct {
	root string
	dir  string
}

func NewTerminalRetryStore(root string) (*TerminalRetryStore, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("terminal retry state root is required")
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf(
			"terminal retry state root must be an absolute cleaned path",
		)
	}
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		if mkdirErr := os.Mkdir(root, 0o700); mkdirErr != nil &&
			!os.IsExist(mkdirErr) {
			return nil, fmt.Errorf(
				"create terminal retry state root: %w",
				mkdirErr,
			)
		}
		info, err = os.Lstat(root)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect terminal retry state root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf(
			"terminal retry state root must be a real directory",
		)
	}
	if err := secureTerminalRetryPath(root, true); err != nil {
		return nil, fmt.Errorf(
			"secure terminal retry state root: %w",
			err,
		)
	}
	dir := filepath.Join(root, terminalRetryDirectoryName)
	if err := os.Mkdir(dir, 0o700); err != nil &&
		!os.IsExist(err) {
		return nil, fmt.Errorf("create terminal retry directory: %w", err)
	}
	dirInfo, err := os.Lstat(dir)
	if err != nil {
		return nil, fmt.Errorf("inspect terminal retry directory: %w", err)
	}
	if !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf(
			"terminal retry path must be a real directory",
		)
	}
	if err := secureTerminalRetryPath(dir, true); err != nil {
		return nil, fmt.Errorf("secure terminal retry directory: %w", err)
	}
	return &TerminalRetryStore{root: root, dir: dir}, nil
}

func (s *TerminalRetryStore) Save(record TerminalRetryRecord) error {
	if s == nil {
		return fmt.Errorf("terminal retry store is not configured")
	}
	if err := validateTerminalRetryRecord(record); err != nil {
		return err
	}
	recordRaw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal terminal retry record: %w", err)
	}
	recordHash := sha256.Sum256(recordRaw)
	envelopeRaw, err := json.Marshal(terminalRetryEnvelope{
		SchemaVersion: indicatorTerminalRetrySchemaVersion,
		Record:        recordRaw,
		RecordSHA256:  hex.EncodeToString(recordHash[:]),
	})
	if err != nil {
		return fmt.Errorf("marshal terminal retry envelope: %w", err)
	}
	envelopeRaw = append(envelopeRaw, '\n')
	if len(envelopeRaw) > terminalRetryMaximumBytes {
		return fmt.Errorf(
			"terminal retry record is too large: %d bytes exceeds %d",
			len(envelopeRaw),
			terminalRetryMaximumBytes,
		)
	}

	temp, err := os.CreateTemp(s.dir, ".terminal-retry-*.tmp")
	if err != nil {
		return fmt.Errorf("create terminal retry temporary file: %w", err)
	}
	tempPath := temp.Name()
	closed := false
	defer func() {
		if !closed {
			_ = temp.Close()
		}
		_ = os.Remove(tempPath)
	}()
	if err := temp.Chmod(0o600); err != nil {
		return fmt.Errorf("secure terminal retry temporary file: %w", err)
	}
	if err := secureTerminalRetryPath(tempPath, false); err != nil {
		return fmt.Errorf(
			"secure terminal retry temporary file ACL: %w",
			err,
		)
	}
	if _, err := temp.Write(envelopeRaw); err != nil {
		return fmt.Errorf("write terminal retry temporary file: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("sync terminal retry temporary file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close terminal retry temporary file: %w", err)
	}
	closed = true
	path := s.recordPath(record.SessionID, record.Generation)
	if err := replaceTerminalRetryFile(tempPath, path); err != nil {
		return fmt.Errorf("replace terminal retry record: %w", err)
	}
	if err := syncTerminalRetryDirectory(s.dir); err != nil {
		return fmt.Errorf("sync terminal retry directory: %w", err)
	}
	return nil
}

func (s *TerminalRetryStore) LoadAll() ([]TerminalRetryRecord, error) {
	if s == nil {
		return nil, fmt.Errorf("terminal retry store is not configured")
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read terminal retry directory: %w", err)
	}
	records := make([]TerminalRetryRecord, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			return nil, fmt.Errorf(
				"unexpected file in terminal retry directory: %s",
				entry.Name(),
			)
		}
		path := filepath.Join(s.dir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf(
				"inspect terminal retry record %s: %w",
				entry.Name(),
				err,
			)
		}
		if !info.Mode().IsRegular() ||
			info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf(
				"terminal retry record %s must be a regular 0600 file",
				entry.Name(),
			)
		}
		if err := validateTerminalRetryPathSecurity(
			path,
			info.Mode(),
			0o600,
		); err != nil {
			return nil, fmt.Errorf(
				"terminal retry record %s is not private: %w",
				entry.Name(),
				err,
			)
		}
		if info.Size() <= 0 || info.Size() > terminalRetryMaximumBytes {
			return nil, fmt.Errorf(
				"terminal retry record %s has invalid size",
				entry.Name(),
			)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf(
				"read terminal retry record %s: %w",
				entry.Name(),
				err,
			)
		}
		var envelope terminalRetryEnvelope
		if err := decodeStrictJSON(raw, &envelope); err != nil {
			return nil, fmt.Errorf(
				"decode terminal retry record %s: %w",
				entry.Name(),
				err,
			)
		}
		if envelope.SchemaVersion != indicatorTerminalRetrySchemaVersion {
			return nil, fmt.Errorf(
				"terminal retry envelope %s schema = %d, want %d",
				entry.Name(),
				envelope.SchemaVersion,
				indicatorTerminalRetrySchemaVersion,
			)
		}
		hash := sha256.Sum256(envelope.Record)
		if envelope.RecordSHA256 != hex.EncodeToString(hash[:]) {
			return nil, fmt.Errorf(
				"terminal retry record %s checksum mismatch",
				entry.Name(),
			)
		}
		var record TerminalRetryRecord
		if err := decodeStrictJSON(envelope.Record, &record); err != nil {
			return nil, fmt.Errorf(
				"decode terminal retry payload %s: %w",
				entry.Name(),
				err,
			)
		}
		if err := validateTerminalRetryRecord(record); err != nil {
			return nil, fmt.Errorf(
				"validate terminal retry payload %s: %w",
				entry.Name(),
				err,
			)
		}
		if filepath.Base(s.recordPath(
			record.SessionID,
			record.Generation,
		)) != entry.Name() {
			return nil, fmt.Errorf(
				"terminal retry record %s identity mismatch",
				entry.Name(),
			)
		}
		key := record.SessionID + "\x00" +
			strconv.FormatUint(record.Generation, 10)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf(
				"terminal retry identity is duplicated: %s",
				record.SessionID,
			)
		}
		seen[key] = struct{}{}
		records = append(records, record)
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].SessionID != records[right].SessionID {
			return records[left].SessionID < records[right].SessionID
		}
		return records[left].Generation < records[right].Generation
	})
	return records, nil
}

func (s *TerminalRetryStore) Delete(
	sessionID string,
	generation uint64,
) error {
	if s == nil {
		return fmt.Errorf("terminal retry store is not configured")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || generation == 0 {
		return fmt.Errorf(
			"terminal retry session_id and generation are required",
		)
	}
	if err := os.Remove(s.recordPath(sessionID, generation)); err != nil &&
		!os.IsNotExist(err) {
		return fmt.Errorf("delete terminal retry record: %w", err)
	}
	if err := syncTerminalRetryDirectory(s.dir); err != nil {
		return fmt.Errorf("sync terminal retry directory: %w", err)
	}
	return nil
}

func (s *TerminalRetryStore) recordPath(
	sessionID string,
	generation uint64,
) string {
	identity := strings.TrimSpace(sessionID) + "\x00" +
		strconv.FormatUint(generation, 10)
	hash := sha256.Sum256([]byte(identity))
	return filepath.Join(s.dir, hex.EncodeToString(hash[:])+".json")
}

func validateTerminalRetryRecord(record TerminalRetryRecord) error {
	if record.SchemaVersion != indicatorTerminalRetrySchemaVersion {
		return fmt.Errorf(
			"terminal retry schema = %d, want %d",
			record.SchemaVersion,
			indicatorTerminalRetrySchemaVersion,
		)
	}
	if strings.TrimSpace(record.SessionID) == "" ||
		strings.TrimSpace(record.SessionID) != record.SessionID ||
		record.Generation == 0 {
		return fmt.Errorf(
			"terminal retry session_id and generation are invalid",
		)
	}
	if record.BarsProcessed < 0 {
		return fmt.Errorf("terminal retry bars_processed cannot be negative")
	}
	if !isTerminalRetryStatus(record.DesiredStatus) ||
		!isTerminalRetryStatus(record.EffectiveStatus) {
		return fmt.Errorf("terminal retry status is invalid")
	}
	if record.ExpectedStatus != "" && record.ExpectedStatus != "pending" {
		return fmt.Errorf("terminal retry expected_status is invalid")
	}
	if len(record.Reason) > 64<<10 {
		return fmt.Errorf("terminal retry reason is too large")
	}
	if record.Indicators != nil {
		if record.Indicators.SessionID != record.SessionID {
			return fmt.Errorf(
				"terminal retry indicator session_id does not match",
			)
		}
		if record.Indicators.SchemaVersion !=
			indicatorSessionCheckpointSchemaV2 {
			return fmt.Errorf(
				"terminal retry indicator checkpoint schema is invalid",
			)
		}
	}
	return nil
}

func isTerminalRetryStatus(status string) bool {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "finished", "failed", "stopped", "stop_failed", "recoverable":
		return true
	default:
		return false
	}
}

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
