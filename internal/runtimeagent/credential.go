package runtimeagent

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const DefaultRuntimeCredentialPath = "/etc/hushine/runtime.cred"

func LoadRuntimeCredential(path string) (*RuntimeCredential, error) {
	if strings.TrimSpace(path) == "" {
		if inline := strings.TrimSpace(os.Getenv("RUNTIME_CREDENTIAL_JSON")); inline != "" {
			return runtimeCredentialFromJSON([]byte(inline), "env:RUNTIME_CREDENTIAL_JSON")
		}
		path = firstNonEmpty(os.Getenv("RUNTIME_CREDENTIAL_PATH"), DefaultRuntimeCredentialPath)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read runtime credential: %w", err)
	}
	return runtimeCredentialFromJSON(body, path)
}

func runtimeCredentialFromJSON(body []byte, source string) (*RuntimeCredential, error) {
	var raw struct {
		Version       int    `json:"version"`
		KeyID         string `json:"key_id"`
		PrivateKeyPEM string `json:"private_key_pem"`
		ClientCertPEM string `json:"client_cert_pem"`
		ClientKeyPEM  string `json:"client_key_pem"`
		ServerCAPEM   string `json:"server_ca_pem"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse runtime credential %s: %w", source, err)
	}
	if raw.Version != 1 {
		return nil, fmt.Errorf("runtime credential version must be 1")
	}
	if strings.TrimSpace(raw.KeyID) == "" {
		return nil, fmt.Errorf("runtime credential key_id is required")
	}
	if strings.TrimSpace(raw.PrivateKeyPEM) == "" {
		return nil, fmt.Errorf("runtime credential private_key_pem is required")
	}
	return &RuntimeCredential{
		KeyID:         strings.TrimSpace(raw.KeyID),
		PrivateKeyPEM: raw.PrivateKeyPEM,
		ClientCertPEM: raw.ClientCertPEM,
		ClientKeyPEM:  raw.ClientKeyPEM,
		ServerCAPEM:   raw.ServerCAPEM,
	}, nil
}
