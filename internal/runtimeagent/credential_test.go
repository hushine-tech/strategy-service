package runtimeagent

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRuntimeCredentialReadsVersionOneJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.cred")
	if err := os.WriteFile(path, []byte(`{
	  "version": 1,
	  "key_id": "key-1",
	  "private_key_pem": "pem-body",
	  "client_cert_pem": "cert-body",
	  "client_key_pem": "key-body",
	  "server_ca_pem": "ca-body"
	}`), 0o600); err != nil {
		t.Fatalf("write credential: %v", err)
	}

	cred, err := LoadRuntimeCredential(path)
	if err != nil {
		t.Fatalf("LoadRuntimeCredential: %v", err)
	}
	if cred.KeyID != "key-1" || cred.PrivateKeyPEM != "pem-body" {
		t.Fatalf("credential = %+v", cred)
	}
	if cred.ClientCertPEM != "cert-body" || cred.ClientKeyPEM != "key-body" || cred.ServerCAPEM != "ca-body" {
		t.Fatalf("credential tls bundle = %+v", cred)
	}
}
