package runtimeagent

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	"google.golang.org/protobuf/proto"
)

func TestRuntimeStartupFailureSignatureCoversEverySafeFact(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	credential := &RuntimeCredential{
		KeyID:         "key-1",
		PrivateKeyPEM: testEd25519PrivateKeyPEM(t, privateKey),
	}
	identity := RuntimeIdentity{
		Source:            "self_hosted",
		RuntimeID:         "runtime-1",
		DependencyProfile: validEmbeddedRuntimeFacts("self_hosted").Profile,
	}
	dependencyErr := &RuntimeDependencyProfileError{
		Code:    runtimeDependencyProfileErrorCode,
		Module:  "hushine_strategy.runtime_dependencies",
		Message: "runtime dependency startup probe failed",
	}
	issuedAt := time.Date(2026, 7, 16, 1, 2, 3, 0, time.UTC)
	request, err := BuildRuntimeStartupFailureRequest(
		identity, credential, dependencyErr, issuedAt, "nonce-123",
	)
	if err != nil {
		t.Fatalf("BuildRuntimeStartupFailureRequest: %v", err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(request.GetSignature())
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(publicKey, canonicalRuntimeStartupFailurePayload(request), signature) {
		t.Fatal("startup failure signature is invalid")
	}

	mutations := map[string]func(*cpv1.ReportRuntimeStartupFailureRequest){
		"code": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.DependencyError.Code = "MUTATED"
		},
		"module": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.DependencyError.Module = "mutated.module"
		},
		"message": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.DependencyError.Message = "mutated"
		},
		"error-profile": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.DependencyError.RuntimeProfile = "mutated"
		},
		"profile": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.ActualProfile.ContractSha256 = "f" + value.ActualProfile.ContractSha256[1:]
		},
		"source": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.Source = "hosted"
		},
		"runtime-id": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.RuntimeId = "runtime-2"
		},
		"issued-at": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.IssuedAtUnixMs++
		},
		"nonce": func(value *cpv1.ReportRuntimeStartupFailureRequest) {
			value.Nonce = "nonce-456"
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(request).(*cpv1.ReportRuntimeStartupFailureRequest)
			mutate(changed)
			if ed25519.Verify(publicKey, canonicalRuntimeStartupFailurePayload(changed), signature) {
				t.Fatal("signature remained valid after mutation")
			}
		})
	}
}

func TestRuntimeStartupFailureRequestRejectsUnsafeOrPartialInput(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	credential := &RuntimeCredential{KeyID: "key-1", PrivateKeyPEM: testEd25519PrivateKeyPEM(t, privateKey)}
	identity := RuntimeIdentity{
		Source: "self_hosted", RuntimeID: "runtime-1",
		DependencyProfile: validEmbeddedRuntimeFacts("self_hosted").Profile,
	}
	for name, dependencyErr := range map[string]*RuntimeDependencyProfileError{
		"nil":            nil,
		"wrong-code":     {Code: "OTHER", Module: "grpc", Message: "failed"},
		"unsafe-module":  {Code: runtimeDependencyProfileErrorCode, Module: "/tmp/secret", Message: "failed"},
		"unsafe-message": {Code: runtimeDependencyProfileErrorCode, Module: "grpc", Message: "failed\nsecret"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildRuntimeStartupFailureRequest(identity, credential, dependencyErr, time.Now(), "nonce"); err == nil {
				t.Fatal("unsafe startup failure input was accepted")
			}
		})
	}
}

func testEd25519PrivateKeyPEM(t *testing.T, privateKey ed25519.PrivateKey) string {
	t.Helper()
	raw, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: raw}))
}
