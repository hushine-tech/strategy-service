package runtimeagent

import "testing"

func TestWorkerAdmissionRejectsBadToken(t *testing.T) {
	reg := NewSessionRegistry()
	reg.ExpectWorker("sess-1", "good-token")

	err := reg.AdmitWorker("sess-1", "bad-token", 123)
	if err == nil {
		t.Fatalf("bad token was accepted")
	}
}

func TestWorkerAdmissionAcceptsExpectedTokenOnce(t *testing.T) {
	reg := NewSessionRegistry()
	reg.ExpectWorker("sess-1", "good-token")

	if err := reg.AdmitWorker("sess-1", "good-token", 123); err != nil {
		t.Fatalf("AdmitWorker: %v", err)
	}
	if err := reg.AdmitWorker("sess-1", "good-token", 456); err == nil {
		t.Fatalf("token was accepted twice")
	}
}
