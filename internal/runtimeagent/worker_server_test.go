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

func TestForgetWorkerIdentityPreservesReusedPIDWithNewToken(t *testing.T) {
	reg := NewSessionRegistry()
	if err := reg.ExpectWorker("sess-1", "old-token"); err != nil {
		t.Fatal(err)
	}
	if err := reg.AdmitWorker("sess-1", "old-token", 123); err != nil {
		t.Fatal(err)
	}
	reg.ForgetWorker("sess-1")
	if err := reg.ExpectWorker("sess-1", "new-token"); err != nil {
		t.Fatal(err)
	}
	if err := reg.AdmitWorker("sess-1", "new-token", 123); err != nil {
		t.Fatal(err)
	}

	reg.ForgetWorkerIdentity("sess-1", 123, "old-token")

	worker, ok := reg.ActiveWorker("sess-1")
	if !ok || worker.PID != 123 {
		t.Fatalf("replacement worker = (%+v, %v), want reused pid 123 preserved", worker, ok)
	}
}
