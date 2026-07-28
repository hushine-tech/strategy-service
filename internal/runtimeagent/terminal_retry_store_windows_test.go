//go:build windows

package runtimeagent

import (
	"testing"

	"golang.org/x/sys/windows"
)

func TestTerminalRetryStoreRejectsWindowsDACLGrantToAnotherPrincipal(
	t *testing.T,
) {
	store, err := NewTerminalRetryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	record := TerminalRetryRecord{
		SchemaVersion:   indicatorTerminalRetrySchemaVersion,
		SessionID:       "sess-windows-dacl",
		Generation:      41,
		DesiredStatus:   "recoverable",
		EffectiveStatus: "recoverable",
		Reason:          "retry",
	}
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
	userSID, err := terminalRetryCurrentUserSID()
	if err != nil {
		t.Fatal(err)
	}
	everyoneSID, err := windows.CreateWellKnownSid(windows.WinWorldSid)
	if err != nil {
		t.Fatal(err)
	}
	access := []windows.EXPLICIT_ACCESS{
		{
			AccessPermissions: windows.GENERIC_ALL,
			AccessMode:        windows.SET_ACCESS,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_USER,
				TrusteeValue: windows.TrusteeValueFromSID(userSID),
			},
		},
		{
			AccessPermissions: windows.GENERIC_READ,
			AccessMode:        windows.GRANT_ACCESS,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_WELL_KNOWN_GROUP,
				TrusteeValue: windows.TrusteeValueFromSID(everyoneSID),
			},
		},
	}
	acl, err := windows.ACLFromEntries(access, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := windows.SetNamedSecurityInfo(
		store.recordPath(record.SessionID, record.Generation),
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		acl,
		nil,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := store.LoadAll(); err == nil {
		t.Fatal("LoadAll accepted a retry journal readable by Everyone")
	}
}
