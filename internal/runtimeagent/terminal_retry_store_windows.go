//go:build windows

package runtimeagent

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func secureTerminalRetryPath(path string, directory bool) error {
	userSID, err := terminalRetryCurrentUserSID()
	if err != nil {
		return err
	}
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read owner: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("read owner SID: %w", err)
	}
	if owner == nil || !owner.Equals(userSID) {
		return fmt.Errorf("path is not owned by the runtime user")
	}
	inheritance := uint32(windows.NO_INHERITANCE)
	if directory {
		inheritance = windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT
	}
	acl, err := windows.ACLFromEntries(
		[]windows.EXPLICIT_ACCESS{{
			AccessPermissions: windows.GENERIC_ALL,
			AccessMode:        windows.SET_ACCESS,
			Inheritance:       inheritance,
			Trustee: windows.TRUSTEE{
				TrusteeForm:  windows.TRUSTEE_IS_SID,
				TrusteeType:  windows.TRUSTEE_IS_USER,
				TrusteeValue: windows.TrusteeValueFromSID(userSID),
			},
		}},
		nil,
	)
	if err != nil {
		return fmt.Errorf("build private DACL: %w", err)
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|
			windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		acl,
		nil,
	); err != nil {
		return fmt.Errorf("set private DACL: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	return validateTerminalRetryPathSecurity(path, info.Mode(), 0)
}

func validateTerminalRetryPathSecurity(
	path string,
	_ os.FileMode,
	_ os.FileMode,
) error {
	userSID, err := terminalRetryCurrentUserSID()
	if err != nil {
		return err
	}
	descriptor, err := windows.GetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|
			windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		return fmt.Errorf("read security descriptor: %w", err)
	}
	owner, _, err := descriptor.Owner()
	if err != nil {
		return fmt.Errorf("read owner SID: %w", err)
	}
	if owner == nil || !owner.Equals(userSID) {
		return fmt.Errorf("owner is not the runtime user")
	}
	control, _, err := descriptor.Control()
	if err != nil {
		return fmt.Errorf("read DACL control: %w", err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		return fmt.Errorf("DACL inherits permissions")
	}
	acl, _, err := descriptor.DACL()
	if err != nil {
		return fmt.Errorf("read DACL: %w", err)
	}
	if acl == nil || acl.AceCount != 1 {
		return fmt.Errorf("DACL must contain exactly one runtime-user ACE")
	}
	var ace *windows.ACCESS_ALLOWED_ACE
	if err := windows.GetAce(acl, 0, &ace); err != nil {
		return fmt.Errorf("read DACL ACE: %w", err)
	}
	if ace == nil ||
		ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE ||
		ace.Mask&windows.GENERIC_ALL == 0 {
		return fmt.Errorf("runtime-user ACE does not grant full access")
	}
	aceSID := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
	if !aceSID.Equals(userSID) {
		return fmt.Errorf("DACL grants access to a different principal")
	}
	return nil
}

func terminalRetryCurrentUserSID() (*windows.SID, error) {
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return nil, fmt.Errorf("read runtime user token: %w", err)
	}
	if user == nil || user.User.Sid == nil {
		return nil, fmt.Errorf("runtime user token has no SID")
	}
	return user.User.Sid, nil
}

func replaceTerminalRetryFile(source string, destination string) error {
	sourceUTF16, err := windows.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	destinationUTF16, err := windows.UTF16PtrFromString(destination)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(
		sourceUTF16,
		destinationUTF16,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func syncTerminalRetryDirectory(string) error {
	// Save uses MoveFileEx(MOVEFILE_WRITE_THROUGH), which is Windows' durable
	// atomic replacement primitive. Delete is idempotent; a crash before its
	// directory metadata reaches disk can only replay an already-safe retry.
	return nil
}
