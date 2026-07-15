package runtimeagent

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/grpc"
)

var startupFailureNoncePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

var safeStartupFailureMessages = map[string]struct{}{
	"worker Python invocation is invalid":                               {},
	"embedded runtime profile is invalid":                               {},
	"runtime dependency startup probe failed":                           {},
	"runtime dependency startup probe returned an invalid response":     {},
	"runtime dependency startup probe did not match the sealed profile": {},
}

func BuildRuntimeStartupFailureRequest(
	identity RuntimeIdentity,
	credential *RuntimeCredential,
	dependencyErr *RuntimeDependencyProfileError,
	issuedAt time.Time,
	nonce string,
) (*cpv1.ReportRuntimeStartupFailureRequest, error) {
	if strings.TrimSpace(identity.Source) != "self_hosted" || strings.TrimSpace(identity.RuntimeID) == "" {
		return nil, fmt.Errorf("startup failure report requires a self_hosted runtime identity")
	}
	if credential == nil || strings.TrimSpace(credential.KeyID) == "" {
		return nil, fmt.Errorf("startup failure report credential is required")
	}
	if dependencyErr == nil || dependencyErr.Code != runtimeDependencyProfileErrorCode ||
		!safeDependencyModule(dependencyErr.Module) || dependencyErr.Module == "" {
		return nil, fmt.Errorf("startup dependency error is invalid")
	}
	if _, ok := safeStartupFailureMessages[dependencyErr.Message]; !ok {
		return nil, fmt.Errorf("startup dependency error message is invalid")
	}
	if issuedAt.IsZero() || issuedAt.UnixMilli() <= 0 || !startupFailureNoncePattern.MatchString(nonce) {
		return nil, fmt.Errorf("startup failure report timestamp or nonce is invalid")
	}
	profile, err := verifiedIdentityDependencyProfile(identity)
	if err != nil {
		return nil, err
	}
	privateKey, err := loadEd25519PrivateKey(credential.PrivateKeyPEM)
	if err != nil {
		return nil, err
	}
	request := &cpv1.ReportRuntimeStartupFailureRequest{
		KeyId:          strings.TrimSpace(credential.KeyID),
		RuntimeId:      strings.TrimSpace(identity.RuntimeID),
		Source:         "self_hosted",
		IssuedAtUnixMs: issuedAt.UTC().UnixMilli(),
		Nonce:          nonce,
		DependencyError: &strategyv1.RuntimeDependencyError{
			Code:                  dependencyErr.Code,
			Module:                dependencyErr.Module,
			RuntimeProfile:        profile.GetProfileName(),
			RuntimeProfileVersion: profile.GetProfileVersion(),
			ImageBuildId:          profile.GetImageBuildId(),
			Message:               dependencyErr.Message,
		},
		ActualProfile: profile,
	}
	request.Signature = b64URLNoPad(ed25519.Sign(privateKey, canonicalRuntimeStartupFailurePayload(request)))
	return request, nil
}

func canonicalRuntimeStartupFailurePayload(request *cpv1.ReportRuntimeStartupFailureRequest) []byte {
	profile := request.GetActualProfile()
	roots := append([]string(nil), profile.GetPublicImportRoots()...)
	sort.Strings(roots)
	errorFact := request.GetDependencyError()
	value := map[string]any{
		"dependency_code":                    errorFact.GetCode(),
		"dependency_contract_sha256":         profile.GetContractSha256(),
		"dependency_error_image_build_id":    errorFact.GetImageBuildId(),
		"dependency_error_profile_name":      errorFact.GetRuntimeProfile(),
		"dependency_error_profile_version":   errorFact.GetRuntimeProfileVersion(),
		"dependency_hosted_python":           profile.GetHostedPython(),
		"dependency_image_build_id":          profile.GetImageBuildId(),
		"dependency_message":                 errorFact.GetMessage(),
		"dependency_module":                  errorFact.GetModule(),
		"dependency_profile_name":            profile.GetProfileName(),
		"dependency_profile_version":         profile.GetProfileVersion(),
		"dependency_public_import_roots":     roots,
		"dependency_schema_version":          profile.GetSchemaVersion(),
		"dependency_strategy_library_commit": profile.GetStrategyLibraryCommit(),
		"dependency_strategy_service_commit": profile.GetStrategyServiceCommit(),
		"issued_at_unix_ms":                  request.GetIssuedAtUnixMs(),
		"key_id":                             request.GetKeyId(),
		"nonce":                              request.GetNonce(),
		"runtime_id":                         request.GetRuntimeId(),
		"source":                             request.GetSource(),
	}
	body, _ := json.Marshal(value)
	return body
}

func ReportRuntimeStartupFailure(
	ctx context.Context,
	address string,
	dialOptions []grpc.DialOption,
	request *cpv1.ReportRuntimeStartupFailureRequest,
) error {
	if strings.TrimSpace(address) == "" || request == nil || len(dialOptions) == 0 {
		return fmt.Errorf("startup failure report transport is incomplete")
	}
	connection, err := grpc.DialContext(ctx, normalizeRuntimeChannelAddress(address), dialOptions...)
	if err != nil {
		return fmt.Errorf("dial startup failure report endpoint")
	}
	defer connection.Close()
	response, err := cpv1.NewControlPanelServiceClient(connection).ReportRuntimeStartupFailure(ctx, request)
	if err != nil {
		return fmt.Errorf("report runtime startup failure")
	}
	if response == nil || !response.GetRecorded() {
		return fmt.Errorf("runtime startup failure was not recorded")
	}
	return nil
}
