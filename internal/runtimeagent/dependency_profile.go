package runtimeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	strategyv1 "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/protobuf/proto"
)

const (
	runtimeDependencyProfileErrorCode = "RUNTIME_DEPENDENCY_PROFILE_INVALID"
	runtimeProbeOutputLimit           = 64 * 1024
	runtimeProbeTimeout               = 30 * time.Second
	strictSemverPattern               = `(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?`
)

var (
	lowerSHA256Pattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	commitPattern       = regexp.MustCompile(`^[0-9a-f]{40}$`)
	modulePattern       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	packagePattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	semverPattern       = regexp.MustCompile(`^` + strictSemverPattern + `$`)
	imageBuildIDPattern = regexp.MustCompile(
		`^([0-9a-f]{12})-([0-9a-f]{12})-([0-9a-f]{12})-` +
			`(` + strictSemverPattern + `)-` +
			`(executor(?:-coverage)?)(?:-dirty-[0-9a-f]{12})?$`,
	)
)

type WorkerPythonInvocation struct {
	Executable string
	ArgsPrefix []string
	WorkDir    string
	Env        []string
}

type EmbeddedRuntimeFacts struct {
	Source  string
	Profile *strategyv1.RuntimeDependencyProfile
}

type RuntimeDependencyProfileError struct {
	Code    string
	Module  string
	Message string
}

func (e *RuntimeDependencyProfileError) Error() string {
	if e == nil {
		return runtimeDependencyProfileErrorCode
	}
	if e.Module != "" {
		return fmt.Sprintf("%s: %s: %s", e.Code, e.Module, e.Message)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

type runtimeProbeRunner interface {
	Run(context.Context, WorkerPythonInvocation, []string) runtimeProbeResult
}

type runtimeProbeResult struct {
	Stdout      []byte
	Stderr      []byte
	ExitCode    int
	FailureKind string
}

type execRuntimeProbeRunner struct{}

type runtimeProbePayload struct {
	SchemaVersion       int                   `json:"schema_version"`
	OK                  bool                  `json:"ok"`
	Source              string                `json:"source"`
	PythonVersion       string                `json:"python_version"`
	DependencyProfile   runtimeProbeProfile   `json:"dependency_profile"`
	SysPrefixSHA256     string                `json:"sys_prefix_sha256"`
	SysExecutableSHA256 string                `json:"sys_executable_sha256"`
	WorkDirSHA256       string                `json:"workdir_sha256"`
	Packages            []runtimeProbePackage `json:"packages"`
	Failures            []runtimeProbeFailure `json:"failures"`
}

type runtimeProbeProfile struct {
	SchemaVersion         uint32   `json:"schema_version"`
	ProfileName           string   `json:"profile_name"`
	ProfileVersion        string   `json:"profile_version"`
	ContractSHA256        string   `json:"contract_sha256"`
	HostedPython          string   `json:"hosted_python"`
	PublicImportRoots     []string `json:"public_import_roots"`
	StrategyServiceCommit string   `json:"strategy_service_commit"`
	StrategyLibraryCommit string   `json:"strategy_library_commit"`
	ImageBuildID          string   `json:"image_build_id"`
}

type runtimeProbePackage struct {
	Distribution     string `json:"distribution"`
	Version          string `json:"version"`
	DirectURLPresent bool   `json:"direct_url_present"`
	Editable         bool   `json:"editable"`
	OriginKind       string `json:"origin_kind"`
	OriginSHA256     string `json:"origin_sha256"`
}

type runtimeProbeFailure struct {
	Code   string `json:"code"`
	Module string `json:"module"`
	Reason string `json:"reason"`
}

func LoadEmbeddedRuntimeFacts(source string, environment []string) (EmbeddedRuntimeFacts, error) {
	var facts EmbeddedRuntimeFacts
	values, err := exactEnvironmentMap(environment)
	if err != nil {
		return facts, dependencyProfileError("strategy_service.runtime_profile", "embedded runtime profile environment is invalid")
	}
	required := []string{
		"HUSHINE_RUNTIME_PROFILE_NAME",
		"HUSHINE_RUNTIME_PROFILE_VERSION",
		"HUSHINE_RUNTIME_CONTRACT_SHA256",
		"HUSHINE_RUNTIME_HOSTED_PYTHON",
		"HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS",
		"HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT",
		"HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT",
		"HUSHINE_RUNTIME_IMAGE_BUILD_ID",
	}
	for _, key := range required {
		value, ok := values[key]
		if !ok || value == "" || len(value) > 1024 || strings.ContainsAny(value, "\x00\r\n") {
			return facts, dependencyProfileError("strategy_service.runtime_profile", "embedded runtime profile environment is incomplete")
		}
	}
	roots := strings.Split(values["HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS"], ",")
	if strings.Join(roots, ",") != values["HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS"] {
		return facts, dependencyProfileError("strategy_service.runtime_profile", "embedded public import roots are invalid")
	}
	facts = EmbeddedRuntimeFacts{
		Source: source,
		Profile: &strategyv1.RuntimeDependencyProfile{
			SchemaVersion:         1,
			ProfileName:           values["HUSHINE_RUNTIME_PROFILE_NAME"],
			ProfileVersion:        values["HUSHINE_RUNTIME_PROFILE_VERSION"],
			ContractSha256:        values["HUSHINE_RUNTIME_CONTRACT_SHA256"],
			HostedPython:          values["HUSHINE_RUNTIME_HOSTED_PYTHON"],
			PublicImportRoots:     append([]string(nil), roots...),
			StrategyServiceCommit: values["HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT"],
			StrategyLibraryCommit: values["HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT"],
			ImageBuildId:          values["HUSHINE_RUNTIME_IMAGE_BUILD_ID"],
		},
	}
	if err := validateEmbeddedRuntimeFacts(facts); err != nil {
		return EmbeddedRuntimeFacts{}, dependencyProfileError("strategy_service.runtime_profile", "embedded runtime profile environment is invalid")
	}
	return EmbeddedRuntimeFacts{
		Source:  facts.Source,
		Profile: proto.Clone(facts.Profile).(*strategyv1.RuntimeDependencyProfile),
	}, nil
}

func VerifyRuntimeDependencyProfile(
	ctx context.Context,
	invocation WorkerPythonInvocation,
	expected EmbeddedRuntimeFacts,
	runner runtimeProbeRunner,
) (*strategyv1.RuntimeDependencyProfile, error) {
	if err := validateWorkerPythonInvocation(invocation); err != nil {
		return nil, dependencyProfileError("", "worker Python invocation is invalid")
	}
	if err := validateEmbeddedRuntimeFacts(expected); err != nil {
		return nil, dependencyProfileError("strategy_service.runtime_profile", "embedded runtime profile is invalid")
	}
	if runner == nil {
		runner = execRuntimeProbeRunner{}
	}
	probeCtx, cancel := context.WithTimeout(ctx, runtimeProbeTimeout)
	defer cancel()
	args := []string{
		"-m", "strategy_service.runtime_startup_probe", "verify",
		"--source", expected.Source,
		"--expected-invocation-sha256", hashText(invocation.Executable),
		"--expected-workdir-sha256", hashText(invocation.WorkDir),
		"--json",
	}
	result := runner.Run(probeCtx, cloneWorkerPythonInvocation(invocation), args)
	if result.FailureKind != "" || len(result.Stderr) != 0 {
		return nil, dependencyProfileError("strategy_service.runtime_startup_probe", "runtime dependency startup probe failed")
	}
	if result.ExitCode != 0 {
		return nil, dependencyProfileError(
			reportedRuntimeProbeFailureModule(result.Stdout, invocation, expected.Source),
			"runtime dependency startup probe failed",
		)
	}
	payload, err := parseRuntimeProbePayload(result.Stdout)
	if err != nil {
		return nil, dependencyProfileError("strategy_service.runtime_startup_probe", "runtime dependency startup probe returned an invalid response")
	}
	profile, module, err := validateRuntimeProbePayload(payload, invocation, expected)
	if err != nil {
		return nil, dependencyProfileError(module, "runtime dependency startup probe did not match the sealed profile")
	}
	return profile, nil
}

func dependencyProfileError(module, message string) *RuntimeDependencyProfileError {
	if !safeDependencyModule(module) {
		module = ""
	}
	return &RuntimeDependencyProfileError{
		Code:    runtimeDependencyProfileErrorCode,
		Module:  module,
		Message: message,
	}
}

func safeDependencyModule(value string) bool {
	return len(value) <= 128 && (value == "" || modulePattern.MatchString(value) || packagePattern.MatchString(value))
}

func reportedRuntimeProbeFailureModule(
	body []byte,
	invocation WorkerPythonInvocation,
	expectedSource string,
) string {
	payload, err := parseRuntimeProbePayload(body)
	if err != nil || payload.SchemaVersion != 1 || payload.OK || payload.Source != expectedSource ||
		!regexp.MustCompile(`^3\.13\.[0-9]+$`).MatchString(payload.PythonVersion) ||
		payload.SysPrefixSHA256 != expectedWorkerPrefixSHA256(invocation.Executable) ||
		payload.SysExecutableSHA256 != hashText(invocation.Executable) ||
		payload.WorkDirSHA256 != hashText(invocation.WorkDir) || len(payload.Failures) == 0 {
		return "strategy_service.runtime_startup_probe"
	}
	for _, failure := range payload.Failures {
		if failure.Module != "" && safeDependencyModule(failure.Module) {
			return failure.Module
		}
	}
	return "strategy_service.runtime_startup_probe"
}

func validateWorkerPythonInvocation(invocation WorkerPythonInvocation) error {
	if invocation.Executable == "" || invocation.WorkDir == "" ||
		!filepath.IsAbs(invocation.Executable) || !filepath.IsAbs(invocation.WorkDir) ||
		filepath.Clean(invocation.Executable) != invocation.Executable ||
		filepath.Clean(invocation.WorkDir) != invocation.WorkDir {
		return errors.New("invocation paths must be absolute")
	}
	if len(invocation.ArgsPrefix) == 0 || invocation.ArgsPrefix[0] != "-I" {
		return errors.New("isolated Python prefix is required")
	}
	remaining := invocation.ArgsPrefix[1:]
	if slices.Contains(remaining, "-I") {
		return errors.New("isolated Python prefix must be unique")
	}
	if len(remaining) > 0 && remaining[0] == "-Xfrozen_modules=off" {
		remaining = remaining[1:]
	}
	if len(remaining) != 0 {
		if err := validateTrustedCoveragePythonArgs(remaining); err != nil {
			return errors.New("worker Python prefix is not an approved resolved prefix")
		}
	}
	for _, item := range append(append([]string(nil), invocation.ArgsPrefix...), invocation.Env...) {
		if strings.ContainsAny(item, "\x00\r\n") {
			return errors.New("invocation contains control characters")
		}
	}
	return validateResolvedWorkerBaseEnvironment(invocation.Env)
}

func validateResolvedWorkerBaseEnvironment(environment []string) error {
	values, err := exactEnvironmentMap(environment)
	if err != nil {
		return errors.New("worker base environment is invalid")
	}
	if values["GRPC_ENABLE_FORK_SUPPORT"] != "0" ||
		values["PYTHONUNBUFFERED"] != "1" ||
		values["PYTHONDONTWRITEBYTECODE"] != "1" ||
		values["PATH"] == "" {
		return errors.New("worker base environment is incomplete")
	}
	allowed := map[string]struct{}{
		"GRPC_ENABLE_FORK_SUPPORT": {},
		"PATH":                     {},
		"PYTHONUNBUFFERED":         {},
		"PYTHONDONTWRITEBYTECODE":  {},
	}
	if runtime.GOOS == "windows" {
		for _, key := range []string{"SYSTEMROOT", "WINDIR", "COMSPEC", "PATHEXT"} {
			allowed[key] = struct{}{}
		}
	}
	for key := range embeddedRuntimeEnvironmentKeys {
		allowed[key] = struct{}{}
	}
	for key, value := range values {
		if _, ok := allowed[key]; !ok || value == "" || len(value) > 4096 || strings.ContainsAny(value, "\x00\r\n") {
			return errors.New("worker base environment contains an unapproved value")
		}
	}
	return nil
}

func validateEmbeddedRuntimeFacts(expected EmbeddedRuntimeFacts) error {
	if expected.Source != "hosted" && expected.Source != "self_hosted" && expected.Source != "bare" {
		return errors.New("invalid source")
	}
	profile := expected.Profile
	if profile == nil || profile.GetSchemaVersion() != 1 ||
		profile.GetProfileName() != "platform-python-3.13" ||
		!semverPattern.MatchString(profile.GetProfileVersion()) ||
		!lowerSHA256Pattern.MatchString(profile.GetContractSha256()) ||
		profile.GetHostedPython() != "3.13" {
		return errors.New("invalid profile identity")
	}
	roots := profile.GetPublicImportRoots()
	if len(roots) == 0 || !sort.StringsAreSorted(roots) {
		return errors.New("invalid public roots")
	}
	for index, root := range roots {
		if !modulePattern.MatchString(root) || (index > 0 && roots[index-1] == root) {
			return errors.New("invalid public roots")
		}
	}
	serviceCommit := profile.GetStrategyServiceCommit()
	libraryCommit := profile.GetStrategyLibraryCommit()
	buildID := profile.GetImageBuildId()
	local := serviceCommit == "local-dev" || libraryCommit == "local-dev" || buildID == "local-dev"
	if local {
		if expected.Source != "bare" || serviceCommit != "local-dev" || libraryCommit != "local-dev" || buildID != "local-dev" {
			return errors.New("invalid local profile")
		}
		return nil
	}
	if !commitPattern.MatchString(serviceCommit) || !commitPattern.MatchString(libraryCommit) || len(buildID) > 96 || !isASCIIText(buildID) {
		return errors.New("invalid build identity")
	}
	match := imageBuildIDPattern.FindStringSubmatch(buildID)
	if match == nil || match[1] != serviceCommit[:12] || match[2] != libraryCommit[:12] || match[4] != profile.GetProfileVersion() {
		return errors.New("invalid build identity")
	}
	return nil
}

func isASCIIText(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range []byte(value) {
		if char < 0x21 || char > 0x7e {
			return false
		}
	}
	return true
}

func parseRuntimeProbePayload(body []byte) (runtimeProbePayload, error) {
	var payload runtimeProbePayload
	if len(body) == 0 || len(body) > runtimeProbeOutputLimit || !utf8.Valid(body) || body[len(body)-1] != '\n' {
		return payload, errors.New("invalid probe body")
	}
	if err := rejectDuplicateJSONKeys(body); err != nil {
		return payload, err
	}
	var generic any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&generic); err != nil {
		return payload, err
	}
	canonical, err := json.Marshal(generic)
	if err != nil || !bytes.Equal(body, append(canonical, '\n')) {
		return payload, errors.New("probe body is not canonical")
	}
	if err := validateRuntimeProbeJSONShape(generic); err != nil {
		return payload, err
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return payload, err
	}
	return payload, nil
}

func validateRuntimeProbeJSONShape(value any) error {
	top, ok := value.(map[string]any)
	if !ok || !exactMapKeys(top, []string{
		"dependency_profile", "failures", "ok", "packages", "python_version",
		"schema_version", "source", "sys_executable_sha256", "sys_prefix_sha256", "workdir_sha256",
	}) {
		return errors.New("invalid top-level shape")
	}
	profile, ok := top["dependency_profile"].(map[string]any)
	if !ok || !exactMapKeys(profile, []string{
		"contract_sha256", "hosted_python", "image_build_id", "profile_name",
		"profile_version", "public_import_roots", "schema_version",
		"strategy_library_commit", "strategy_service_commit",
	}) {
		return errors.New("invalid profile shape")
	}
	packages, ok := top["packages"].([]any)
	if !ok {
		return errors.New("invalid packages shape")
	}
	for _, item := range packages {
		record, ok := item.(map[string]any)
		if !ok || !exactMapKeys(record, []string{
			"direct_url_present", "distribution", "editable", "origin_kind", "origin_sha256", "version",
		}) {
			return errors.New("invalid package shape")
		}
	}
	failures, ok := top["failures"].([]any)
	if !ok {
		return errors.New("invalid failures shape")
	}
	for _, item := range failures {
		record, ok := item.(map[string]any)
		if !ok || !exactMapKeys(record, []string{"code", "module", "reason"}) {
			return errors.New("invalid failure shape")
		}
	}
	return nil
}

func exactMapKeys(value map[string]any, keys []string) bool {
	if len(value) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := value[key]; !ok {
			return false
		}
	}
	return true
}

func rejectDuplicateJSONKeys(body []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := readUniqueJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON")
		}
		return err
	}
	return nil
}

func readUniqueJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if _, exists := seen[key]; exists {
				return errors.New("duplicate object key")
			}
			seen[key] = struct{}{}
			if err := readUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("invalid object ending")
		}
	case '[':
		for decoder.More() {
			if err := readUniqueJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("invalid array ending")
		}
	default:
		return errors.New("invalid JSON delimiter")
	}
	return nil
}

func validateRuntimeProbePayload(
	payload runtimeProbePayload,
	invocation WorkerPythonInvocation,
	expected EmbeddedRuntimeFacts,
) (*strategyv1.RuntimeDependencyProfile, string, error) {
	if payload.SchemaVersion != 1 || !payload.OK || payload.Source != expected.Source ||
		!regexp.MustCompile(`^3\.13\.[0-9]+$`).MatchString(payload.PythonVersion) ||
		payload.SysPrefixSHA256 != expectedWorkerPrefixSHA256(invocation.Executable) ||
		payload.SysExecutableSHA256 != hashText(invocation.Executable) ||
		payload.WorkDirSHA256 != hashText(invocation.WorkDir) || len(payload.Failures) != 0 {
		return nil, "strategy_service.runtime_startup_probe", errors.New("invalid probe facts")
	}
	actual := &strategyv1.RuntimeDependencyProfile{
		SchemaVersion:         payload.DependencyProfile.SchemaVersion,
		ProfileName:           payload.DependencyProfile.ProfileName,
		ProfileVersion:        payload.DependencyProfile.ProfileVersion,
		ContractSha256:        payload.DependencyProfile.ContractSHA256,
		HostedPython:          payload.DependencyProfile.HostedPython,
		PublicImportRoots:     append([]string(nil), payload.DependencyProfile.PublicImportRoots...),
		StrategyServiceCommit: payload.DependencyProfile.StrategyServiceCommit,
		StrategyLibraryCommit: payload.DependencyProfile.StrategyLibraryCommit,
		ImageBuildId:          payload.DependencyProfile.ImageBuildID,
	}
	if err := validateEmbeddedRuntimeFacts(EmbeddedRuntimeFacts{Source: expected.Source, Profile: actual}); err != nil || !proto.Equal(actual, expected.Profile) {
		return nil, "strategy_service.runtime_profile", errors.New("profile mismatch")
	}
	if len(payload.Packages) != 2 {
		return nil, "importlib.metadata", errors.New("package count mismatch")
	}
	wantDistributions := []string{"hushine-strategy-library", "hushine-strategy-service"}
	for index, item := range payload.Packages {
		if item.Distribution != wantDistributions[index] || !packagePattern.MatchString(item.Version) ||
			!lowerSHA256Pattern.MatchString(item.OriginSHA256) {
			return nil, wantDistributions[index], errors.New("invalid package facts")
		}
		if expected.Source == "bare" {
			if (item.Editable && item.OriginKind != "editable") || (!item.Editable && item.OriginKind != "venv-site") {
				return nil, item.Distribution, errors.New("invalid bare package origin")
			}
		} else if item.Editable || item.OriginKind != "venv-site" {
			return nil, item.Distribution, errors.New("invalid sealed package origin")
		}
	}
	return proto.Clone(actual).(*strategyv1.RuntimeDependencyProfile), "", nil
}

func cloneWorkerPythonInvocation(value WorkerPythonInvocation) WorkerPythonInvocation {
	return WorkerPythonInvocation{
		Executable: value.Executable,
		ArgsPrefix: append([]string(nil), value.ArgsPrefix...),
		WorkDir:    value.WorkDir,
		Env:        append([]string(nil), value.Env...),
	}
}

func hashText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func expectedWorkerPrefixSHA256(executable string) string {
	prefix := filepath.Clean(filepath.Dir(filepath.Dir(executable)))
	if resolved, err := filepath.EvalSymlinks(prefix); err == nil {
		prefix = filepath.Clean(resolved)
	}
	return hashText(prefix)
}

type boundedPipeResult struct {
	body     []byte
	overflow bool
}

func (execRuntimeProbeRunner) Run(
	ctx context.Context,
	invocation WorkerPythonInvocation,
	args []string,
) runtimeProbeResult {
	argv := append(append([]string(nil), invocation.ArgsPrefix...), args...)
	cmd := exec.Command(invocation.Executable, argv...)
	cmd.Dir = invocation.WorkDir
	cmd.Env = append([]string(nil), invocation.Env...)
	cmd.Stdin = nil
	configureRuntimeProbeCommand(cmd)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return runtimeProbeResult{FailureKind: "launch"}
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return runtimeProbeResult{FailureKind: "launch"}
	}
	if err := cmd.Start(); err != nil {
		return runtimeProbeResult{FailureKind: "launch"}
	}

	stdoutDone := make(chan boundedPipeResult, 1)
	stderrDone := make(chan boundedPipeResult, 1)
	go func() { stdoutDone <- readBoundedProbePipe(stdout) }()
	go func() { stderrDone <- readBoundedProbePipe(stderr) }()
	waitDone := make(chan error, 1)
	go func() { waitDone <- cmd.Wait() }()

	var stdoutResult, stderrResult boundedPipeResult
	var waitErr error
	stdoutReady, stderrReady, waitReady := false, false, false
	failureKind := ""
	stopped := false
	for !stdoutReady || !stderrReady || !waitReady {
		select {
		case stdoutResult = <-stdoutDone:
			stdoutReady = true
			if stdoutResult.overflow && failureKind == "" {
				failureKind = "overflow"
			}
		case stderrResult = <-stderrDone:
			stderrReady = true
			if stderrResult.overflow && failureKind == "" {
				failureKind = "overflow"
			}
		case waitErr = <-waitDone:
			waitReady = true
		case <-ctx.Done():
			if failureKind == "" {
				failureKind = "timeout"
			}
		}
		if failureKind != "" && !stopped && !waitReady {
			stopped = true
			_ = terminateRuntimeProbeProcess(cmd.Process)
			timer := time.NewTimer(250 * time.Millisecond)
			select {
			case waitErr = <-waitDone:
				waitReady = true
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				_ = killRuntimeProbeProcess(cmd.Process)
			}
		}
	}
	exitCode := 0
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else if failureKind == "" {
			failureKind = "wait"
		}
	}
	return runtimeProbeResult{
		Stdout:      stdoutResult.body,
		Stderr:      stderrResult.body,
		ExitCode:    exitCode,
		FailureKind: failureKind,
	}
}

func readBoundedProbePipe(reader io.Reader) boundedPipeResult {
	limited := io.LimitReader(reader, runtimeProbeOutputLimit+1)
	var buffer bytes.Buffer
	_, _ = io.Copy(&buffer, limited)
	body := buffer.Bytes()
	if len(body) > runtimeProbeOutputLimit {
		return boundedPipeResult{body: append([]byte(nil), body[:runtimeProbeOutputLimit]...), overflow: true}
	}
	return boundedPipeResult{body: append([]byte(nil), body...)}
}
