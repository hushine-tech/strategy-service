package runtimeagent

import (
	"testing"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
	workerpb "github.com/hushine-tech/strategy-service/gen/runtimeworkerv1"
	strategypb "github.com/hushine-tech/strategy-service/gen/strategyv1"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func requireTask7Message(t *testing.T, file protoreflect.FileDescriptor, name protoreflect.Name) protoreflect.MessageDescriptor {
	t.Helper()
	message := file.Messages().ByName(name)
	if message == nil {
		t.Fatalf("%s.%s is missing", file.Package(), name)
	}
	return message
}

func assertTask7Fields(t *testing.T, message protoreflect.MessageDescriptor, expected map[protoreflect.Name]protoreflect.FieldNumber) {
	t.Helper()
	if message.Fields().Len() != len(expected) {
		t.Fatalf("%s has %d fields, want %d", message.FullName(), message.Fields().Len(), len(expected))
	}
	for name, number := range expected {
		field := message.Fields().ByName(name)
		if field == nil {
			t.Fatalf("%s.%s is missing", message.FullName(), name)
		}
		if field.Number() != number {
			t.Fatalf("%s.%s tag = %d, want %d", message.FullName(), name, field.Number(), number)
		}
	}
}

func assertTask7MessageField(t *testing.T, message protoreflect.MessageDescriptor, name protoreflect.Name, number protoreflect.FieldNumber, target protoreflect.FullName) {
	t.Helper()
	field := message.Fields().ByName(name)
	if field == nil {
		t.Fatalf("%s.%s is missing", message.FullName(), name)
	}
	if field.Number() != number || field.Message() == nil || field.Message().FullName() != target {
		t.Fatalf("%s.%s = tag %d type %v, want tag %d type %s", message.FullName(), name, field.Number(), field.Message(), number, target)
	}
}

func assertTask7Method(t *testing.T, file protoreflect.FileDescriptor, serviceName, methodName protoreflect.Name, input, output protoreflect.FullName) {
	t.Helper()
	service := file.Services().ByName(serviceName)
	if service == nil {
		t.Fatalf("%s.%s is missing", file.Package(), serviceName)
	}
	method := service.Methods().ByName(methodName)
	if method == nil {
		t.Fatalf("%s.%s is missing", service.FullName(), methodName)
	}
	if method.Input().FullName() != input || method.Output().FullName() != output {
		t.Fatalf("%s input/output = %s/%s, want %s/%s", method.FullName(), method.Input().FullName(), method.Output().FullName(), input, output)
	}
}

func TestRuntimeChannelProtoAvailableToGoAgent(t *testing.T) {
	frame := &cpv1.RuntimeFrame{
		FrameType: cpv1.FrameType_FRAME_TYPE_HELLO,
		Payload: &cpv1.RuntimeFrame_Hello{
			Hello: &cpv1.RuntimeHello{
				Source:    "bare",
				UserId:    6,
				RuntimeId: "bare-6-test",
				Name:      "bare-debug-6-test",
			},
		},
	}

	if frame.GetHello().GetRuntimeId() != "bare-6-test" {
		t.Fatalf("runtime id = %q", frame.GetHello().GetRuntimeId())
	}
}

func TestRuntimeDependencyChannelProto(t *testing.T) {
	strategyFile := strategypb.File_strategy_service_proto
	profile := requireTask7Message(t, strategyFile, "RuntimeDependencyProfile")
	assertTask7Fields(t, profile, map[protoreflect.Name]protoreflect.FieldNumber{
		"schema_version": 1, "profile_name": 2, "profile_version": 3,
		"contract_sha256": 4, "hosted_python": 5, "public_import_roots": 6,
		"strategy_service_commit": 7, "strategy_library_commit": 8, "image_build_id": 9,
	})
	if profile.Fields().ByName("public_import_roots").Cardinality() != protoreflect.Repeated {
		t.Fatal("RuntimeDependencyProfile.public_import_roots must be repeated")
	}
	dependencyError := requireTask7Message(t, strategyFile, "RuntimeDependencyError")
	assertTask7Fields(t, dependencyError, map[protoreflect.Name]protoreflect.FieldNumber{
		"code": 1, "module": 2, "runtime_profile": 3,
		"runtime_profile_version": 4, "image_build_id": 5, "message": 6,
	})
	validationIssue := requireTask7Message(t, strategyFile, "StrategyValidationIssueProto")
	assertTask7Fields(t, validationIssue, map[protoreflect.Name]protoreflect.FieldNumber{
		"code": 1, "message": 2, "module": 3, "line": 4, "symbol": 5,
	})
	validateRequest := requireTask7Message(t, strategyFile, "ValidateStrategySourceRequest")
	assertTask7Fields(t, validateRequest, map[protoreflect.Name]protoreflect.FieldNumber{
		"source": 1, "user_id": 100, "runtime_id": 101, "include_declarations": 102,
	})
	if validateRequest.Fields().ByName("include_declarations").Kind() != protoreflect.BoolKind {
		t.Fatal("ValidateStrategySourceRequest.include_declarations must be bool")
	}
	validateResponse := requireTask7Message(t, strategyFile, "ValidateStrategySourceResponse")
	assertTask7Fields(t, validateResponse, map[protoreflect.Name]protoreflect.FieldNumber{
		"ok": 1, "issues": 2, "runtime_profile": 3,
		"declared_inputs": 4, "declared_order_targets": 5,
	})
	assertTask7MessageField(t, validateResponse, "runtime_profile", 3, "strategy.v1.RuntimeDependencyProfile")
	assertTask7MessageField(t, validateResponse, "declared_inputs", 4, "strategy.v1.StrategyInputDeclaration")
	assertTask7MessageField(t, validateResponse, "declared_order_targets", 5, "strategy.v1.StrategyOrderTargetBinding")
	if validateResponse.Fields().ByName("declared_inputs").Cardinality() != protoreflect.Repeated {
		t.Fatal("ValidateStrategySourceResponse.declared_inputs must be repeated")
	}
	if validateResponse.Fields().ByName("declared_order_targets").Cardinality() != protoreflect.Repeated {
		t.Fatal("ValidateStrategySourceResponse.declared_order_targets must be repeated")
	}
	assertTask7Method(t, strategyFile, "StrategyService", "ValidateStrategySource", "strategy.v1.ValidateStrategySourceRequest", "strategy.v1.ValidateStrategySourceResponse")
	stopRequest := requireTask7Message(t, strategyFile, "StopStrategyRequest")
	assertTask7Fields(t, stopRequest, map[protoreflect.Name]protoreflect.FieldNumber{
		"session_id": 1, "stop_action": 3, "operation_id": 4,
		"user_id": 100, "runtime_id": 101,
	})
	stopResponse := requireTask7Message(t, strategyFile, "StopStrategyResponse")
	assertTask7Fields(t, stopResponse, map[protoreflect.Name]protoreflect.FieldNumber{
		"stopped": 1, "status": 2, "code": 3, "target_results": 4,
		"reconciliation_run_id": 5, "operation_id": 6,
	})

	profileValue := dynamicpb.NewMessage(profile)
	profileName := profile.Fields().ByName("profile_name")
	profileValue.Set(profileName, protoreflect.ValueOfString("platform-python-3.13"))
	if got := profileValue.Get(profileName).String(); got != "platform-python-3.13" {
		t.Fatalf("dynamic profile_name = %q", got)
	}

	workerFile := workerpb.File_runtime_worker_proto
	for _, item := range []struct {
		message protoreflect.Name
		tag     protoreflect.FieldNumber
	}{
		{"SessionProgress", 6},
		{"PlatformCallResult", 5},
		{"FinalStatus", 5},
		{"WorkerError", 5},
	} {
		assertTask7MessageField(t, requireTask7Message(t, workerFile, item.message), "dependency_error", item.tag, "strategy.v1.RuntimeDependencyError")
	}
	workerFrame := requireTask7Message(t, workerFile, "WorkerFrame")
	v2Indicator := workerFrame.Fields().ByName("indicator_frame_v2")
	incomeBatchField := requireTask7Message(t, workerFile, "AgentFrame").Fields().ByName("income_batch")
	workerDataAckField := workerFrame.Fields().ByName("data_ack")
	workerHello := requireTask7Message(t, workerFile, "WorkerHello")
	protocolVersion := workerHello.Fields().ByName("protocol_version")
	if workerFrame.Fields().ByName("indicator_frame") != nil {
		t.Fatal("legacy WorkerFrame.indicator_frame remains")
	}
	if protocolVersion == nil || protocolVersion.Number() != 5 ||
		v2Indicator == nil || v2Indicator.Number() != 21 {
		t.Fatal("sealed Indicator V2 tags are invalid")
	}
	if incomeBatchField == nil || incomeBatchField.Number() != 18 ||
		incomeBatchField.Message() == nil || incomeBatchField.Message().FullName() != "runtime.worker.v1.IncomeBatch" {
		t.Fatal("AgentFrame.income_batch must be tag 18 IncomeBatch")
	}
	assertTask7Fields(t, requireTask7Message(t, workerFile, "IncomeBatch"), map[protoreflect.Name]protoreflect.FieldNumber{
		"session_id": 1, "stream_key": 2, "sequence": 3, "entries": 4,
	})
	if workerDataAckField == nil || workerDataAckField.Number() != 22 ||
		workerDataAckField.Message() == nil || workerDataAckField.Message().FullName() != "runtime.worker.v1.WorkerDataAck" {
		t.Fatal("WorkerFrame.data_ack must be tag 22 WorkerDataAck")
	}
	assertTask7Fields(t, requireTask7Message(t, workerFile, "WorkerDataAck"), map[protoreflect.Name]protoreflect.FieldNumber{
		"session_id": 1, "stream_key": 2, "sequence": 3,
	})
	if !workerFrame.ReservedRanges().Has(15) ||
		!workerFrame.ReservedNames().Has("indicator_frame") {
		t.Fatal("sealed Indicator V2 must reserve legacy indicator tag and name")
	}
	for _, name := range []protoreflect.Name{"IndicatorValue", "IndicatorFrame"} {
		if workerFile.Messages().ByName(name) != nil {
			t.Fatalf("legacy worker message remains: %s", name)
		}
	}
	finalStatus := requireTask7Message(t, workerFile, "FinalStatus")
	if reconciliation := finalStatus.Fields().ByName("reconciliation_run_id"); reconciliation != nil && reconciliation.Number() != 6 {
		t.Fatalf("FinalStatus.reconciliation_run_id tag = %d, want 6", reconciliation.Number())
	}

	controlFile := cpv1.File_control_panel_service_proto
	incomeFrame := requireTask7Message(t, controlFile, "RuntimeFrame").Fields().ByName("income_batch")
	if incomeFrame == nil || incomeFrame.Number() != 31 || incomeFrame.Message() == nil ||
		incomeFrame.Message().FullName() != "controlpanel.v1.RuntimeIncomeBatch" {
		t.Fatal("RuntimeFrame.income_batch must be tag 31 RuntimeIncomeBatch")
	}
	assertTask7MessageField(t, requireTask7Message(t, controlFile, "RuntimeHello"), "dependency_profile", 15, "strategy.v1.RuntimeDependencyProfile")
	assertTask7MessageField(t, requireTask7Message(t, controlFile, "RuntimeResume"), "dependency_profile", 4, "strategy.v1.RuntimeDependencyProfile")
	streamError := requireTask7Message(t, controlFile, "StreamError")
	assertTask7MessageField(t, streamError, "dependency_error", 3, "strategy.v1.RuntimeDependencyError")
	errorDetailJSON := streamError.Fields().ByName("error_detail_json")
	if errorDetailJSON == nil || errorDetailJSON.Number() != 4 || errorDetailJSON.Kind() != protoreflect.StringKind {
		t.Fatalf("StreamError.error_detail_json = %v, want string tag 4", errorDetailJSON)
	}
	startupRequest := requireTask7Message(t, controlFile, "ReportRuntimeStartupFailureRequest")
	assertTask7Fields(t, startupRequest, map[protoreflect.Name]protoreflect.FieldNumber{
		"key_id": 1, "runtime_id": 2, "source": 3, "issued_at_unix_ms": 4,
		"nonce": 5, "dependency_error": 6, "actual_profile": 7, "signature": 8,
	})
	assertTask7MessageField(t, startupRequest, "dependency_error", 6, "strategy.v1.RuntimeDependencyError")
	assertTask7MessageField(t, startupRequest, "actual_profile", 7, "strategy.v1.RuntimeDependencyProfile")
	assertTask7Fields(t, requireTask7Message(t, controlFile, "ReportRuntimeStartupFailureResponse"), map[protoreflect.Name]protoreflect.FieldNumber{"recorded": 1})
	assertTask7Method(t, controlFile, "ControlPanelService", "ValidateStrategySource", "strategy.v1.ValidateStrategySourceRequest", "strategy.v1.ValidateStrategySourceResponse")
	assertTask7Method(t, controlFile, "ControlPanelService", "ReportRuntimeStartupFailure", "controlpanel.v1.ReportRuntimeStartupFailureRequest", "controlpanel.v1.ReportRuntimeStartupFailureResponse")
}
