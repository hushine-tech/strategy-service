package runtimeagent

import (
	"testing"

	cpv1 "github.com/hushine-tech/strategy-service/gen/controlpanelv1"
)

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
