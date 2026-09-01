//go:build !slive_internal

package sdk

import (
	"reflect"
	"testing"
)

// TestSDK_HandlerHooksGated verifies Client.Handler() does not expose
// test hooks without slive_internal. Compile-time presence is asserted in the
// tagged file.
func TestSDK_HandlerHooksGated(t *testing.T) {
	client := newSTUNFreeClient(t)
	h := client.Handler()
	if h == nil {
		t.Fatal("Handler is nil")
	}
	typ := reflect.TypeOf(h)
	for _, name := range []string{"ResetMetrics", "ResetGCReapedCount", "ArmGhostForTest", "ReapGhostForTest"} {
		if _, ok := typ.MethodByName(name); ok {
			t.Errorf("Handler exposes %s without slive_internal", name)
		}
	}
	if _, ok := any(h).(interface{ ResetMetrics() }); ok {
		t.Error("Handler implements ResetMetrics without slive_internal")
	}
}
