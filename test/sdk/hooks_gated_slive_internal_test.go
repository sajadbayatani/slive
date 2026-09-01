//go:build slive_internal

package sdk

import (
	"testing"

	"github.com/sajadbayatani/slive/pkg/slive"
)

// Compile-time assertions that hooks are present under slive_internal.
var _ interface{ ResetMetrics() } = (*slive.Handler)(nil)
var _ interface{ ResetGCReapedCount() } = (*slive.Handler)(nil)
var _ interface{ ArmGhostForTest(string, string) } = (*slive.Handler)(nil)
var _ interface{ ReapGhostForTest(string, string) } = (*slive.Handler)(nil)

func TestSDK_HandlerHooksGated(t *testing.T) {
	client := newSTUNFreeClient(t)
	h := client.Handler()
	if h == nil {
		t.Fatal("Handler is nil")
	}
	// Presence check via type assertion
	if _, ok := any(h).(interface{ ResetMetrics() }); !ok {
		t.Error("Handler should expose ResetMetrics with slive_internal")
	}
	if _, ok := any(h).(interface{ ArmGhostForTest(string, string) }); !ok {
		t.Error("Handler should expose ArmGhostForTest with slive_internal")
	}
	if _, ok := any(h).(interface{ ReapGhostForTest(string, string) }); !ok {
		t.Error("Handler should expose ReapGhostForTest with slive_internal")
	}
	// Ensure they are callable without panic
	h.ResetMetrics()
	h.ResetGCReapedCount()
}
