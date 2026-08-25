package logger

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestNew tests the New function for logger initialization.
func TestNew(t *testing.T) {
	// Capture stdout to verify logger output.
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	log := New()
	if log == nil {
		t.Fatal("Expected logger to be non-nil")
	}

	// Write a test log entry.
	log.Info("test message", "key", "value")

	// Restore stdout.
	w.Close()
	os.Stdout = oldStdout

	// Read the output.
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("Failed to read output: %v", err)
	}

	// Verify the output is valid JSON.
	output := buf.String()
	if !strings.Contains(output, `"level":"INFO"`) {
		t.Errorf("Expected output to contain 'level:INFO', got: %s", output)
	}
	if !strings.Contains(output, `"msg":"test message"`) {
		t.Errorf("Expected output to contain 'msg:test message', got: %s", output)
	}
	if !strings.Contains(output, `"key":"value"`) {
		t.Errorf("Expected output to contain 'key:value', got: %s", output)
	}
}

// TestLogger_Levels tests logging at different levels.
func TestLogger_Levels(t *testing.T) {
	// Use a custom handler to capture output.
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	log := &Logger{Logger: slog.New(handler)}

	if log == nil {
		t.Fatal("Expected logger to be non-nil")
	}

	log.Info("info message")
	log.Error("error message")

	output := buf.String()
	if !strings.Contains(output, `"level":"INFO"`) {
		t.Errorf("Expected output to contain 'level:INFO', got: %s", output)
	}
	if !strings.Contains(output, `"level":"ERROR"`) {
		t.Errorf("Expected output to contain 'level:ERROR', got: %s", output)
	}
}

// TestLogger_WithHandler tests logger initialization with a custom handler.
func TestLogger_WithHandler(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	log := &Logger{Logger: slog.New(handler)}

	if log == nil {
		t.Fatal("Expected logger to be non-nil")
	}

	log.Info("custom handler test")

	output := buf.String()
	if !strings.Contains(output, `"msg":"custom handler test"`) {
		t.Errorf("Expected output to contain 'msg:custom handler test', got: %s", output)
	}
}
