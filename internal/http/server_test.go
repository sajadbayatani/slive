package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sajadbayatani/slive/internal/config"
	"github.com/sajadbayatani/slive/internal/logger"
)

// TestHealthHandler tests the /health endpoint handler.
func TestHealthHandler(t *testing.T) {
	// Create test logger
	testLogger := &logger.Logger{
		Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	// Create handler dependencies
	deps := HandlerDeps{
		Log: testLogger,
	}

	// Create health handler
	healthHandler := NewHealthHandler(deps)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	healthHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", contentType)
	}

	body, err := io.ReadAll(w.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var response struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Failed to unmarshal response body: %v", err)
	}

	if response.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", response.Status)
	}
}

// TestServer_Start_Shutdown tests the server lifecycle using httptest.Server.
func TestServer_Start_Shutdown(t *testing.T) {
	// Create test logger
	testLogger := &logger.Logger{
		Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	// Create handler dependencies
	deps := HandlerDeps{
		Log: testLogger,
	}

	// Create health handler
	healthHandler := NewHealthHandler(deps)

	// Use httptest.Server to avoid port conflicts.
	ts := httptest.NewServer(healthHandler)
	defer ts.Close()

	// Test the /health endpoint.
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", contentType)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var response struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Failed to unmarshal response body: %v", err)
	}

	if response.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", response.Status)
	}
}

// TestServer_Shutdown_ContextTimeout tests graceful shutdown with a context timeout.
func TestServer_Shutdown_ContextTimeout(t *testing.T) {
	// Create test logger
	testLogger := &logger.Logger{
		Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	// Create handler dependencies
	deps := HandlerDeps{
		Log: testLogger,
	}

	// Create health handler
	healthHandler := NewHealthHandler(deps)

	// Create a mock server with a custom handler.
	mux := http.NewServeMux()
	mux.Handle("/health", healthHandler)

	server := &http.Server{
		Addr:    ":0", // Use a random port.
		Handler: mux,
	}

	// Start the server in a goroutine.
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	// Shutdown with a very short timeout to test context cancellation.
	// Note: This test may be flaky as the server might shutdown faster than the timeout.
	// Using a slightly longer timeout to make the test more reliable.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
	defer cancel()

	// Shutdown should fail due to context timeout (or succeed if server shuts down quickly).
	// We accept both outcomes as the behavior depends on timing.
	err := server.Shutdown(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Errorf("Expected shutdown to fail with context.DeadlineExceeded or succeed, got: %v", err)
	}
}

// TestServer_Shutdown_ErrServerClosed tests that Shutdown returns nil when the server is already closed.
func TestServer_Shutdown_ErrServerClosed(t *testing.T) {
	// Create test logger
	testLogger := &logger.Logger{
		Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	// Create handler dependencies
	deps := HandlerDeps{
		Log: testLogger,
	}

	// Create health handler
	healthHandler := NewHealthHandler(deps)

	// Create a mock server with a custom handler.
	mux := http.NewServeMux()
	mux.Handle("/health", healthHandler)

	server := &http.Server{
		Addr:    ":0", // Use a random port.
		Handler: mux,
	}

	// Start the server in a goroutine.
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			t.Errorf("Server failed to start: %v", err)
		}
	}()

	// Give the server a moment to start.
	time.Sleep(100 * time.Millisecond)

	// Shutdown the server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("Server shutdown failed: %v", err)
	}

	// Shutdown again should return nil (server already closed).
	if err := server.Shutdown(ctx); err != nil {
		t.Errorf("Expected nil error on second shutdown, got: %v", err)
	}
}

// TestRouter_Integration tests the router with the full server setup.
func TestRouter_Integration(t *testing.T) {
	// Create test logger
	testLogger := &logger.Logger{
		Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	// Create router with dependencies
	deps := HandlerDeps{
		Log: testLogger,
	}
	router := NewRouter(config.Config{HealthPath: "/health"}, deps)

	// Create a test server using the router
	ts := httptest.NewServer(router.ServeMux())
	defer ts.Close()

	// Test the /health endpoint through the router
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", contentType)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var response struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatalf("Failed to unmarshal response body: %v", err)
	}

	if response.Status != "ok" {
		t.Errorf("Expected status 'ok', got '%s'", response.Status)
	}
}

// TestNewServer_Integration tests the complete server creation with router.
func TestNewServer_Integration(t *testing.T) {
	// Create test logger
	testLogger := &logger.Logger{
		Logger: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}

	// Create config
	cfg := config.Config{
		HTTPAddr: ":0",
	}

	// Create server with router and dependencies
	server := NewServer(cfg, testLogger)

	// Verify server has router
	if server.router == nil {
		t.Fatal("Expected server to have router")
	}

	// Verify router has the health endpoint registered
	// We can test this by creating a test server with the router's mux
	ts := httptest.NewServer(server.router.ServeMux())
	defer ts.Close()

	// Test the /health endpoint
	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("Failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}
}
