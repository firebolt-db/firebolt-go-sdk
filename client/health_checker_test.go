package client

import (
	"sync"
	"testing"
	"time"
)

func TestHealthChecker_UpdateIPs(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	hc.UpdateIPs([]string{"10.0.0.1", "10.0.0.2"})

	hc.mu.RLock()
	if len(hc.status) != 2 {
		t.Errorf("expected 2 IPs, got %d", len(hc.status))
	}
	if !hc.status["10.0.0.1"] || !hc.status["10.0.0.2"] {
		t.Error("new IPs should be healthy by default")
	}
	hc.mu.RUnlock()

	// Adding new IPs while keeping existing ones.
	hc.UpdateIPs([]string{"10.0.0.1", "10.0.0.2", "10.0.0.3"})
	hc.mu.RLock()
	if len(hc.status) != 3 {
		t.Errorf("expected 3 IPs, got %d", len(hc.status))
	}
	hc.mu.RUnlock()

	// Removing IPs that are no longer in DNS.
	hc.UpdateIPs([]string{"10.0.0.3"})
	hc.mu.RLock()
	if len(hc.status) != 1 {
		t.Errorf("expected 1 IP after removal, got %d", len(hc.status))
	}
	if _, exists := hc.status["10.0.0.1"]; exists {
		t.Error("10.0.0.1 should have been removed")
	}
	hc.mu.RUnlock()
}

func TestHealthChecker_UpdateIPsPreservesExistingStatus(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	hc.UpdateIPs([]string{"10.0.0.1", "10.0.0.2"})
	hc.ReportDialFailure("10.0.0.1")

	// Re-update with same IPs: unhealthy status must persist.
	hc.UpdateIPs([]string{"10.0.0.1", "10.0.0.2"})
	hc.mu.RLock()
	if hc.status["10.0.0.1"] {
		t.Error("10.0.0.1 should still be unhealthy after UpdateIPs")
	}
	if !hc.status["10.0.0.2"] {
		t.Error("10.0.0.2 should still be healthy")
	}
	hc.mu.RUnlock()
}

func TestHealthChecker_FilterHealthy_MultipleIPs(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	all := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	hc.UpdateIPs(all)
	hc.ReportDialFailure("10.0.0.2")

	healthy := hc.FilterHealthy(all)
	if len(healthy) != 2 {
		t.Fatalf("expected 2 healthy IPs, got %d: %v", len(healthy), healthy)
	}
	for _, ip := range healthy {
		if ip == "10.0.0.2" {
			t.Error("unhealthy IP 10.0.0.2 should be filtered out")
		}
	}
}

func TestHealthChecker_FilterHealthy_SingleIP(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	single := []string{"10.0.0.1"}
	hc.UpdateIPs(single)
	hc.ReportDialFailure("10.0.0.1")

	result := hc.FilterHealthy(single)
	if len(result) != 1 || result[0] != "10.0.0.1" {
		t.Errorf("single unhealthy IP should still be returned, got: %v", result)
	}
}

func TestHealthChecker_FilterHealthy_AllUnhealthy(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	all := []string{"10.0.0.1", "10.0.0.2"}
	hc.UpdateIPs(all)
	hc.ReportDialFailure("10.0.0.1")
	hc.ReportDialFailure("10.0.0.2")

	result := hc.FilterHealthy(all)
	if len(result) != 2 {
		t.Errorf("when all unhealthy, full list should be returned; got %d IPs", len(result))
	}
}

func TestHealthChecker_FilterHealthy_DynamicIPCount(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	// Start with 1 IP: filter is bypassed.
	hc.UpdateIPs([]string{"10.0.0.1"})
	hc.ReportDialFailure("10.0.0.1")
	result := hc.FilterHealthy([]string{"10.0.0.1"})
	if len(result) != 1 {
		t.Fatalf("single IP must always be returned; got %d", len(result))
	}

	// Scale to 5 IPs: filtering active, 10.0.0.1 is still unhealthy.
	five := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4", "10.0.0.5"}
	hc.UpdateIPs(five)
	result = hc.FilterHealthy(five)
	if len(result) != 4 {
		t.Fatalf("expected 4 healthy IPs, got %d: %v", len(result), result)
	}

	// Scale back to 1 IP: filter is bypassed again.
	hc.UpdateIPs([]string{"10.0.0.1"})
	result = hc.FilterHealthy([]string{"10.0.0.1"})
	if len(result) != 1 {
		t.Fatalf("single IP must always be returned after scale-down; got %d", len(result))
	}
}

func TestHealthChecker_BackgroundProbe(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	var mu sync.Mutex
	probeResults := map[string]bool{
		"10.0.0.1": true,
		"10.0.0.2": false,
	}
	hc.probeFunc = func(ip string) bool {
		mu.Lock()
		defer mu.Unlock()
		return probeResults[ip]
	}

	hc.UpdateIPs([]string{"10.0.0.1", "10.0.0.2"})
	hc.Start()
	defer hc.Stop()

	// Wait for at least one probe cycle.
	time.Sleep(150 * time.Millisecond)

	hc.mu.RLock()
	if !hc.status["10.0.0.1"] {
		t.Error("10.0.0.1 should be healthy")
	}
	if hc.status["10.0.0.2"] {
		t.Error("10.0.0.2 should be unhealthy after probe")
	}
	hc.mu.RUnlock()

	// Make 10.0.0.2 recover.
	mu.Lock()
	probeResults["10.0.0.2"] = true
	mu.Unlock()

	time.Sleep(150 * time.Millisecond)

	hc.mu.RLock()
	if !hc.status["10.0.0.2"] {
		t.Error("10.0.0.2 should have recovered")
	}
	hc.mu.RUnlock()
}

func TestHealthChecker_StopIsIdempotent(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}
	hc.Start()
	hc.Stop()
	hc.Stop() // second call must not panic or block
}

func TestHealthChecker_ReportDialFailure(t *testing.T) {
	hc, err := NewHealthChecker("http://placeholder:8122/", 1*time.Hour)
	if err != nil {
		t.Fatalf("NewHealthChecker: %v", err)
	}

	hc.UpdateIPs([]string{"10.0.0.1", "10.0.0.2"})

	hc.ReportDialFailure("10.0.0.1")
	hc.mu.RLock()
	if hc.status["10.0.0.1"] {
		t.Error("10.0.0.1 should be unhealthy after dial failure")
	}
	if !hc.status["10.0.0.2"] {
		t.Error("10.0.0.2 should be unaffected")
	}
	hc.mu.RUnlock()

	// Reporting for unknown IP is a no-op.
	hc.ReportDialFailure("10.0.0.99")
	hc.mu.RLock()
	if _, exists := hc.status["10.0.0.99"]; exists {
		t.Error("unknown IP should not be added")
	}
	hc.mu.RUnlock()
}
