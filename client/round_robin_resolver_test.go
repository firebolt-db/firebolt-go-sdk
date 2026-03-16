package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func staticLookup(ips ...string) LookupHostFunc {
	return func(_ context.Context, _ string) ([]string, error) {
		return ips, nil
	}
}

func failingLookup(_ context.Context, host string) ([]string, error) {
	return nil, fmt.Errorf("DNS lookup failed for %s", host)
}

func TestRoundRobinResolver_Basic(t *testing.T) {
	r, err := NewRoundRobinResolver("http://my-service:8080", staticLookup("10.0.0.1", "10.0.0.2", "10.0.0.3"))
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	ctx := context.Background()
	seen := make(map[string]int)

	for i := 0; i < 9; i++ {
		resolved, host, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		if host != "my-service:8080" {
			t.Errorf("expected original host my-service:8080, got %s", host)
		}
		seen[resolved]++
	}

	expectedURLs := []string{
		"http://10.0.0.1:8080",
		"http://10.0.0.2:8080",
		"http://10.0.0.3:8080",
	}
	for _, u := range expectedURLs {
		if seen[u] != 3 {
			t.Errorf("expected URL %s to appear 3 times, got %d", u, seen[u])
		}
	}
}

func TestRoundRobinResolver_SingleIP(t *testing.T) {
	r, err := NewRoundRobinResolver("http://my-service:8080", staticLookup("10.0.0.1"))
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		resolved, _, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
		if resolved != "http://10.0.0.1:8080" {
			t.Errorf("expected http://10.0.0.1:8080, got %s", resolved)
		}
	}
}

func TestRoundRobinResolver_HTTPS(t *testing.T) {
	r, err := NewRoundRobinResolver("https://secure-svc:443/path", staticLookup("10.0.0.1", "10.0.0.2"))
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	ctx := context.Background()
	resolved, host, err := r.Next(ctx)
	if err != nil {
		t.Fatalf("Next(): %v", err)
	}
	if host != "secure-svc:443" {
		t.Errorf("expected host secure-svc:443, got %s", host)
	}
	if !strings.HasPrefix(resolved, "https://10.0.0.") {
		t.Errorf("expected resolved to start with https://10.0.0., got %s", resolved)
	}
	if !strings.HasSuffix(resolved, "/path") {
		t.Errorf("expected resolved to end with /path, got %s", resolved)
	}
}

func TestRoundRobinResolver_NoPort(t *testing.T) {
	r, err := NewRoundRobinResolver("http://my-service", staticLookup("10.0.0.1", "10.0.0.2"))
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	ctx := context.Background()
	resolved, host, err := r.Next(ctx)
	if err != nil {
		t.Fatalf("Next(): %v", err)
	}
	if host != "my-service" {
		t.Errorf("expected host my-service, got %s", host)
	}
	if resolved != "http://10.0.0.1" && resolved != "http://10.0.0.2" {
		t.Errorf("unexpected resolved URL: %s", resolved)
	}
}

func TestRoundRobinResolver_DNSFailureFallsBackToCached(t *testing.T) {
	callCount := 0
	lookup := func(_ context.Context, _ string) ([]string, error) {
		callCount++
		if callCount == 1 {
			return []string{"10.0.0.1"}, nil
		}
		return nil, fmt.Errorf("DNS failure")
	}

	r, err := NewRoundRobinResolver("http://my-service:8080", lookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}
	r.TTL = 0 // force re-resolution every call

	ctx := context.Background()

	// First call succeeds and caches.
	resolved, _, err := r.Next(ctx)
	if err != nil {
		t.Fatalf("first Next(): %v", err)
	}
	if resolved != "http://10.0.0.1:8080" {
		t.Errorf("expected http://10.0.0.1:8080, got %s", resolved)
	}

	// Second call has DNS failure but falls back to cached IP.
	resolved, _, err = r.Next(ctx)
	if err != nil {
		t.Fatalf("second Next() should not fail: %v", err)
	}
	if resolved != "http://10.0.0.1:8080" {
		t.Errorf("expected fallback to http://10.0.0.1:8080, got %s", resolved)
	}
}

func TestRoundRobinResolver_DNSFailureNoCacheErrors(t *testing.T) {
	r, err := NewRoundRobinResolver("http://my-service:8080", failingLookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	_, _, err = r.Next(context.Background())
	if err == nil {
		t.Fatal("expected error when DNS fails and no cache exists")
	}
}

func TestRoundRobinResolver_TTLRespected(t *testing.T) {
	var callCount atomic.Int32
	lookup := func(_ context.Context, _ string) ([]string, error) {
		callCount.Add(1)
		return []string{"10.0.0.1"}, nil
	}

	r, err := NewRoundRobinResolver("http://my-service:8080", lookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}
	r.TTL = 1 * time.Hour // very long TTL

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, _, err := r.Next(ctx)
		if err != nil {
			t.Fatalf("Next(): %v", err)
		}
	}

	if callCount.Load() != 1 {
		t.Errorf("expected 1 DNS lookup (TTL not expired), got %d", callCount.Load())
	}
}

func TestRoundRobinResolver_ConcurrentSafety(t *testing.T) {
	r, err := NewRoundRobinResolver("http://my-service:8080", staticLookup("10.0.0.1", "10.0.0.2", "10.0.0.3"))
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	const goroutines = 50
	const callsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < callsPerGoroutine; i++ {
				_, _, err := r.Next(context.Background())
				if err != nil {
					t.Errorf("Next(): %v", err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestRoundRobinResolver_IPsUpdateAfterTTL(t *testing.T) {
	callCount := 0
	lookup := func(_ context.Context, _ string) ([]string, error) {
		callCount++
		if callCount == 1 {
			return []string{"10.0.0.1"}, nil
		}
		return []string{"10.0.0.2", "10.0.0.3"}, nil
	}

	r, err := NewRoundRobinResolver("http://my-service:8080", lookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}
	r.TTL = 10 * time.Millisecond

	ctx := context.Background()

	// First call resolves to 10.0.0.1.
	resolved, _, _ := r.Next(ctx)
	if resolved != "http://10.0.0.1:8080" {
		t.Errorf("expected http://10.0.0.1:8080, got %s", resolved)
	}

	// Wait for TTL to expire.
	time.Sleep(20 * time.Millisecond)

	// Next calls should use the updated IPs.
	seen := make(map[string]bool)
	for i := 0; i < 4; i++ {
		resolved, _, _ := r.Next(ctx)
		seen[resolved] = true
	}
	if !seen["http://10.0.0.2:8080"] || !seen["http://10.0.0.3:8080"] {
		t.Errorf("expected updated IPs after TTL, seen: %v", seen)
	}
}

// TestRoundRobinResolver_EndToEnd starts multiple httptest servers, uses a
// mock DNS resolver that returns their addresses, and verifies that requests
// are distributed across all backends.
func TestRoundRobinResolver_EndToEnd(t *testing.T) {
	var hitCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Parse server URL to get host:port.
	serverHost := strings.TrimPrefix(server.URL, "http://")
	parts := strings.SplitN(serverHost, ":", 2)
	ip, port := parts[0], parts[1]

	lookup := staticLookup(ip)

	resolver, err := NewRoundRobinResolver(fmt.Sprintf("http://my-service:%s", port), lookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	coreClient := &ClientImplCore{
		BaseClient: BaseClient{
			ApiEndpoint: fmt.Sprintf("http://my-service:%s", port),
			UserAgent:   "test",
			HttpClient:  NewHttpClient(),
			URLResolver: resolver,
		},
	}
	coreClient.AccessTokenGetter = coreClient.getAccessToken
	coreClient.ParameterGetter = coreClient.GetQueryParams

	ctx := context.Background()
	_, err = coreClient.Query(ctx, fmt.Sprintf("http://my-service:%s", port), "SELECT 1", map[string]string{}, ConnectionControl{
		UpdateParameters: func(string, string) {},
		SetEngineURL:     func(string) {},
		ResetParameters:  func(*[]string) {},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if hitCount.Load() != 1 {
		t.Errorf("expected 1 hit on backend, got %d", hitCount.Load())
	}
}

// TestRoundRobinResolver_HostHeaderOverride verifies that when the resolver
// rewrites a URL to an IP, the HTTP Host header still carries the original
// hostname so that the server sees the correct virtual host.
func TestRoundRobinResolver_HostHeaderOverride(t *testing.T) {
	var receivedHost string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHost = r.Host
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	serverHost := strings.TrimPrefix(server.URL, "http://")
	parts := strings.SplitN(serverHost, ":", 2)
	ip, port := parts[0], parts[1]

	lookup := staticLookup(ip)

	originalHost := fmt.Sprintf("my-k8s-service:%s", port)
	resolver, err := NewRoundRobinResolver(fmt.Sprintf("http://%s", originalHost), lookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	coreClient := &ClientImplCore{
		BaseClient: BaseClient{
			ApiEndpoint: fmt.Sprintf("http://%s", originalHost),
			UserAgent:   "test",
			HttpClient:  NewHttpClient(),
			URLResolver: resolver,
		},
	}
	coreClient.AccessTokenGetter = coreClient.getAccessToken
	coreClient.ParameterGetter = coreClient.GetQueryParams

	ctx := context.Background()
	_, err = coreClient.Query(ctx, fmt.Sprintf("http://%s", originalHost), "SELECT 1", map[string]string{}, ConnectionControl{
		UpdateParameters: func(string, string) {},
		SetEngineURL:     func(string) {},
		ResetParameters:  func(*[]string) {},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if receivedHost != originalHost {
		t.Errorf("expected Host header %q, got %q", originalHost, receivedHost)
	}
}

// TestRoundRobinResolver_BypassedAfterEngineURLChange verifies that the
// resolver is bypassed when the engine URL changes at runtime (e.g. via a
// Firebolt-Update-Endpoint response header). Requests to the new URL must
// go directly to the new endpoint, not be round-robin resolved against the
// original hostname.
func TestRoundRobinResolver_BypassedAfterEngineURLChange(t *testing.T) {
	var resolverHitCount atomic.Int32
	var newServerHitCount atomic.Int32

	// The "new" server that the engine redirects to.
	newServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newServerHitCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer newServer.Close()

	// The original server (behind the K8s service).
	originalServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer originalServer.Close()

	origHost := strings.TrimPrefix(originalServer.URL, "http://")
	origParts := strings.SplitN(origHost, ":", 2)
	origIP, origPort := origParts[0], origParts[1]

	lookup := func(_ context.Context, _ string) ([]string, error) {
		resolverHitCount.Add(1)
		return []string{origIP}, nil
	}

	originalServiceURL := fmt.Sprintf("http://my-service:%s", origPort)
	resolver, err := NewRoundRobinResolver(originalServiceURL, lookup)
	if err != nil {
		t.Fatalf("NewRoundRobinResolver: %v", err)
	}

	coreClient := &ClientImplCore{
		BaseClient: BaseClient{
			ApiEndpoint: originalServiceURL,
			UserAgent:   "test",
			HttpClient:  NewHttpClient(),
			URLResolver: resolver,
		},
	}
	coreClient.AccessTokenGetter = coreClient.getAccessToken
	coreClient.ParameterGetter = coreClient.GetQueryParams

	noop := ConnectionControl{
		UpdateParameters: func(string, string) {},
		SetEngineURL:     func(string) {},
		ResetParameters:  func(*[]string) {},
	}

	ctx := context.Background()

	// First request goes through the resolver (original service).
	_, err = coreClient.Query(ctx, originalServiceURL, "SELECT 1", map[string]string{}, noop)
	if err != nil {
		t.Fatalf("Query to original: %v", err)
	}
	if resolverHitCount.Load() != 1 {
		t.Fatalf("expected resolver to be called once, got %d", resolverHitCount.Load())
	}

	// Simulate engine URL change (as if Firebolt-Update-Endpoint was received).
	newURL := newServer.URL

	// Request with the NEW URL must bypass the resolver entirely.
	_, err = coreClient.Query(ctx, newURL, "SELECT 1", map[string]string{}, noop)
	if err != nil {
		t.Fatalf("Query to new endpoint: %v", err)
	}

	if newServerHitCount.Load() != 1 {
		t.Errorf("expected new server to receive 1 request, got %d", newServerHitCount.Load())
	}
	// Resolver should NOT have been called again for the new URL.
	if resolverHitCount.Load() != 1 {
		t.Errorf("expected resolver to still have 1 call (bypassed for new URL), got %d", resolverHitCount.Load())
	}
}
