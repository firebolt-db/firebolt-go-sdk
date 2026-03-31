package client

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const (
	defaultHCInterval = 5 * time.Second
	hcProbeTimeout    = 2 * time.Second
)

// HealthChecker periodically probes resolved IPs via HTTP to determine
// which are healthy. Unhealthy IPs are filtered out by the resolver's
// Next() method so queries are only sent to responsive nodes.
//
// The checker also accepts dial-failure reports which immediately mark
// an IP as unhealthy without waiting for the next probe cycle.
type HealthChecker struct {
	hcURL    *url.URL
	client   *http.Client
	interval time.Duration

	mu     sync.RWMutex
	status map[string]bool // ip -> healthy

	// probeFunc overrides the default HTTP probe; used by tests.
	probeFunc func(ip string) bool

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewHealthChecker creates a health checker that will probe each IP by
// replacing the host in hcURL with the IP. For example, if hcURL is
// "http://placeholder:8122/" and an IP is "10.0.0.1", the probe URL
// becomes "http://10.0.0.1:8122/".
//
// interval controls how often probes run; zero or negative values fall
// back to defaultHCInterval (5s).
func NewHealthChecker(rawHCURL string, interval time.Duration) (*HealthChecker, error) {
	parsed, err := url.Parse(rawHCURL)
	if err != nil {
		return nil, err
	}
	if interval <= 0 {
		interval = defaultHCInterval
	}
	hc := &HealthChecker{
		hcURL: parsed,
		client: &http.Client{
			Timeout: hcProbeTimeout,
			Transport: &http.Transport{
				DialContext: (&net.Dialer{Timeout: hcProbeTimeout}).DialContext,
			},
		},
		interval: interval,
		status:   make(map[string]bool),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	return hc, nil
}

// Start launches the background probe goroutine.
func (hc *HealthChecker) Start() {
	go hc.run()
}

// Stop signals the background goroutine to exit and blocks until it
// has finished. Safe to call multiple times.
func (hc *HealthChecker) Stop() {
	hc.stopOnce.Do(func() {
		close(hc.stopCh)
		<-hc.doneCh
	})
}

func (hc *HealthChecker) run() {
	defer close(hc.doneCh)
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			hc.checkAll()
		case <-hc.stopCh:
			return
		}
	}
}

func (hc *HealthChecker) checkAll() {
	hc.mu.RLock()
	ips := make([]string, 0, len(hc.status))
	for ip := range hc.status {
		ips = append(ips, ip)
	}
	hc.mu.RUnlock()

	for _, ip := range ips {
		select {
		case <-hc.stopCh:
			return
		default:
		}
		healthy := hc.probe(ip)
		hc.mu.Lock()
		if prev, exists := hc.status[ip]; exists {
			hc.status[ip] = healthy
			if prev != healthy {
				if healthy {
					logging.Infolog.Printf("health check: %s is now healthy", ip)
				} else {
					logging.Infolog.Printf("health check: %s is now unhealthy", ip)
				}
			}
		}
		hc.mu.Unlock()
	}
}

func (hc *HealthChecker) probe(ip string) bool {
	if hc.probeFunc != nil {
		return hc.probeFunc(ip)
	}
	u := *hc.hcURL
	port := hc.hcURL.Port()
	if port != "" {
		u.Host = net.JoinHostPort(ip, port)
	} else {
		u.Host = ip
	}

	ctx, cancel := context.WithTimeout(context.Background(), hcProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return false
	}
	resp, err := hc.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// UpdateIPs syncs the tracked IP set with the latest DNS results.
// New IPs are optimistically marked healthy; IPs that disappeared from
// DNS are removed from the status map.
func (hc *HealthChecker) UpdateIPs(ips []string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	current := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		current[ip] = struct{}{}
		if _, exists := hc.status[ip]; !exists {
			hc.status[ip] = true
		}
	}
	for ip := range hc.status {
		if _, exists := current[ip]; !exists {
			delete(hc.status, ip)
		}
	}
}

// FilterHealthy returns only the healthy IPs from the input slice.
// When there is at most 1 IP the full slice is returned regardless of
// health (there is no alternative to try). If all IPs are unhealthy
// the full slice is also returned as a fallback.
func (hc *HealthChecker) FilterHealthy(ips []string) []string {
	if len(ips) <= 1 {
		return ips
	}

	hc.mu.RLock()
	defer hc.mu.RUnlock()

	healthy := make([]string, 0, len(ips))
	for _, ip := range ips {
		if h, ok := hc.status[ip]; !ok || h {
			healthy = append(healthy, ip)
		}
	}
	if len(healthy) == 0 {
		return ips
	}
	return healthy
}

// ReportDialFailure immediately marks an IP as unhealthy. The
// background prober will re-check it on the next cycle and restore it
// if the node has recovered.
func (hc *HealthChecker) ReportDialFailure(ip string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if _, exists := hc.status[ip]; exists {
		if hc.status[ip] {
			logging.Infolog.Printf("health check: marking %s unhealthy (dial failure)", ip)
		}
		hc.status[ip] = false
	}
}
