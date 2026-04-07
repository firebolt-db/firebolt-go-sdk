package client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

// DebugHC enables verbose real-time logging of the client-side health
// checker to stderr. Set to true before opening a connection to watch
// probe cycles, IP filtering, and state transitions as they happen.
var DebugHC bool

var hcLog = log.New(os.Stderr, "[firebolt-hc] ", log.Ldate|log.Ltime|log.Lmicroseconds)

func hcDebug(format string, args ...interface{}) {
	if DebugHC {
		hcLog.Output(2, fmt.Sprintf(format, args...))
	}
}

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
				DialContext: (&net.Dialer{
					Timeout:   hcProbeTimeout,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConns:        10,
				MaxIdleConnsPerHost: 1,
				IdleConnTimeout:     60 * time.Second,
			},
		},
		interval: interval,
		status:   make(map[string]bool),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	hcDebug("created health checker: url=%s interval=%s probe_timeout=%s", parsed.String(), interval, hcProbeTimeout)
	return hc, nil
}

// Start launches the background probe goroutine.
func (hc *HealthChecker) Start() {
	hcDebug("starting background probe goroutine (interval=%s)", hc.interval)
	go hc.run()
}

// Stop signals the background goroutine to exit and blocks until it
// has finished. Safe to call multiple times.
func (hc *HealthChecker) Stop() {
	hc.stopOnce.Do(func() {
		hcDebug("stopping background probe goroutine")
		close(hc.stopCh)
		<-hc.doneCh
		hc.client.CloseIdleConnections()
		hcDebug("background probe goroutine stopped")
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

	start := time.Now()
	healthyCount := 0
	for _, ip := range ips {
		select {
		case <-hc.stopCh:
			return
		default:
		}
		healthy := hc.probe(ip)
		if healthy {
			healthyCount++
		}
		hc.mu.Lock()
		if prev, exists := hc.status[ip]; exists {
			hc.status[ip] = healthy
			if prev != healthy {
				if healthy {
					hcDebug("state change: %s unhealthy -> healthy", ip)
					logging.Infolog.Printf("[firebolt-hc] %s is now healthy", ip)
				} else {
					hcDebug("state change: %s healthy -> unhealthy", ip)
					logging.Infolog.Printf("[firebolt-hc] %s is now unhealthy", ip)
				}
			}
		}
		hc.mu.Unlock()
	}
	hcDebug("probe cycle: %d/%d healthy (took %s)", healthyCount, len(ips), time.Since(start))
}

func (hc *HealthChecker) probe(ip string) bool {
	if hc.probeFunc != nil {
		result := hc.probeFunc(ip)
		hcDebug("probe %s (test func): healthy=%t", ip, result)
		return result
	}
	u := *hc.hcURL
	port := hc.hcURL.Port()
	if port != "" {
		u.Host = net.JoinHostPort(ip, port)
	} else {
		u.Host = ip
	}

	probeURL := u.String()
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), hcProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", probeURL, nil)
	if err != nil {
		hcDebug("probe %s -> request creation error: %v (took %s)", ip, err, time.Since(start))
		return false
	}
	resp, err := hc.client.Do(req)
	if err != nil {
		hcDebug("probe %s -> error: %v (took %s)", ip, err, time.Since(start))
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	healthy := resp.StatusCode >= 200 && resp.StatusCode < 300
	if !healthy {
		hcDebug("probe %s -> HTTP %d, body=%q (took %s)", ip, resp.StatusCode, string(body), time.Since(start))
	}
	return healthy
}

// UpdateIPs syncs the tracked IP set with the latest DNS results.
// New IPs are optimistically marked healthy; IPs that disappeared from
// DNS are removed from the status map.
func (hc *HealthChecker) UpdateIPs(ips []string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	var added, removed []string

	current := make(map[string]struct{}, len(ips))
	for _, ip := range ips {
		current[ip] = struct{}{}
		if _, exists := hc.status[ip]; !exists {
			hc.status[ip] = true
			added = append(added, ip)
		}
	}
	for ip := range hc.status {
		if _, exists := current[ip]; !exists {
			delete(hc.status, ip)
			removed = append(removed, ip)
		}
	}

	if len(added) > 0 || len(removed) > 0 {
		hcDebug("UpdateIPs: total=%d added=[%s] removed=[%s]", len(ips), strings.Join(added, ", "), strings.Join(removed, ", "))
	} else {
		hcDebug("UpdateIPs: total=%d (no changes)", len(ips))
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
	var unhealthyList []string
	for _, ip := range ips {
		if h, ok := hc.status[ip]; !ok || h {
			healthy = append(healthy, ip)
		} else {
			unhealthyList = append(unhealthyList, ip)
		}
	}
	if len(healthy) == 0 {
		hcDebug("FilterHealthy: all %d IPs unhealthy, returning full list as fallback", len(ips))
		return ips
	}
	if len(unhealthyList) > 0 {
		hcDebug("FilterHealthy: %d/%d healthy, filtered out [%s]",
			len(healthy), len(ips), strings.Join(unhealthyList, ", "))
	}
	return healthy
}

// countHealthy returns the number of healthy IPs without logging.
// Used by HealthyIPCount to avoid duplicate log lines on the hot path.
func (hc *HealthChecker) countHealthy(ips []string) int {
	if len(ips) <= 1 {
		return len(ips)
	}
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	n := 0
	for _, ip := range ips {
		if h, ok := hc.status[ip]; !ok || h {
			n++
		}
	}
	if n == 0 {
		return len(ips)
	}
	return n
}

// ReportDialFailure immediately marks an IP as unhealthy. The
// background prober will re-check it on the next cycle and restore it
// if the node has recovered.
func (hc *HealthChecker) ReportDialFailure(ip string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	if _, exists := hc.status[ip]; exists {
		was := hc.status[ip]
		hc.status[ip] = false
		if was {
			hcDebug("ReportDialFailure: %s healthy -> unhealthy (dial failure)", ip)
			logging.Infolog.Printf("[firebolt-hc] marking %s unhealthy (dial failure)", ip)
		} else {
			hcDebug("ReportDialFailure: %s already unhealthy (dial failure)", ip)
		}
	} else {
		hcDebug("ReportDialFailure: %s not tracked, ignoring", ip)
	}
}
