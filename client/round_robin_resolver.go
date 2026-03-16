package client

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

// LookupHostFunc resolves a hostname to a list of IP addresses.
// Its signature matches net.Resolver.LookupHost.
type LookupHostFunc func(ctx context.Context, host string) ([]string, error)

// RoundRobinResolver resolves a service hostname to its underlying IP
// addresses and cycles through them in round-robin order on each call
// to Next. This distributes HTTP requests across all pods behind a
// Kubernetes (headless) service.
//
// DNS results are cached for a configurable TTL and refreshed lazily.
// If a refresh fails, the previously cached addresses are kept.
type RoundRobinResolver struct {
	originalURL  *url.URL
	originalHost string // hostname without port
	port         string

	lookupHost LookupHostFunc
	TTL        time.Duration

	mu           sync.RWMutex
	ips          []string
	lastResolved time.Time

	counter atomic.Uint64
}

const defaultDNSTTL = 30 * time.Second

// NewRoundRobinResolver creates a resolver that will cycle through the
// IPs returned by lookupHost for the given URL. If lookupHost is nil,
// net.DefaultResolver.LookupHost is used.
func NewRoundRobinResolver(rawURL string, lookupHost LookupHostFunc) (*RoundRobinResolver, error) {
	canonical := MakeCanonicalUrl(rawURL)
	parsed, err := url.Parse(canonical)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", rawURL, err)
	}

	host := parsed.Hostname()
	port := parsed.Port()

	if lookupHost == nil {
		lookupHost = net.DefaultResolver.LookupHost
	}

	return &RoundRobinResolver{
		originalURL:  parsed,
		originalHost: host,
		port:         port,
		lookupHost:   lookupHost,
		TTL:          defaultDNSTTL,
	}, nil
}

// resolve refreshes the cached IP list when the TTL has expired.
func (r *RoundRobinResolver) resolve(ctx context.Context) ([]string, error) {
	r.mu.RLock()
	if len(r.ips) > 0 && time.Since(r.lastResolved) < r.TTL {
		ips := r.ips
		r.mu.RUnlock()
		return ips, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock.
	if len(r.ips) > 0 && time.Since(r.lastResolved) < r.TTL {
		return r.ips, nil
	}

	ips, err := r.lookupHost(ctx, r.originalHost)
	if err != nil {
		if len(r.ips) > 0 {
			logging.Infolog.Printf("DNS refresh failed for %s, using cached addresses: %v", r.originalHost, err)
			return r.ips, nil
		}
		return nil, fmt.Errorf("DNS lookup failed for %s: %w", r.originalHost, err)
	}
	if len(ips) == 0 {
		if len(r.ips) > 0 {
			logging.Infolog.Printf("DNS returned no addresses for %s, using cached addresses", r.originalHost)
			return r.ips, nil
		}
		return nil, fmt.Errorf("DNS lookup returned no addresses for %s", r.originalHost)
	}

	r.ips = ips
	r.lastResolved = time.Now()
	return ips, nil
}

// Next returns the URL rewritten with the next IP in round-robin
// rotation, along with the original host (with port) for use as the
// HTTP Host header. Each successive call advances to the next IP.
func (r *RoundRobinResolver) Next(ctx context.Context) (resolvedURL string, originalHostWithPort string, err error) {
	ips, err := r.resolve(ctx)
	if err != nil {
		return "", "", err
	}

	idx := r.counter.Add(1) - 1
	ip := ips[idx%uint64(len(ips))]

	resolved := *r.originalURL
	if r.port != "" {
		resolved.Host = net.JoinHostPort(ip, r.port)
	} else {
		resolved.Host = ip
	}

	return resolved.String(), r.originalURL.Host, nil
}
