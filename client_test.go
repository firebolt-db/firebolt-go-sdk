package fireboltgosdk

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// TestCacheAccessToken tests that a token is cached during authentication and reused for subsequent requests
func TestCacheAccessToken(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v1/login" {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponse(10000))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	t.Setenv("FIREBOLT_ENDPOINT", server.URL)
	var client = &Client{Username: "username", Password: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"}
	var err error
	for i := 0; i < 3; i++ {
		_, err = client.request(context.TODO(), "GET", server.URL, "userAgent", nil, "")
		if err != nil {
			t.Errorf("Did not expect an error %s", err)
		}
	}
	if fetchTokenCount != 1 {
		t.Errorf("Did not fetch token only once. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		t.Errorf("Expected to call the server 4 times (1x to fetch token and 3x to send another request). Total: %d", totalCount)
	}

	if getCachedAccessToken("username", server.URL) != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}
}

// TestRefreshTokenOn401 tests that a token is refreshed when the server returns a 401
func TestRefreshTokenOn401(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v1/login" {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponse(10000))
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		totalCount++
	}))
	defer server.Close()
	t.Setenv("FIREBOLT_ENDPOINT", server.URL)
	var client = &Client{Username: "username", Password: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"}
	_, _ = client.request(context.TODO(), "GET", server.URL, "userAgent", nil, "")
	_, _ = client.request(context.TODO(), "GET", server.URL, "userAgent", nil, "")

	if fetchTokenCount != 2 {
		// The token should be fetched twice as it is removed from the cache during the second client.request() call
		t.Errorf("Did not fetch token twice. Total: %d", fetchTokenCount)
	}

	if totalCount != 5 {
		t.Errorf("Expected to call the server 5 times (2x to fetch tokens and 3x to send the request that returns a 403). Total: %d", totalCount)
	}

	if getCachedAccessToken("username", server.URL) != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

}

// TestFetchTokenWhenExpired tests that a new token is fetched upon expiry
func TestFetchTokenWhenExpired(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v1/login" {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponse(1))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	t.Setenv("FIREBOLT_ENDPOINT", server.URL)
	var client = &Client{Username: "username", Password: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"}
	_, _ = client.request(context.TODO(), "GET", server.URL, "userAgent", nil, "")
	// Waiting for the token to get expired
	time.Sleep(2 * time.Millisecond)
	_, _ = client.request(context.TODO(), "GET", server.URL, "userAgent", nil, "")

	if fetchTokenCount != 2 {
		// The token should be fetched twice as it is automatically removed from the cache because it is expired
		t.Errorf("Did not fetch token twice. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		t.Errorf("Expected to call the server 5 times (2x to fetch tokens and 3x to send the request that returns a 403). Total: %d", totalCount)
	}

	if getCachedAccessToken("username", server.URL) != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

}
func getAuthResponse(expiry int) []byte {
	var response = `{
   "access_token": "aMysteriousToken",
   "refresh_token": "refresh",
   "scope": "offline_access",
   "expires_in": ` + strconv.Itoa(expiry) + `,
   "token_type": "Bearer"
}`
	return []byte(response)
}
