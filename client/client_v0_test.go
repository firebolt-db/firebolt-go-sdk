package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

const mockClientId = "client_id@firebolt.io"

// TestCacheAccessToken tests that a token is cached during authentication and reused for subsequent requests
func TestCacheAccessTokenV0(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v1/login" {
			fetchTokenCount++
			_, _ = w.Write(utils.GetAuthResponse(10000))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		"",
		BaseClient{ClientID: mockClientId, ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.AccessTokenGetter = client.getAccessToken
	for i := 0; i < 3; i++ {
		resp := client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")
		if resp.err != nil {
			t.Errorf("Did not expect an error %s", resp.err)
		}
	}

	token, _ := getAccessTokenUsernamePassword(mockClientId, "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if getCachedAccessToken(mockClientId, server.URL) != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if fetchTokenCount != 1 {
		t.Errorf("Did not fetch token only once. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		t.Errorf("Expected to call the server 4 times (1x to fetch token and 3x to send another request). Total: %d", totalCount)
	}
}

// TestRefreshTokenOn401 tests that a token is refreshed when the server returns a 401
func TestRefreshTokenOn401V0(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v1/login" {
			fetchTokenCount++
			_, _ = w.Write(utils.GetAuthResponse(10000))
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		"",
		BaseClient{ClientID: mockClientId, ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.AccessTokenGetter = client.getAccessToken
	_ = client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")

	if getCachedAccessToken(mockClientId, server.URL) != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if fetchTokenCount != 2 {
		// The token should be fetched twice as it is removed from the cache due to the 403 and then fetched again
		t.Errorf("Did not fetch token twice. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		// The token is fetched twice and the request is retried
		t.Errorf("Expected to call the server 4 times (2x to fetch tokens and 2x to send the request that returns a 403). Total: %d", totalCount)
	}

}

// TestFetchTokenWhenExpired tests that a new token is fetched upon expiry
func TestFetchTokenWhenExpiredV0(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == UsernamePasswordURLSuffix {
			fetchTokenCount++
			_, _ = w.Write(utils.GetAuthResponse(1))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		"",
		BaseClient{ClientID: mockClientId, ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.AccessTokenGetter = client.getAccessToken
	_ = client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")
	// Waiting for the token to get expired
	time.Sleep(2 * time.Millisecond)
	_ = client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")

	token, _ := getAccessTokenUsernamePassword(mockClientId, "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if getCachedAccessToken(mockClientId, server.URL) != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if fetchTokenCount != 2 {
		// The token should be fetched twice as it is automatically removed from the cache because it is expired
		t.Errorf("Did not fetch token twice. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		t.Errorf("Expected to call the server 4 times (2x to fetch tokens and 4x to send the request that returns a 403). Total: %d", totalCount)
	}
}

// TestUserAgent tests that UserAgent is correctly set on request
func TestUserAgentV0(t *testing.T) {
	var userAgentValue = "userAgent"
	var userAgentHeader = ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgentHeader = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		"",
		BaseClient{ClientID: mockClientId, ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: userAgentValue},
	}
	client.AccessTokenGetter = client.getAccessToken
	client.ParameterGetter = client.getQueryParams

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{}, ConnectionControl{})
	if userAgentHeader != userAgentValue {
		t.Errorf("Did not set User-Agent value correctly on a query request")
	}
}

func clientFactoryV0(apiEndpoint string) Client {
	var client = &ClientImplV0{
		"",
		BaseClient{ClientID: mockClientId, ClientSecret: "password", ApiEndpoint: apiEndpoint},
	}
	client.AccessTokenGetter = client.getAccessToken
	client.ParameterGetter = client.getQueryParams
	return client
}

// TestProtocolVersion tests that protocol version is correctly set on request
func TestProtocolVersionV0(t *testing.T) {
	testProtocolVersion(t, clientFactoryV0)
}

func TestUpdateParametersV0(t *testing.T) {
	testUpdateParameters(t, clientFactoryV0)
}

func TestAdditionalHeadersV0(t *testing.T) {
	testAdditionalHeaders(t, clientFactoryV0)
}

func prepareEnvVariablesForTest(t *testing.T, server *httptest.Server) {
	os.Setenv("FIREBOLT_ENDPOINT", server.URL)
	t.Cleanup(cleanupEnvVariables)
}

func cleanupEnvVariables() {
	os.Setenv("FIREBOLT_ENDPOINT", originalEndpoint)
}
