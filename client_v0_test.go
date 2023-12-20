package fireboltgosdk

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestCacheAccessToken tests that a token is cached during authentication and reused for subsequent requests
func TestCacheAccessTokenV0(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth/v1/login" {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponseV0(10000))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		BaseClient{ClientID: "ClientID@firebolt.io", ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.accessTokenGetter = client.getAccessToken
	var err error
	for i := 0; i < 3; i++ {
		_, _, _, err = client.request(context.TODO(), "GET", server.URL, nil, "")
		if err != nil {
			t.Errorf("Did not expect an error %s", err)
		}
	}

	token, _ := getAccessTokenUsernamePassword("ClientID@firebolt.io", "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

	if getCachedAccessToken("ClientID@firebolt.io", server.URL) != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
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
			_, _ = w.Write(getAuthResponseV0(10000))
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		BaseClient{ClientID: "ClientID@firebolt.io", ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.accessTokenGetter = client.getAccessToken
	_, _, _, _ = client.request(context.TODO(), "GET", server.URL, nil, "")

	if getCachedAccessToken("ClientID@firebolt.io", server.URL) != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
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
			_, _ = w.Write(getAuthResponseV0(1))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		BaseClient{ClientID: "ClientID@firebolt.io", ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.accessTokenGetter = client.getAccessToken
	_, _, _, _ = client.request(context.TODO(), "GET", server.URL, nil, "")
	// Waiting for the token to get expired
	time.Sleep(2 * time.Millisecond)
	_, _, _, _ = client.request(context.TODO(), "GET", server.URL, nil, "")

	token, _ := getAccessTokenUsernamePassword("ClientID@firebolt.io", "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

	if getCachedAccessToken("ClientID@firebolt.io", server.URL) != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

	if fetchTokenCount != 2 {
		// The token should be fetched twice as it is automatically removed from the cache because it is expired
		t.Errorf("Did not fetch token twice. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		t.Errorf("Expected to call the server 5 times (2x to fetch tokens and 3x to send the request that returns a 403). Total: %d", totalCount)
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
		BaseClient{ClientID: "ClientID@firebolt.io", ClientSecret: "password", ApiEndpoint: server.URL, UserAgent: userAgentValue},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{}, func(key, value string) {})
	if userAgentHeader != userAgentValue {
		t.Errorf("Did not set User-Agent value correctly on a query request")
	}
}

// TestProtocolVersion tests that protocol version is correctly set on request
func TestProtocolVersionV0(t *testing.T) {
	var protocolVersionValue = ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocolVersionValue = r.Header.Get(protocolVersionHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		BaseClient{ClientID: "ClientID@firebolt.io", ClientSecret: "password", ApiEndpoint: server.URL},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{}, func(key, value string) {})
	if protocolVersionValue != protocolVersion {
		t.Errorf("Did not set Protocol-Version value correctly on a query request")
	}
}

func TestUpdateParametersV0(t *testing.T) {
	var newDatabaseName = "new_database"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == UsernamePasswordURLSuffix {
			_, _ = w.Write(getAuthResponseV0(10000))
		} else {
			w.Header().Set(updateParametersHeader, fmt.Sprintf("%s=%s", "database", newDatabaseName))
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImplV0{
		BaseClient{ClientID: "ClientID@firebolt.io", ClientSecret: "password", ApiEndpoint: server.URL},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams

	params := map[string]string{
		"database": "db",
	}
	_, err := client.Query(context.TODO(), server.URL, "SELECT 1", params, func(key, value string) {
		params[key] = value
	})
	if err != nil {
		t.Errorf("Error during query execution with update parameters header in response %s", err)
	}
	if params["database"] != newDatabaseName {
		t.Errorf("Did not set Update-Parameters value correctly")
	}
}

func getAuthResponseV0(expiry int) []byte {
	var response = `{
   "access_token": "aMysteriousToken",
   "refresh_token": "refresh",
   "scope": "offline_access",
   "expires_in": ` + strconv.Itoa(expiry) + `,
   "token_type": "Bearer"
}`
	return []byte(response)
}

func prepareEnvVariablesForTest(t *testing.T, server *httptest.Server) {
	os.Setenv("FIREBOLT_ENDPOINT", server.URL)
	t.Cleanup(cleanupEnvVariables)
}

func cleanupEnvVariables() {
	os.Setenv("FIREBOLT_ENDPOINT", originalEndpoint)
}
