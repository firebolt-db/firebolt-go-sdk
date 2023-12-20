package fireboltgosdk

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func init() {
	originalEndpoint = os.Getenv("FIREBOLT_ENDPOINT")
}

var originalEndpoint string

// TestCacheAccessToken tests that a token is cached during authentication and reused for subsequent requests
func TestCacheAccessToken(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponse(10000))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.accessTokenGetter = client.getAccessToken
	var err error
	for i := 0; i < 3; i++ {
		_, err, _ = client.request(context.TODO(), "GET", server.URL, nil, "")
		if err != nil {
			t.Errorf("Did not expect an error %s", err)
		}
	}

	token, _ := getAccessTokenServiceAccount("client_id", "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

	if getCachedAccessToken("client_id", server.URL) != "aMysteriousToken" {
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
func TestRefreshTokenOn401(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponse(10000))
		} else {
			w.WriteHeader(http.StatusUnauthorized)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.accessTokenGetter = client.getAccessToken
	_, _, _ = client.request(context.TODO(), "GET", server.URL, nil, "")

	if getCachedAccessToken("client_id", server.URL) != "aMysteriousToken" {
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
func TestFetchTokenWhenExpired(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			fetchTokenCount++
			_, _ = w.Write(getAuthResponse(1))
		} else {
			w.WriteHeader(http.StatusOK)
		}
		totalCount++
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL, UserAgent: "userAgent"},
	}
	client.accessTokenGetter = client.getAccessToken
	_, _, _ = client.request(context.TODO(), "GET", server.URL, nil, "")
	// Waiting for the token to get expired
	time.Sleep(2 * time.Millisecond)
	_, _, _ = client.request(context.TODO(), "GET", server.URL, nil, "")

	token, _ := getAccessTokenUsernamePassword("client_id", "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Errorf("Did not fetch missing token")
	}

	if getCachedAccessToken("client_id", server.URL) != "aMysteriousToken" {
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
func TestUserAgent(t *testing.T) {
	var userAgentValue = "userAgent"
	var userAgentHeader = ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgentHeader = r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL, UserAgent: userAgentValue},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{})
	if userAgentHeader != userAgentValue {
		t.Errorf("Did not set User-Agent value correctly on a query request")
	}
}

// TestProtocolVersion tests that protocol version is correctly set on request
func TestProtocolVersion(t *testing.T) {
	var protocolVersionValue = ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocolVersionValue = r.Header.Get(protocolVersionHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{})
	if protocolVersionValue != protocolVersion {
		t.Errorf("Did not set Protocol-Version value correctly on a query request")
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

func setupTestServerAndClient(t *testing.T, testAccountName string) (*httptest.Server, *ClientImpl) {
	// Create a mock server that returns a 404 status code
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == fmt.Sprintf(EngineUrlByAccountName, testAccountName) || req.URL.Path == fmt.Sprintf(AccountIdByAccountName, testAccountName) {
			rw.WriteHeader(http.StatusNotFound)
		} else {
			_, _ = rw.Write(getAuthResponse(10000))
		}
	}))

	prepareEnvVariablesForTest(t, server)
	client := &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams

	return server, client
}

func TestGetSystemEngineURLReturnsErrorOn404(t *testing.T) {
	testAccountName := "testAccount"
	server, client := setupTestServerAndClient(t, testAccountName)
	defer server.Close()

	// Call the getSystemEngineURL method and check if it returns an error
	_, err := client.getSystemEngineURL(context.Background(), testAccountName)
	if err == nil {
		t.Errorf("Expected an error, got nil")
	}
	if !strings.HasPrefix(err.Error(), fmt.Sprintf("account '%s' does not exist", testAccountName)) {
		t.Errorf("Expected error to start with \"account '%s' does not exist\", got \"%s\"", testAccountName, err.Error())
	}
}

func TestGetAccountIdReturnsErrorOn404(t *testing.T) {
	testAccountName := "testAccount"
	server, client := setupTestServerAndClient(t, testAccountName)
	defer server.Close()

	// Call the getAccountID method and check if it returns an error
	_, err := client.getAccountID(context.Background(), testAccountName)
	if err == nil {
		t.Errorf("Expected an error, got nil")
	}
	if !strings.HasPrefix(err.Error(), fmt.Sprintf("account '%s' does not exist", testAccountName)) {
		t.Errorf("Expected error to start with \"account '%s' does not exist\", got \"%s\"", testAccountName, err.Error())
	}
}
