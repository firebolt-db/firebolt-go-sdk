package fireboltgosdk

import (
	"context"
	"fmt"
	"log"
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

func raiseIfError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Encountered error %s", err)
	}
}

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
	for i := 0; i < 3; i++ {
		resp := client.request(context.TODO(), "GET", server.URL, nil, "")
		raiseIfError(t, resp.err)
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
	_ = client.request(context.TODO(), "GET", server.URL, nil, "")

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
	_ = client.request(context.TODO(), "GET", server.URL, nil, "")
	// Waiting for the token to get expired
	time.Sleep(2 * time.Millisecond)
	_ = client.request(context.TODO(), "GET", server.URL, nil, "")

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

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{}, connectionControl{})
	if userAgentHeader != userAgentValue {
		t.Errorf("Did not set User-Agent value correctly on a query request")
	}
}

func clientFactory(apiEndpoint string) Client {
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: apiEndpoint},
	}
	client.accessTokenGetter = client.getAccessToken
	client.parameterGetter = client.getQueryParams
	err := initialiseCaches()
	if err != nil {
		log.Printf("Error while initializing caches: %s", err)
	}
	return client
}

// TestProtocolVersion tests that protocol version is correctly set on request
func TestProtocolVersion(t *testing.T) {
	testProtocolVersion(t, clientFactory)
}

// TestUpdateParameters tests that update parameters are correctly set on request
func TestUpdateParameters(t *testing.T) {
	testUpdateParameters(t, clientFactory)
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
		if req.URL.Path == fmt.Sprintf(EngineUrlByAccountName, testAccountName) || req.URL.Path == fmt.Sprintf(AccountInfoByAccountName, testAccountName) {
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
	_, _, err := client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	if err == nil {
		t.Errorf("Expected an error, got nil")
	}
	if !strings.HasPrefix(err.Error(), fmt.Sprintf("account '%s' does not exist", testAccountName)) {
		t.Errorf("Expected error to start with \"account '%s' does not exist\", got \"%s\"", testAccountName, err.Error())
	}
}

func TestGetSystemEngineURLCaching(t *testing.T) {
	testAccountName := "testAccount"

	urlCalled := 0

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == fmt.Sprintf(EngineUrlByAccountName, testAccountName) {
			_, _ = rw.Write([]byte(`{"engineUrl": "https://my_url.com"}`))
			urlCalled++
		} else {
			_, _ = rw.Write(getAuthResponse(10000))
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)

	var client = clientFactory(server.URL).(*ClientImpl)

	var err error
	_, _, err = client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	raiseIfError(t, err)
	_, _, err = client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	raiseIfError(t, err)
	if urlCalled != 1 {
		t.Errorf("Expected to call the server only once, got %d", urlCalled)
	}
	// Create a new client

	client = clientFactory(server.URL).(*ClientImpl)
	_, _, err = client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	raiseIfError(t, err)
	// Still only one call, as the cache is shared between clients
	if urlCalled != 1 {
		t.Errorf("Expected to call the server only once, got %d", urlCalled)
	}
}

func TestGetAccountInfoReturnsErrorOn404(t *testing.T) {
	testAccountName := "testAccount"
	server, client := setupTestServerAndClient(t, testAccountName)
	defer server.Close()

	// Call the getAccountID method and check if it returns an error
	_, _, err := client.getAccountInfo(context.Background(), testAccountName)
	if err == nil {
		t.Errorf("Expected an error, got nil")
	}
	if !strings.HasPrefix(err.Error(), fmt.Sprintf("account '%s' does not exist", testAccountName)) {
		t.Errorf("Expected error to start with \"account '%s' does not exist\", got \"%s\"", testAccountName, err.Error())
	}
}

func TestGetAccountInfo(t *testing.T) {
	testAccountName := "testAccount"

	// Create a mock server that returns a 200 status code
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == fmt.Sprintf(AccountInfoByAccountName, testAccountName) {
			_, _ = rw.Write([]byte(`{"id": "account_id", "infraVersion": 2}`))
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

	// Call the getAccountID method and check if it returns the correct account ID and version
	accountID, accountVersion, err := client.getAccountInfo(context.Background(), testAccountName)
	raiseIfError(t, err)
	if accountID != "account_id" {
		t.Errorf("Expected account ID to be 'account_id', got %s", accountID)
	}
	if accountVersion != 2 {
		t.Errorf("Expected account version to be 2, got %d", accountVersion)
	}
}

func TestGetAccountInfoCached(t *testing.T) {
	testAccountName := "testAccount"

	urlCalled := 0

	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == fmt.Sprintf(AccountInfoByAccountName, testAccountName) {
			_, _ = rw.Write([]byte(`{"id": "account_id", "infraVersion": 2}`))
			urlCalled++
		} else {
			_, _ = rw.Write(getAuthResponse(10000))
		}
	}))

	prepareEnvVariablesForTest(t, server)

	var client = clientFactory(server.URL).(*ClientImpl)

	// Account info should be fetched from the cache so the server should not be called
	accountID, accountVersion, err := client.getAccountInfo(context.Background(), testAccountName)
	raiseIfError(t, err)
	if accountID != "account_id" {
		t.Errorf("Expected account ID to be 'account_id', got %s", accountID)
	}
	if accountVersion != 2 {
		t.Errorf("Expected account version to be 2, got %d", accountVersion)
	}
	url := fmt.Sprintf(server.URL+AccountInfoByAccountName, testAccountName)
	if AccountCache.Get(url) == nil {
		t.Errorf("Expected account info to be cached")
	}
	_, _, err = client.getAccountInfo(context.Background(), testAccountName)
	raiseIfError(t, err)
	if urlCalled != 1 {
		t.Errorf("Expected to call the server only once, got %d", urlCalled)
	}
	client = clientFactory(server.URL).(*ClientImpl)
	_, _, err = client.getAccountInfo(context.Background(), testAccountName)
	raiseIfError(t, err)
	// Still only one call, as the cache is shared between clients
	if urlCalled != 1 {
		t.Errorf("Expected to call the server only once, got %d", urlCalled)
	}
}

func TestGetAccountInfoDefaultVersion(t *testing.T) {
	testAccountName := "testAccount"

	// Create a mock server that returns a 200 status code
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == fmt.Sprintf(AccountInfoByAccountName, testAccountName) {
			_, _ = rw.Write([]byte(`{"id": "account_id"}`))
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

	// Call the getAccountID method and check if it returns the correct account ID and version
	accountID, accountVersion, err := client.getAccountInfo(context.Background(), testAccountName)
	raiseIfError(t, err)
	if accountID != "account_id" {
		t.Errorf("Expected account ID to be 'account_id', got %s", accountID)
	}
	if accountVersion != 1 {
		t.Errorf("Expected account version to be 1, got %d", accountVersion)
	}
}

func TestUpdateEndpoint(t *testing.T) {
	var newEndpoint = "new-endpoint/path?query=param"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			_, _ = w.Write(getAuthResponse(10000))
		} else if r.URL.Path == UsernamePasswordURLSuffix {
			_, _ = w.Write(getAuthResponseV0(10000))
		} else {
			w.Header().Set(updateEndpointHeader, newEndpoint)
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	params := map[string]string{
		"database": "db",
	}

	engineEndpoint := "old-endpoint"

	_, err := client.Query(context.TODO(), server.URL, "SELECT 1", params, connectionControl{
		updateParameters: func(key, value string) {
			params[key] = value
		},
		setEngineURL: func(value string) {
			engineEndpoint = value
		},
	})
	raiseIfError(t, err)
	if params["query"] != "param" {
		t.Errorf("Query parameter was not set correctly. Expected 'param' but was %s", params["query"])
	}
	expectedEndpoint := "new-endpoint/path"
	if engineEndpoint != expectedEndpoint {
		t.Errorf("Engine endpoint was not set correctly. Expected %s but was %s", expectedEndpoint, engineEndpoint)
	}
}

func TestResetSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			_, _ = w.Write(getAuthResponse(10000))
		} else {
			w.Header().Set(resetSessionHeader, "true")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	resetCalled := false
	params := map[string]string{
		"database": "db",
	}

	_, err := client.Query(context.TODO(), server.URL, "SELECT 1", params, connectionControl{
		resetParameters: func() {
			resetCalled = true
		},
	})
	raiseIfError(t, err)
	if !resetCalled {
		t.Errorf("Reset session was not called")
	}
}
