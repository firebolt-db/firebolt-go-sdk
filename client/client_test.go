package client

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

const missingTokenError = "Did not fetch missing token"

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
			_, _ = w.Write(utils.GetAuthResponse(10000))
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
	client.AccessTokenGetter = client.getAccessToken
	for i := 0; i < 3; i++ {
		resp := client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")
		utils.RaiseIfError(t, resp.err)
	}

	token, _ := getAccessTokenServiceAccount("client_id", "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if getCachedAccessToken("client_id", server.URL) != "aMysteriousToken" {
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
func TestRefreshTokenOn401(t *testing.T) {
	var fetchTokenCount = 0
	var totalCount = 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			fetchTokenCount++
			_, _ = w.Write(utils.GetAuthResponse(10000))
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
	client.AccessTokenGetter = client.getAccessToken
	_ = client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")

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
			_, _ = w.Write(utils.GetAuthResponse(1))
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
	client.AccessTokenGetter = client.getAccessToken
	_ = client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")
	// Waiting for the token to get expired
	time.Sleep(2 * time.Millisecond)
	_ = client.requestWithAuthRetry(context.TODO(), "GET", server.URL, nil, "")

	token, _ := getAccessTokenUsernamePassword("client_id", "", server.URL, "")

	if token != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if getCachedAccessToken("client_id", server.URL) != "aMysteriousToken" {
		t.Error(missingTokenError)
	}

	if fetchTokenCount != 2 {
		// The token should be fetched twice as it is automatically removed from the cache because it is expired
		t.Errorf("Did not fetch token twice. Total: %d", fetchTokenCount)
	}

	if totalCount != 4 {
		t.Errorf("Expected to call the server 4 times (2x to fetch tokens and 2x to send the request that returns a 403). Total: %d", totalCount)
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
	client.AccessTokenGetter = client.getAccessToken
	client.ParameterGetter = client.GetQueryParams

	_, _ = client.Query(context.TODO(), server.URL, selectOne, map[string]string{}, ConnectionControl{})
	if userAgentHeader != userAgentValue {
		t.Errorf("Did not set User-Agent value correctly on a query request")
	}
}

func clientFactory(apiEndpoint string) Client {
	var client = &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: apiEndpoint},
	}
	client.AccessTokenGetter = client.getAccessToken
	client.ParameterGetter = client.GetQueryParams
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

func setupTestServerAndClient(t *testing.T, testAccountName string) (*httptest.Server, *ClientImpl) {
	// Create a mock server that returns a 404 status code
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if req.URL.Path == fmt.Sprintf(EngineUrlByAccountName, testAccountName) {
			rw.WriteHeader(http.StatusNotFound)
		} else {
			_, _ = rw.Write(utils.GetAuthResponse(10000))
		}
	}))

	prepareEnvVariablesForTest(t, server)
	client := &ClientImpl{
		BaseClient: BaseClient{ClientID: "client_id", ClientSecret: "client_secret", ApiEndpoint: server.URL},
	}
	client.AccessTokenGetter = client.getAccessToken
	client.ParameterGetter = client.GetQueryParams

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
	if !strings.HasPrefix(err.Error(), "provided account name does not exist") {
		t.Errorf("Expected error to start with \"provided account name does not exist\", got \"%s\"", err.Error())
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
			_, _ = rw.Write(utils.GetAuthResponse(10000))
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)

	var client = clientFactory(server.URL).(*ClientImpl)

	var err error
	_, _, err = client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	utils.RaiseIfError(t, err)
	_, _, err = client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	utils.RaiseIfError(t, err)
	if urlCalled != 1 {
		t.Errorf("Expected to call the server only once, got %d", urlCalled)
	}
	// Create a new client

	client = clientFactory(server.URL).(*ClientImpl)
	_, _, err = client.getSystemEngineURLAndParameters(context.Background(), testAccountName, "")
	utils.RaiseIfError(t, err)
	// Still only one call, as the cache is shared between clients
	if urlCalled != 1 {
		t.Errorf("Expected to call the server only once, got %d", urlCalled)
	}
}

func TestUpdateEndpoint(t *testing.T) {
	var newEndpoint = "new-endpoint/path?query=param"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case ServiceAccountLoginURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		case UsernamePasswordURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		default:
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

	_, err := client.Query(context.TODO(), server.URL, selectOne, params, ConnectionControl{
		UpdateParameters: func(key, value string) {
			params[key] = value
		},
		SetEngineURL: func(value string) {
			engineEndpoint = value
		},
	})
	utils.RaiseIfError(t, err)
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
			_, _ = w.Write(utils.GetAuthResponse(10000))
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

	_, err := client.Query(context.TODO(), server.URL, selectOne, params, ConnectionControl{
		ResetParameters: func(r *[]string) {
			if r == nil {
				resetCalled = true
			}
		},
	})
	utils.RaiseIfError(t, err)
	if !resetCalled {
		t.Errorf("Reset session was not called")
	}
}

func TestRemoveParameters(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			_, _ = w.Write(utils.GetAuthResponse(10000))
		} else {
			w.Header().Add(removeParametersHeader, "key1")
			w.Header().Add(removeParametersHeader, "key2")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	params := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}
	var resetCalledWith *[]string

	_, err := client.Query(context.TODO(), server.URL, selectOne, params, ConnectionControl{
		ResetParameters: func(r *[]string) {
			resetCalledWith = r
		},
	})
	utils.RaiseIfError(t, err)
	if resetCalledWith == nil {
		t.Errorf("Reset parameters wasn't called or was called with nil argument")
	}
	if len(*resetCalledWith) != 2 || (*resetCalledWith)[0] != "key1" || (*resetCalledWith)[1] != "key2" {
		t.Errorf("Reset parameters was called with unexpected keys: %v", *resetCalledWith)
	}

}

func TestAdditionalHeaders(t *testing.T) {
	testAdditionalHeaders(t, clientFactory)
}

func TestAsyncQuery(t *testing.T) {
	validatedAsync := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case ServiceAccountLoginURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		default:
			if r.URL.Query().Get("async") != "true" {
				t.Errorf("Did not set async query parameter to true in async context")
			}
			validatedAsync = true
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	ctx := contextUtils.WithAsync(context.Background())

	_, _ = client.Query(ctx, server.URL, selectOne, map[string]string{}, ConnectionControl{})

	if !validatedAsync {
		t.Errorf("Async query was not validated correctly")
	}
}

func TestDescribeQuery(t *testing.T) {
	validatedDescribe := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case ServiceAccountLoginURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		default:
			if r.URL.Query().Get("execution_mode") != "describe_parameters" {
				t.Errorf("Did not set execution_mode query parameter to describe_parameters in describe context")
			}
			validatedDescribe = true
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	ctx := contextUtils.WithDescribe(context.Background())

	_, _ = client.Query(ctx, server.URL, selectOne, map[string]string{}, ConnectionControl{})

	if !validatedDescribe {
		t.Errorf("Describe query was not validated correctly")
	}
}
