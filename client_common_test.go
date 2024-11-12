package fireboltgosdk

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testProtocolVersion(t *testing.T, clientFactory func(string) Client) {
	var protocolVersionValue = ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		protocolVersionValue = r.Header.Get(protocolVersionHeader)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)

	client := clientFactory(server.URL)

	_, _ = client.Query(context.TODO(), server.URL, "SELECT 1", map[string]string{}, connectionControl{})
	if protocolVersionValue != protocolVersion {
		t.Errorf("Did not set Protocol-Version value correctly on a query request")
	}
}

func testUpdateParameters(t *testing.T, clientFactory func(string) Client) {
	var newDatabaseName = "new_database"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			_, _ = w.Write(getAuthResponse(10000))
		} else if r.URL.Path == UsernamePasswordURLSuffix {
			_, _ = w.Write(getAuthResponseV0(10000))
		} else {
			w.Header().Set(updateParametersHeader, fmt.Sprintf("%s=%s", "database", newDatabaseName))
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()
	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	params := map[string]string{
		"database": "db",
	}
	_, err := client.Query(context.TODO(), server.URL, "SELECT 1", params, connectionControl{
		updateParameters: func(key, value string) {
			params[key] = value
		},
	})
	if err != nil {
		t.Errorf("Error during query execution with update parameters header in response %s", err)
	}
	if params["database"] != newDatabaseName {
		t.Errorf("Database is not set correctly. Expected %s but was %s", newDatabaseName, params["database"])
	}
}

func testAdditionalHeaders(t *testing.T, clientFactory func(string) Client) {
	// Test that additional headers, passed in ctx are respected

	var additionalHeaders = map[string]string{
		"Firebolt-Test-Header": "test",
		"Ignored-Header":       "ignored",
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ServiceAccountLoginURLSuffix {
			_, _ = w.Write(getAuthResponse(10000))
		} else if r.URL.Path == UsernamePasswordURLSuffix {
			_, _ = w.Write(getAuthResponseV0(10000))
		} else {
			if r.Header.Get("Firebolt-Test-Header") != "test" {
				t.Errorf("Did not set Firebolt-Test-Header value when passed in ctx")
			}
			if r.Header.Get("Ignored-Header") != "" {
				t.Errorf("Did not ignore Ignored-Header value when passed in ctx")
			}
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	prepareEnvVariablesForTest(t, server)
	client := clientFactory(server.URL)

	ctx := context.WithValue(context.TODO(), "additionalHeaders", additionalHeaders)

	_, _ = client.Query(ctx, server.URL, "SELECT 1", map[string]string{}, connectionControl{})

}
