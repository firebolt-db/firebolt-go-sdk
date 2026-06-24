package client

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

func TestDiscoveryGetConnectionParameters(t *testing.T) {
	var queryHit bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case discoveryPath:
			_, _ = fmt.Fprintf(w, `{
				"engine_url": "%s/query?from_discovery=true",
				"query_parameters": {"timezone": "UTC", "database": "discovered_db"}
			}`, r.Host)
		case "/query":
			queryHit = true
			if r.URL.Query().Get("database") != "client_db" {
				t.Errorf("database query parameter got %q want client_db", r.URL.Query().Get("database"))
			}
			if r.URL.Query().Get("engine") != "client_engine" {
				t.Errorf("engine query parameter got %q want client_engine", r.URL.Query().Get("engine"))
			}
			if r.URL.Query().Get("timezone") != "UTC" {
				t.Errorf("timezone query parameter got %q want UTC", r.URL.Query().Get("timezone"))
			}
			if r.URL.Query().Get("from_discovery") != "true" {
				t.Errorf("from_discovery query parameter got %q want true", r.URL.Query().Get("from_discovery"))
			}
			if r.Header.Get("Authorization") != "" {
				t.Errorf("discovery client should not send Authorization header, got %q", r.Header.Get("Authorization"))
			}
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := MakeClientDiscovery(&types.FireboltSettings{
		DiscoveryEndpoint: server.URL,
		SSLMode:           "none",
		ConnectionParameters: map[string]string{
			"database": "client_db",
			"engine":   "client_engine",
		},
	})
	if err != nil {
		t.Fatalf("MakeClientDiscovery returned an error: %v", err)
	}

	engineURL, params, err := client.GetConnectionParameters(context.Background(), "", "")
	if err != nil {
		t.Fatalf("GetConnectionParameters returned an error: %v", err)
	}
	if engineURL != server.URL+"/query" {
		t.Fatalf("engineURL got %q want %q", engineURL, server.URL+"/query")
	}
	if params["database"] != "client_db" || params["engine"] != "client_engine" || params["timezone"] != "UTC" || params["from_discovery"] != "true" {
		t.Fatalf("unexpected params: %v", params)
	}

	_, err = client.Query(context.Background(), engineURL, "SELECT 1", params, ConnectionControl{})
	if err != nil {
		t.Fatalf("Query returned an error: %v", err)
	}
	if !queryHit {
		t.Fatal("expected query endpoint to be called")
	}
}

func TestDiscoveryEndpointAliases(t *testing.T) {
	tests := []string{
		`{"engineUrl": "%s/query"}`,
		`{"endpoint": "%s/query"}`,
		`{"query_endpoint": "%s/query"}`,
		`{"queryEndpoint": "%s/query"}`,
		`{"sql_url": "%s/query"}`,
		`{"sqlUrl": "%s/query"}`,
		`{"sql_http_url": "%s/query"}`,
	}

	for _, responseTemplate := range tests {
		t.Run(responseTemplate, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != discoveryPath {
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = fmt.Fprintf(w, responseTemplate, r.Host)
			}))
			defer server.Close()

			client, err := MakeClientDiscovery(&types.FireboltSettings{DiscoveryEndpoint: server.URL, SSLMode: "none"})
			if err != nil {
				t.Fatalf("MakeClientDiscovery returned an error: %v", err)
			}
			engineURL, _, err := client.GetConnectionParameters(context.Background(), "", "")
			if err != nil {
				t.Fatalf("GetConnectionParameters returned an error: %v", err)
			}
			if engineURL != server.URL+"/query" {
				t.Fatalf("engineURL got %q want %q", engineURL, server.URL+"/query")
			}
		})
	}
}

func TestDiscoveryRelativeEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == discoveryPath {
			_, _ = w.Write([]byte(`{"endpoint": "/query"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := MakeClientDiscovery(&types.FireboltSettings{DiscoveryEndpoint: server.URL, SSLMode: "none"})
	if err != nil {
		t.Fatalf("MakeClientDiscovery returned an error: %v", err)
	}
	engineURL, _, err := client.GetConnectionParameters(context.Background(), "", "")
	if err != nil {
		t.Fatalf("GetConnectionParameters returned an error: %v", err)
	}
	if engineURL != server.URL+"/query" {
		t.Fatalf("engineURL got %q want %q", engineURL, server.URL+"/query")
	}
}
