package fireboltgosdk

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestDriverOpen tests that the connector is opened (happy path)
func TestDriverOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == UsernamePasswordURLSuffix {
			_, _ = w.Write(getAuthResponse(10000))
		} else if r.URL.Path == DefaultAccountURL {
			_, _ = w.Write(getDefaultAccountResponse())
		}
	}))
	defer server.Close()

	currentEndpoint := os.Getenv("FIREBOLT_ENDPOINT")
	os.Setenv("FIREBOLT_ENDPOINT", server.URL)
	defer os.Setenv("FIREBOLT_ENDPOINT", currentEndpoint)

	db, err := sql.Open("firebolt", "firebolt://user@fb:pass@db_name/eng.firebolt.io")
	if err != nil {
		t.Errorf("connection failed unexpectedly: %v", err)
	}
	if _, ok := db.Driver().(*FireboltDriver); !ok {
		t.Errorf("returned connector is not a firebolt connector")
	}
}

func getDefaultAccountResponse() []byte {
	var response = `{
       "account": {
           "name": "default_account",
           "id": "default_account_id"
       }
    }`
	return []byte(response)
}

// TestDriverOpenFail tests opening a connector with wrong dsn
func TestDriverOpenFail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == UsernamePasswordURLSuffix {
			_, _ = w.Write(getAuthResponse(10000))
		} else if r.URL.Path == DefaultAccountURL {
			_, _ = w.Write(getDefaultAccountResponse())
		}
	}))
	defer server.Close()

	currentEndpoint := os.Getenv("FIREBOLT_ENDPOINT")
	os.Setenv("FIREBOLT_ENDPOINT", server.URL)
	defer os.Setenv("FIREBOLT_ENDPOINT", currentEndpoint)

	if _, err := sql.Open("firebolt", "firebolt://pass@db_name"); err == nil {
		t.Errorf("missing username in dsn should result into sql.Open error")
	}
}
