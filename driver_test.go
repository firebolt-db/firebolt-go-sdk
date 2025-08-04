package fireboltgosdk

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

// TestDriverOpen tests that the connector is opened (happy path)
func TestDriverOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case client.UsernamePasswordURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		case client.DefaultAccountURL:
			_, _ = w.Write(getDefaultAccountResponse())
		}
	}))
	defer server.Close()

	currentEndpoint := os.Getenv("FIREBOLT_ENDPOINT")
	utils.Must(os.Setenv("FIREBOLT_ENDPOINT", server.URL))
	defer func() { utils.Must(os.Setenv("FIREBOLT_ENDPOINT", currentEndpoint)) }()

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
		switch r.URL.Path {
		case client.UsernamePasswordURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		case client.DefaultAccountURL:
			_, _ = w.Write(getDefaultAccountResponse())
		}
	}))
	defer server.Close()

	currentEndpoint := os.Getenv("FIREBOLT_ENDPOINT")
	utils.Must(os.Setenv("FIREBOLT_ENDPOINT", server.URL))
	defer func() { utils.Must(os.Setenv("FIREBOLT_ENDPOINT", currentEndpoint)) }()

	if _, err := sql.Open("firebolt", "firebolt://pass@db_name"); err == nil {
		t.Errorf("missing username in dsn should result into sql.Open error")
	}
}
