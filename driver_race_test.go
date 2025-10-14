//go:build race
// +build race

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

func TestDriverOpenRace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case client.UsernamePasswordURLSuffix:
			_, _ = w.Write(utils.GetAuthResponse(10000))
		case client.DefaultAccountURL:
			_, _ = w.Write(getDefaultAccountResponse())
		}
	}))
	currentEndpoint := os.Getenv("FIREBOLT_ENDPOINT")
	utils.Must(os.Setenv("FIREBOLT_ENDPOINT", server.URL))
	defer func() { utils.Must(os.Setenv("FIREBOLT_ENDPOINT", currentEndpoint)) }()
	defer server.Close()
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			db, err := sql.Open("firebolt", "firebolt://user@fb:pass@db_name/eng.firebolt.io")
			if err != nil {
				t.Errorf("connection failed unexpectedly: %v", err)
			}
			if _, ok := db.Driver().(*FireboltDriver); !ok {
				t.Errorf("returned connector is not a firebolt connector")
			}
			done <- true
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
