//go:build integration || integration_v0 || integration_core
// +build integration integration_v0 integration_core

package fireboltgosdk

import (
	"database/sql"
	"errors"
	"testing"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
)

func runTestDriverExecStatement(t *testing.T, dsn string) {
	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		t.Errorf("failed unexpectedly")
	}

	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Errorf("connection is not established correctly")
	}
}

func runTestDriverInvalidAuthError(t *testing.T, dsn string) {
	db, err := sql.Open("firebolt", dsn)
	if err == nil {
		t.Errorf("failed unexpectedly")
	}

	if !errors.Is(err, errorUtils.AuthenticationError) {
		t.Errorf("error should be AuthenticationError")
	}

	if db != nil {
		t.Errorf("connection should not be established")
	}
}
