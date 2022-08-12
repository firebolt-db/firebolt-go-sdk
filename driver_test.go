package fireboltgosdk

import (
	"context"
	"database/sql"
	"testing"
)

// TestDriverOpen tests that the driver is opened (happy path)
func TestDriverOpen(t *testing.T) {
	db, err := sql.Open("firebolt", "firebolt://user:pass@db_name")
	if err != nil {
		t.Errorf("failed unexpectedly")
	}
	if _, ok := db.Driver().(*FireboltDriver); !ok {
		t.Errorf("returned driver is not a firebolt driver")
	}
}

// TestDriverOpenFail tests opening a driver with wrong dsn
func TestDriverOpenFail(t *testing.T) {
	db, _ := sql.Open("firebolt", "firebolt://pass@db_name")
	ctx := context.TODO()

	if _, err := db.Conn(ctx); err == nil {
		t.Errorf("missing username in dsn should result into sql.Open error")
	}
}
