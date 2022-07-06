package fireboltgosdk

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
)

var (
	dsn               string
	dsnDefaultEngine  string
	dsnDefaultAccount string
	username          string
	password          string
	database          string
	engineUrl         string
	engineName        string
	accountName       string
)

func init() {
	username = os.Getenv("USER_NAME")
	password = os.Getenv("PASSWORD")
	database = os.Getenv("DATABASE_NAME")
	engineName = os.Getenv("ENGINE_NAME")
	engineUrl = os.Getenv("ENGINE_URL")
	accountName = os.Getenv("ACCOUNT_NAME")

	dsn = fmt.Sprintf("firebolt://%s:%s@%s/%s?account_name=%s", username, password, database, engineName, accountName)
	dsnDefaultEngine = fmt.Sprintf("firebolt://%s:%s@%s?account_name=%s", username, password, database, accountName)
	dsnDefaultAccount = fmt.Sprintf("firebolt://%s:%s@%s", username, password, database)
}

func TestDriverOpen(t *testing.T) {
	db, err := sql.Open("firebolt", "firebolt://user:pass@db_name")
	if err != nil {
		t.Errorf("failed unexpectedly")
	}
	if _, ok := db.Driver().(*FireboltDriver); !ok {
		t.Errorf("returned driver is not a firebolt driver")
	}
}

func TestDriverOpenFail(t *testing.T) {
	db, _ := sql.Open("firebolt", "firebolt://pass@db_name")
	ctx := context.TODO()

	if _, err := db.Conn(ctx); err == nil {
		t.Errorf("missing username in dsn should result into sql.Open error")
	}
}

func TestDriverOpenConnection(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		t.Errorf("failed unexpectedly")
	}

	ctx := context.TODO()
	if _, err = db.Conn(ctx); err != nil {
		t.Errorf("connection is not established correctly")
	}
}

func runTestDriverExecStatement(t *testing.T, dsn string) {
	if testing.Short() {
		t.Skip()
	}

	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		t.Errorf("failed unexpectedly")
	}

	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Errorf("connection is not established correctly")
	}
}

func TestDriverOpenDefaultEngine(t *testing.T) {
	runTestDriverExecStatement(t, dsnDefaultEngine)
}

func TestDriverExecStatement(t *testing.T) {
	runTestDriverExecStatement(t, dsn)
}
