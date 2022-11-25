//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"
)

var (
	dsnMock                    string
	dsnEngineUrlMock           string
	dsnDefaultEngineMock       string
	dsnDefaultAccountMock      string
	dsnSystemEngineMock        string
	usernameMock               string
	passwordMock               string
	databaseMock               string
	engineUrlMock              string
	engineNameMock             string
	accountNameMock            string
	serviceAccountClientId     string
	serviceAccountClientSecret string
	clientMock                 *Client
)

// init populates mock variables and client for integration tests
func init() {
	usernameMock = os.Getenv("USER_NAME")
	passwordMock = os.Getenv("PASSWORD")
	databaseMock = os.Getenv("DATABASE_NAME")
	engineNameMock = os.Getenv("ENGINE_NAME")
	engineUrlMock = os.Getenv("ENGINE_URL")
	accountNameMock = os.Getenv("ACCOUNT_NAME")

	dsnMock = fmt.Sprintf("firebolt://%s:%s@%s/%s?account_name=%s", usernameMock, passwordMock, databaseMock, engineNameMock, accountNameMock)
	dsnEngineUrlMock = fmt.Sprintf("firebolt://%s:%s@%s/%s?account_name=%s", usernameMock, passwordMock, databaseMock, engineUrlMock, accountNameMock)
	dsnDefaultEngineMock = fmt.Sprintf("firebolt://%s:%s@%s?account_name=%s", usernameMock, passwordMock, databaseMock, accountNameMock)
	dsnDefaultAccountMock = fmt.Sprintf("firebolt://%s:%s@%s", usernameMock, passwordMock, databaseMock)
	dsnSystemEngineMock = fmt.Sprintf("firebolt://%s:%s@%s/%s", usernameMock, passwordMock, databaseMock, "system")
	clientMock, _ = Authenticate(usernameMock, passwordMock, GetHostNameURL())
}

// TestDriverQueryResult tests query happy path, as user would do it
func TestDriverQueryResult(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")

	db, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	rows, err := db.Query(
		"SELECT CAST('2020-01-03 19:08:45' AS DATETIME) as dt, CAST('2020-01-03' AS DATE) as d, CAST(1 AS INT) as i " +
			"UNION " +
			"SELECT CAST('2021-01-03 19:38:34' AS DATETIME) as dt, CAST('2000-12-03' AS DATE) as d, CAST(2 AS INT) as i ORDER BY i")
	if err != nil {
		t.Errorf("db.Query returned an error: %v", err)
	}
	var dt, d time.Time
	var i int

	expectedColumns := []string{"dt", "d", "i"}
	if columns, err := rows.Columns(); reflect.DeepEqual(expectedColumns, columns) && err != nil {
		t.Errorf("columns are not equal (%v != %v) and error is %v", expectedColumns, columns, err)
	}

	assert(rows.Next(), t, "Next returned end of output")
	assert(rows.Scan(&dt, &d, &i) == nil, t, "Scan returned an error")
	assert(dt == time.Date(2020, 01, 03, 19, 8, 45, 0, loc), t, "results not equal for datetime")
	assert(d == time.Date(2020, 01, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(i == 1, t, "results not equal for int")

	assert(rows.Next(), t, "Next returned end of output")
	assert(rows.Scan(&dt, &d, &i) == nil, t, "Scan returned an error")
	assert(dt == time.Date(2021, 01, 03, 19, 38, 34, 0, loc), t, "results not equal for datetime")
	assert(d == time.Date(2000, 12, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(i == 2, t, "results not equal for int")

	assert(!rows.Next(), t, "Next didn't returned false, although no data is expected")

}

// TestDriverOpenConnection checks making a connection on opened driver
func TestDriverOpenConnection(t *testing.T) {
	db, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("failed unexpectedly")
	}

	ctx := context.TODO()
	if _, err = db.Conn(ctx); err != nil {
		t.Errorf("connection is not established correctly: %v", err)
	}
}

func runTestDriverExecStatement(t *testing.T, dsn string) {
	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		t.Errorf("failed unexpectedly")
	}

	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Errorf("connection is not established correctly")
	}
}

// TestDriverOpenEngineUrl checks opening driver with a default engine
func TestDriverOpenEngineUrl(t *testing.T) {
	runTestDriverExecStatement(t, dsnEngineUrlMock)
}

// TestDriverOpenDefaultEngine checks opening driver with a default engine
func TestDriverOpenDefaultEngine(t *testing.T) {
	runTestDriverExecStatement(t, dsnDefaultEngineMock)
}

// TestDriverExecStatement checks exec with full dsn
func TestDriverExecStatement(t *testing.T) {
	runTestDriverExecStatement(t, dsnMock)
}

// TestDriverSystemEngine checks system engine queries are executed without error
func TestDriverSystemEngine(t *testing.T) {
	databaseName := "go_sdk_system_engine_integration_test"
	engineName := "go_sdk_system_engine_integration_test_engine"
	engineNewName := "go_sdk_system_engine_integration_test_engine_2"

	db, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	_, err = db.Query(fmt.Sprintf("DROP DATABASE IF EXISTS %s", databaseName))
	if err != nil {
		t.Errorf("Could not drop database %s. The query returned an error: %v", databaseName, err)
	}

	queries := []string{fmt.Sprintf("CREATE DATABASE %s", databaseName),
		fmt.Sprintf("CREATE ENGINE %s", engineName),
		fmt.Sprintf("ATTACH ENGINE %s TO %s", engineName, databaseName),
		fmt.Sprintf("ALTER DATABASE %s WITH DESCRIPTION = 'GO SDK Integration test'", databaseName),
		fmt.Sprintf("ALTER ENGINE %s RENAME TO %s", engineName, engineNewName),
		fmt.Sprintf("START ENGINE %s", engineNewName),
		fmt.Sprintf("STOP ENGINE %s", engineNewName),
		fmt.Sprintf("DROP DATABASE %s", databaseName)}

	for _, query := range queries {
		_, err := db.Query(query)
		if err != nil {
			t.Errorf("The query %s returned an error: %v", query, err)
		}
	}
}
