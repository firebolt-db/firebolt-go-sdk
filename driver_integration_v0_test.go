//go:build integration_v0
// +build integration_v0

package fireboltgosdk

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

var (
	dsnMock               string
	dsnEngineUrlMock      string
	dsnDefaultEngineMock  string
	dsnDefaultAccountMock string
	dsnSystemEngineMock   string
	usernameMock          string
	passwordMock          string
	databaseMock          string
	engineUrlMock         string
	engineNameMock        string
	accountNameMock       string
)

const scanErrorMessage = "Scan returned an error"

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
}

// TestDriverQueryResult tests query happy path, as user would do it
func TestDriverQueryResult(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")

	db, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	rows, err := db.Query(
		"SELECT CAST('2020-01-03 19:08:45' AS DATETIME) as dt, CAST('2020-01-03' AS DATE) as d, CAST(1 AS INT) as i, '-inf'::float as f " +
			"UNION " +
			"SELECT CAST('2021-01-03 19:38:34' AS DATETIME) as dt, CAST('2000-12-03' AS DATE) as d, CAST(2 AS INT) as i, 'nan'::float as f ORDER BY i")
	if err != nil {
		t.Errorf("db.Query returned an error: %v", err)
	}
	var dt, d time.Time
	var i int
	var f float64

	expectedColumns := []string{"dt", "d", "i", "f"}
	if columns, err := rows.Columns(); reflect.DeepEqual(expectedColumns, columns) && err != nil {
		t.Errorf("columns are not equal (%v != %v) and error is %v", expectedColumns, columns, err)
	}

	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	utils.AssertEqual(rows.Scan(&dt, &d, &i, &f), nil, t, scanErrorMessage)
	utils.AssertEqual(dt, time.Date(2020, 01, 03, 19, 8, 45, 0, loc), t, "results not equal for datetime")
	utils.AssertEqual(d, time.Date(2020, 01, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	utils.AssertEqual(i, 1, t, "results not equal for int")
	utils.AssertEqual(f, math.Inf(-1), t, "results not equal for float")

	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	utils.AssertEqual(rows.Scan(&dt, &d, &i, &f), nil, t, scanErrorMessage)
	utils.AssertEqual(dt, time.Date(2021, 01, 03, 19, 38, 34, 0, loc), t, "results not equal for datetime")
	utils.AssertEqual(d, time.Date(2000, 12, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	utils.AssertEqual(i, 2, t, "results not equal for int")
	if !math.IsNaN(f) {
		t.Log(string(debug.Stack()))
		t.Errorf("results not equal for float Expected: NaN Got: %f", f)
	}

	if rows.Next() {
		t.Errorf("Next didn't returned false, although no data is expected")
	}
}

// TestDriverInfNanValues tests query with inf and nan values
func TestDriverInfNanValues(t *testing.T) {
	db, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	rows, err := db.Query("SELECT '-inf'::double as f, 'inf'::double as f2, 'nan'::double as f3, '-nan'::double as f4")
	if err != nil {
		t.Errorf("db.Query returned an error: %v", err)
	}
	var f, f2, f3, f4 float64

	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	utils.AssertEqual(rows.Scan(&f, &f2, &f3, &f4), nil, t, scanErrorMessage)
	if !math.IsInf(f, -1) {
		t.Errorf("results not equal for float Expected: -Inf Got: %f", f)
	}
	if !math.IsInf(f2, 1) {
		t.Errorf("results not equal for float Expected: Inf Got: %f", f2)
	}
	if !math.IsNaN(f3) {
		t.Errorf("results not equal for float Expected: NaN Got: %f", f3)
	}
	if !math.IsNaN(f4) {
		t.Errorf("results not equal for float Expected: NaN Got: %f", f4)
	}
}

// TestDriverOpenConnection checks making a connection on opened connector
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

// TestDriverOpenEngineUrl checks opening connector with a default engine
func TestDriverOpenEngineUrl(t *testing.T) {
	runTestDriverExecStatement(t, dsnEngineUrlMock)
}

// TestDriverOpenDefaultEngine checks opening connector with a default engine
func TestDriverOpenDefaultEngine(t *testing.T) {
	runTestDriverExecStatement(t, dsnDefaultEngineMock)
}

// TestDriverExecStatement checks exec with full dsn
func TestDriverExecStatement(t *testing.T) {
	runTestDriverExecStatement(t, dsnMock)
}
