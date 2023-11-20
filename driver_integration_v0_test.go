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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
	clientMock                 *ClientImplV0
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
	client, err := Authenticate(&fireboltSettings{
		clientID:     usernameMock,
		clientSecret: passwordMock,
		newVersion:   false,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Errorf("Error authenticating with username password %s: %v", usernameMock, err))
	}
	clientMock = client.(*ClientImplV0)
}

// TestDriverQueryResult tests query happy path, as user would do it
func TestDriverQueryResult(t *testing.T) {
	loc, _ := time.LoadLocation("UTC")

	db, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	rows, err := db.Query(
		"SELECT CAST('2020-01-03 19:08:45' AS DATETIME) as dt, CAST('2020-01-03' AS DATE) as d, CAST(1 AS INT) as i, CAST(-1/0 as FLOAT) as f " +
			"UNION " +
			"SELECT CAST('2021-01-03 19:38:34' AS DATETIME) as dt, CAST('2000-12-03' AS DATE) as d, CAST(2 AS INT) as i, CAST(0/0 as FLOAT) as f ORDER BY i")
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
	assert(rows.Scan(&dt, &d, &i, &f), nil, t, "Scan returned an error")
	assert(dt, time.Date(2020, 01, 03, 19, 8, 45, 0, loc), t, "results not equal for datetime")
	assert(d, time.Date(2020, 01, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(i, 1, t, "results not equal for int")
	assert(f, math.Inf(-1), t, "results not equal for float")

	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	assert(rows.Scan(&dt, &d, &i, &f), nil, t, "Scan returned an error")
	assert(dt, time.Date(2021, 01, 03, 19, 38, 34, 0, loc), t, "results not equal for datetime")
	assert(d, time.Date(2000, 12, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(i, 2, t, "results not equal for int")
	if !math.IsNaN(f) {
		t.Log(string(debug.Stack()))
		t.Errorf("results not equal for float Expected: NaN Got: %f", f)
	}

	if rows.Next() {
		t.Errorf("Next didn't returned false, although no data is expected")
	}
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
	suffix := strings.ReplaceAll(uuid.New().String(), "-", "")
	databaseName := fmt.Sprintf("gosdk_system_engine_test_%s", suffix)
	engineName := fmt.Sprintf("gosdk_system_engine_test_e_%s", suffix)
	engineNewName := fmt.Sprintf("gosdk_system_engine_test_e_2_%s", suffix)

	db, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	ddlStatements := []string{
		fmt.Sprintf("CREATE DATABASE %s", databaseName),
		fmt.Sprintf("CREATE ENGINE %s WITH SPEC = 'C1' SCALE = 1", engineName),
		fmt.Sprintf("ATTACH ENGINE %s TO %s", engineName, databaseName),
		fmt.Sprintf("ALTER DATABASE %s WITH DESCRIPTION = 'GO SDK Integration test'", databaseName),
		fmt.Sprintf("ALTER ENGINE %s RENAME TO %s", engineName, engineNewName),
		fmt.Sprintf("START ENGINE %s", engineNewName),
		fmt.Sprintf("STOP ENGINE %s", engineNewName),
	}

	for _, query := range ddlStatements {
		_, err := db.Query(query)
		if err != nil {
			t.Errorf("The query %s returned an error: %v", query, err)
		}
	}
	rows, err := db.Query("SHOW DATABASES")
	defer rows.Close()
	if err != nil {
		t.Errorf("Failed to execute query 'SHOW DATABASES' : %v", err)
	}
	containsDatabase, err := containsDatabase(rows, databaseName)
	if err != nil {
		t.Errorf("Failed to read response for query 'SHOW DATABASES' : %v", err)
	}

	if !containsDatabase {
		t.Errorf("Could not find database with name %s", databaseName)
	}
	// Uncomment once https://packboard.atlassian.net/browse/FIR-17301 is done
	//rows, err = db.Query("SHOW ENGINES")
	//defer rows.Close()
	//if err != nil {
	//	t.Errorf("Failed to execute query 'SHOW ENGINES' : %v", err)
	//}
	//containsEngine, err := containsEngine(rows, databaseName)
	//if err != nil {
	//	t.Errorf("Failed to read response for query 'SHOW ENGINES' : %v", err)
	//}
	//if !containsEngine {
	//	t.Errorf("Could not find engine with name %s", engineName)
	//}

	dropDbQuery := fmt.Sprintf("DROP DATABASE %s", databaseName)
	_, err = db.Query(dropDbQuery)
	if err != nil {
		t.Errorf("The query %s returned an error: %v", dropDbQuery, err)
	}
}

func containsDatabase(rows *sql.Rows, databaseToFind string) (bool, error) {
	var databaseName, compressed_size, uncompressed_size, description, createdOn, createdBy, region, attachedEngines, errors string
	for rows.Next() {
		if err := rows.Scan(
			&databaseName,
			&compressed_size,
			&uncompressed_size,
			&description,
			&createdOn,
			&createdBy,
			&region,
			&attachedEngines,
			&errors); err != nil {
			return false, err
		}
		if databaseToFind == databaseName {
			return true, nil
		}
	}
	return false, nil
}

func containsEngine(rows *sql.Rows, engineToFind string) (bool, error) {
	var engineName, region, spec, scale, status, attachedTo, version string
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(&engineName, &region, &spec, &scale, &status, &attachedTo, &version); err != nil {
			return false, err
		}
		if engineName == engineToFind {
			return true, nil
		}
	}
	return false, nil
}
