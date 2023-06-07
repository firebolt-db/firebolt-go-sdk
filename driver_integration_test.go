//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	dsnMock                         string
	dsnNoDatabaseMock               string
	dsnSystemEngineWithDatabaseMock string
	dsnSystemEngineMock             string
	clientIdMock                    string
	clientSecretMock                string
	databaseMock                    string
	engineNameMock                  string
	engineUrlMock                   string
	accountNameMock                 string
	clientMock                      *Client
	clientMockWithAccount           *Client
)

// init populates mock variables and client for integration tests
func init() {
	clientIdMock = os.Getenv("CLIENT_ID")
	clientSecretMock = os.Getenv("CLIENT_SECRET")
	databaseMock = os.Getenv("DATABASE_NAME")
	engineNameMock = os.Getenv("ENGINE_NAME")
	accountNameMock = os.Getenv("ACCOUNT_NAME")

	dsnMock = fmt.Sprintf("firebolt:///%s?account_name=%s&engine=%s&client_id=%s&client_secret=%s", databaseMock, accountNameMock, engineNameMock, clientIdMock, clientSecretMock)
	dsnSystemEngineMock = fmt.Sprintf("firebolt://?account_name=%s&client_id=%s&client_secret=%s", accountNameMock, clientIdMock, clientSecretMock)
	dsnNoDatabaseMock = fmt.Sprintf("firebolt://?account_name=%s&engine=%s&client_id=%s&client_secret=%s", accountNameMock, engineNameMock, clientIdMock, clientSecretMock)
	dsnSystemEngineWithDatabaseMock = fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s", databaseMock, accountNameMock, clientIdMock, clientSecretMock)
	var err error
	clientMock, err = Authenticate(clientIdMock, clientSecretMock, GetHostNameURL())
	clientMockWithAccount, err = Authenticate(clientIdMock, clientSecretMock, GetHostNameURL())
	clientMockWithAccount.AccountId, err = clientMockWithAccount.GetAccountId(context.TODO(), accountNameMock)
	if err != nil {
		panic(fmt.Errorf("Error resolving account %s to an id: %v", accountNameMock, err))
	}
	if err != nil {
		panic(fmt.Sprintf("Authentication error: %v", err))
	}
	engineUrlMock = getEngineURL()
}

func getEngineURL() string {
	systemEngineURL, err := clientMockWithAccount.GetSystemEngineURL(context.TODO(), accountNameMock)
	if err != nil {
		panic(fmt.Sprintf("Error returned by GetSystemEngineURL: %s", err))
	}
	if len(systemEngineURL) == 0 {
		panic(fmt.Sprintf("Empty system engine url returned by GetSystemEngineURL for account: %s", accountNameMock))
	}

	engineURL, _, _, err := clientMockWithAccount.GetEngineUrlStatusDBByName(context.TODO(), engineNameMock, systemEngineURL)
	if err != nil {
		panic(fmt.Sprintf("Error returned by GetEngineUrlStatusDBByName: %s", err))
	}
	return engineURL
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

	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	assert(rows.Scan(&dt, &d, &i), nil, t, "Scan returned an error")
	assert(dt, time.Date(2020, 01, 03, 19, 8, 45, 0, loc), t, "results not equal for datetime")
	assert(d, time.Date(2020, 01, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(i, 1, t, "results not equal for int")

	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	assert(rows.Scan(&dt, &d, &i), nil, t, "Scan returned an error")
	assert(dt, time.Date(2021, 01, 03, 19, 38, 34, 0, loc), t, "results not equal for datetime")
	assert(d, time.Date(2000, 12, 03, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(i, 2, t, "results not equal for int")

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
func TestDriverOpenNoDatabase(t *testing.T) {
	runTestDriverExecStatement(t, dsnNoDatabaseMock)
}

// TestDriverExecStatement checks exec with full dsn
func TestDriverExecStatement(t *testing.T) {
	runTestDriverExecStatement(t, dsnMock)
}

// TestDriverExecStatement checks exec with full dsn
func TestDriverSystemEngineDbContext(t *testing.T) {
	db, err := sql.Open("firebolt", dsnSystemEngineWithDatabaseMock)
	if err != nil {
		t.Errorf("failed unexpectedly")
	}

	query := "SELECT table_name FROM information_schema.tables WHERE table_type!='VIEW'"

	if _, err = db.Exec(query); err != nil {
		t.Errorf("System engine with DB context not able to list tables")
	}
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
	rows, err := db.Query(fmt.Sprintf("SELECT database_name FROM information_schema.databases WHERE database_name='%s'", databaseName))
	defer rows.Close()
	if err != nil {
		t.Errorf("Failed to query information_schema.databases : %v", err)
	}

	if !rows.Next() {
		t.Errorf("Could not find database with name %s", databaseName)
	}
	rows, err = db.Query(fmt.Sprintf("SELECT engine_name FROM information_schema.engines WHERE engine_name='%s'", engineNewName))
	defer rows.Close()
	if err != nil {
		t.Errorf("Failed to query information_schema.engines : %v", err)
	}
	if !rows.Next() {
		t.Errorf("Could not find engine with name %s", engineNewName)
	}

	dropDbQuery := fmt.Sprintf("DROP DATABASE %s", databaseName)
	_, err = db.Query(dropDbQuery)
	if err != nil {
		t.Errorf("The query %s returned an error: %v", dropDbQuery, err)
	}
}

func containsDatabase(rows *sql.Rows, databaseToFind string) (bool, error) {
	var databaseName, region, attachedEngines, createdOn, createdBy, errors string
	for rows.Next() {
		if err := rows.Scan(&databaseName, &region, &attachedEngines, &createdOn, &createdBy, &errors); err != nil {
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
