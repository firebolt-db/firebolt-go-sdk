//go:build integration
// +build integration

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
	accountName                     string
	serviceAccountNoUserName        string
	clientMock                      *ClientImpl
	clientMockWithAccount           *ClientImpl
)

const v0Testing = false

// init populates mock variables and client for integration tests
func init() {
	clientIdMock = os.Getenv("CLIENT_ID")
	clientSecretMock = os.Getenv("CLIENT_SECRET")
	databaseMock = os.Getenv("DATABASE_NAME")
	engineNameMock = os.Getenv("ENGINE_NAME")
	accountName = os.Getenv("ACCOUNT_NAME")

	dsnMock = fmt.Sprintf("firebolt:///%s?account_name=%s&engine=%s&client_id=%s&client_secret=%s", databaseMock, accountName, engineNameMock, clientIdMock, clientSecretMock)
	dsnNoDatabaseMock = fmt.Sprintf("firebolt://?account_name=%s&engine=%s&client_id=%s&client_secret=%s", accountName, engineNameMock, clientIdMock, clientSecretMock)
	dsnSystemEngineWithDatabaseMock = fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s", databaseMock, accountName, clientIdMock, clientSecretMock)

	dsnSystemEngineMock = fmt.Sprintf("firebolt://?account_name=%s&client_id=%s&client_secret=%s", accountName, clientIdMock, clientSecretMock)

	var err error
	client, err := Authenticate(&fireboltSettings{
		clientID:     clientIdMock,
		clientSecret: clientSecretMock,
		accountName:  accountName,
		engineName:   engineNameMock,
		database:     databaseMock,
		newVersion:   true,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Errorf("Error authenticating with client id %s: %v", clientIdMock, err))
	}
	clientMock = client.(*ClientImpl)
	clientWithAccount, err := Authenticate(&fireboltSettings{
		clientID:     clientIdMock,
		clientSecret: clientSecretMock,
		accountName:  accountName,
		database:     databaseMock,
		newVersion:   true,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Sprintf("Authentication error: %v", err))
	}
	engineUrlMock, _, err = clientMock.GetConnectionParameters(context.TODO(), engineNameMock, databaseMock)
	if err != nil {
		panic(fmt.Errorf("Error getting connection parameters: %v", err))
	}
	clientMockWithAccount = clientWithAccount.(*ClientImpl)
	clientMockWithAccount.ConnectedToSystemEngine = true
	serviceAccountNoUserName = databaseMock + "_sa_no_user"
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
	assert(rows.Scan(&f, &f2, &f3, &f4), nil, t, "Scan returned an error")
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
		t.Errorf("failed unexpectedly: %s", err)
	}

	if _, err = db.Exec("SELECT 1"); err != nil {
		t.Errorf("connection is not established correctly: %s", err)
	}
}

// TestDriverOpenEngineUrl checks opening connector with a default engine
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

// function that creates a service account and returns its id and secret
func createServiceAccountNoUser(t *testing.T, serviceAccountName string) (string, string) {
	serviceAccountDescription := "test_service_account_description"

	db, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	// create service account
	createServiceAccountQuery := fmt.Sprintf("CREATE SERVICE ACCOUNT \"%s\" WITH DESCRIPTION = '%s'", serviceAccountName, serviceAccountDescription)
	_, err = db.Query(createServiceAccountQuery)
	if err != nil {
		t.Errorf("The query %s returned an error: %v", createServiceAccountQuery, err)
	}
	// generate credentials for service account
	generateServiceAccountKeyQuery := fmt.Sprintf("CALL fb_GENERATESERVICEACCOUNTKEY('%s')", serviceAccountName)
	// get service account id and secret from the result
	rows, err := db.Query(generateServiceAccountKeyQuery)
	var serviceAccountNameReturned, serviceAccountID, serviceAccountSecret string
	for rows.Next() {
		if err := rows.Scan(&serviceAccountNameReturned, &serviceAccountID, &serviceAccountSecret); err != nil {
			t.Errorf("Failed to retrieve service account id and secret: %v", err)
		}
	}
	// Currently this is bugged so retrieve id via a query if not returned otherwise. FIR-28719
	if serviceAccountID == "" {
		getServiceAccountIDQuery := fmt.Sprintf("SELECT service_account_id FROM information_schema.service_accounts WHERE service_account_name = '%s'", serviceAccountName)
		rows, err := db.Query(getServiceAccountIDQuery)
		if err != nil {
			t.Errorf("Failed to retrieve service account id: %v", err)
		}
		for rows.Next() {
			if err := rows.Scan(&serviceAccountID); err != nil {
				t.Errorf("Failed to retrieve service account id: %v", err)
			}
		}
	}
	return serviceAccountID, serviceAccountSecret
}

func deleteServiceAccount(t *testing.T, serviceAccountName string) {
	db, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	// delete service account
	deleteServiceAccountQuery := fmt.Sprintf("DROP SERVICE ACCOUNT \"%s\"", serviceAccountName)
	_, err = db.Query(deleteServiceAccountQuery)
	if err != nil {
		t.Errorf("The query %s returned an error: %v", deleteServiceAccountQuery, err)
	}
}

// test authentication with service account without a user fails
func TestServiceAccountAuthentication(t *testing.T) {
	serviceAccountID, serviceAccountSecret := createServiceAccountNoUser(t, serviceAccountNoUserName)
	defer deleteServiceAccount(t, serviceAccountNoUserName) // Delete service account after the test

	dsnNoUser := fmt.Sprintf(
		"firebolt:///%s?account_name=%s&engine=%s&client_id=%s&client_secret=%s",
		databaseMock, accountName, engineNameMock, serviceAccountID, serviceAccountSecret)

	_, err := sql.Open("firebolt", dsnNoUser)
	if err == nil {
		t.Errorf("Authentication didn't return an error, although it should")
		t.FailNow()
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("Database '%s' does not exist or not authorized", databaseMock)) {
		t.Errorf("Authentication didn't return an error with correct message, got: %s", err.Error())
	}
}

func TestIncorrectQueryThrowingStructuredError(t *testing.T) {
	db, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	_, err = db.Query("SELECT 'blue'::int")
	if err == nil {
		t.Errorf("Query didn't return an error, although it should")
	}

	if !strings.HasPrefix(err.Error(), "error during query execution: error during query request:") || !strings.Contains(err.Error(), "Unable to cast text 'blue' to integer") {
		t.Errorf("Query didn't return an error with correct message, got: %s", err.Error())
	}
}

func TestParametrisedQuery(t *testing.T) {
	ctx := context.TODO()
	db, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("failed unexpectedly with %v", err)
	}
	query := "SELECT engine_name, status from information_schema.engines WHERE engine_name = ? AND status = ?"
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		t.Errorf("The query %s returned an error: %v", query, err)
	}
	rows, err := stmt.QueryContext(ctx, engineNameMock, "RUNNING")
	if err != nil {
		t.Errorf("The query %s returned an error: %v", query, err)
	}
	if !rows.Next() {
		t.Errorf("Next returned end of output")
	}
	var engineName, status string
	if err := rows.Scan(&engineName, &status); err != nil {
		t.Errorf("Scan returned an error: %v", err)
	}
	if engineName != engineNameMock || status != "RUNNING" {
		t.Errorf("Results not equal: %s %s", engineName, status)
	}
}
