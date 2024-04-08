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

	"github.com/google/uuid"
)

var (
	dsnMock                         string
	dsnNoDatabaseMock               string
	dsnSystemEngineWithDatabaseMock string
	dsnSystemEngineMock             string
	dsnV2Mock                       string
	dsnSystemEngineV2Mock           string
	clientIdMock                    string
	clientSecretMock                string
	databaseMock                    string
	engineNameMock                  string
	engineUrlMock                   string
	accountNameV1Mock               string
	accountNameV2Mock               string
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
	accountNameV1Mock = os.Getenv("ACCOUNT_NAME_V1")
	accountNameV2Mock = os.Getenv("ACCOUNT_NAME_V2")

	dsnMock = fmt.Sprintf("firebolt:///%s?account_name=%s&engine=%s&client_id=%s&client_secret=%s", databaseMock, accountNameV1Mock, engineNameMock, clientIdMock, clientSecretMock)
	dsnSystemEngineMock = fmt.Sprintf("firebolt://?account_name=%s&client_id=%s&client_secret=%s", accountNameV1Mock, clientIdMock, clientSecretMock)
	dsnNoDatabaseMock = fmt.Sprintf("firebolt://?account_name=%s&engine=%s&client_id=%s&client_secret=%s", accountNameV1Mock, engineNameMock, clientIdMock, clientSecretMock)
	dsnSystemEngineWithDatabaseMock = fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s", databaseMock, accountNameV1Mock, clientIdMock, clientSecretMock)

	dsnV2Mock = fmt.Sprintf("firebolt:///%s?account_name=%s&engine=%s&client_id=%s&client_secret=%s", databaseMock, accountNameV2Mock, engineNameMock, clientIdMock, clientSecretMock)
	dsnSystemEngineV2Mock = fmt.Sprintf("firebolt://?account_name=%s&client_id=%s&client_secret=%s", accountNameV2Mock, clientIdMock, clientSecretMock)

	var err error
	client, err := Authenticate(&fireboltSettings{
		clientID:     clientIdMock,
		clientSecret: clientSecretMock,
		accountName:  accountNameV1Mock,
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
		accountName:  accountNameV1Mock,
		database:     databaseMock,
		newVersion:   true,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Sprintf("Authentication error: %v", err))
	}
	clientMockWithAccount = clientWithAccount.(*ClientImpl)
	clientMockWithAccount.ConnectedToSystemEngine = true
	engineUrlMock = getEngineURL()
	serviceAccountNoUserName = databaseMock + "_sa_no_user"
}

func getEngineURL() string {
	systemEngineURL, _, err := clientMockWithAccount.getSystemEngineURLAndParameters(context.TODO(), accountNameV1Mock, "")
	if err != nil {
		panic(fmt.Sprintf("Error returned by getSystemEngineURL: %s", err))
	}
	if len(systemEngineURL) == 0 {
		panic(fmt.Sprintf("Empty system engine url returned by getSystemEngineURL for account: %s", accountNameV1Mock))
	}

	engineURL, _, _, err := clientMockWithAccount.getEngineUrlStatusDBByName(context.TODO(), engineNameMock, systemEngineURL)
	if err != nil {
		panic(fmt.Sprintf("Error returned by getEngineUrlStatusDBByName: %s", err))
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
		fmt.Sprintf("ALTER DATABASE %s SET DESCRIPTION = 'GO SDK Integration test'", databaseName),
		fmt.Sprintf("ALTER ENGINE %s RENAME TO %s", engineName, engineNewName),
		fmt.Sprintf("START ENGINE %s", engineNewName),
		fmt.Sprintf("STOP ENGINE %s", engineNewName),
	}

	// Cleanup
	defer func() {
		stopEngineQuery := fmt.Sprintf("STOP ENGINE %s", engineName)
		stopNewEngineQuery := fmt.Sprintf("STOP ENGINE %s", engineNewName)
		dropEngineQuery := fmt.Sprintf("DROP ENGINE IF EXISTS %s", engineName)
		dropNewEngineQuery := fmt.Sprintf("DROP ENGINE IF EXISTS %s", engineNewName)
		for _, query := range []string{stopEngineQuery, stopNewEngineQuery, dropEngineQuery, dropNewEngineQuery} {
			db.Query(query)
		}
		dropDbQuery := fmt.Sprintf("DROP DATABASE %s", databaseName)
		_, err = db.Query(dropDbQuery)
		if err != nil {
			t.Errorf("The cleanup query %s returned an error: %v", dropDbQuery, err)
		}
	}()

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

func TestIncorrectAccount(t *testing.T) {
	_, err := Authenticate(&fireboltSettings{
		clientID:     clientIdMock,
		clientSecret: clientSecretMock,
		accountName:  "incorrect_account",
		engineName:   engineNameMock,
		database:     databaseMock,
		newVersion:   true,
	}, GetHostNameURL())
	if err == nil {
		t.Errorf("Authentication didn't return an error, although it should")
	}
	if !strings.HasPrefix(err.Error(), "error during getting account id: account 'incorrect_account' does not exist") {
		t.Errorf("Authentication didn't return an error with correct message, got: %s", err.Error())
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
	createServiceAccountQuery := fmt.Sprintf("CREATE SERVICE ACCOUNT \"%s\" WITH DESCRIPTION = \"%s\"", serviceAccountName, serviceAccountDescription)
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

	_, err := Authenticate(&fireboltSettings{
		clientID:     serviceAccountID,
		clientSecret: serviceAccountSecret,
		accountName:  accountNameV1Mock,
		engineName:   engineNameMock,
		database:     databaseMock,
		newVersion:   true,
	}, GetHostNameURL())
	if err == nil {
		t.Errorf("Authentication didn't return an error, although it should")
	}
	if !strings.HasPrefix(err.Error(), fmt.Sprintf("error during getting account id: account '%s' does not exist", accountNameV1Mock)) {
		t.Errorf("Authentication didn't return an error with correct message, got: %s", err.Error())
	}
}
