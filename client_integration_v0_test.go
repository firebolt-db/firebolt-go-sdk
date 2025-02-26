//go:build integration_v0
// +build integration_v0

package fireboltgosdk

import (
	"context"
	"testing"
	"time"
)

// TestGetAccountId test getting account ID with existing and not existing accounts
func TestGetAccountId(t *testing.T) {
	accountId, err := clientMock.getAccountIDByName(context.TODO(), accountNameMock)
	if err != nil {
		t.Errorf("getAccountID failed with: %s", err)
	}
	if len(accountId) == 0 {
		t.Errorf("returned empty accountId")
	}

	_, err = clientMock.getAccountIDByName(context.TODO(), "firebolt_not_existing_account")
	if err == nil {
		t.Errorf("getAccountID didn't failed with not-existing account")
	}
}

// TestGetEnginePropsByName test getting engine url by name step by step
func TestGetEnginePropsByName(t *testing.T) {
	accountId, err := clientMock.getAccountIDByName(context.TODO(), "firebolt")
	if err != nil {
		t.Errorf("getAccountID failed with: %s", err)
	}

	engineId, err := clientMock.getEngineIdByName(context.TODO(), engineNameMock, accountId)
	if err != nil {
		t.Errorf("getEngineIdByName failed with: %s", err)
	}
	if len(engineId) == 0 {
		t.Errorf("getEngineIdByName succeed but returned a zero length account id")
	}

	engineUrl, err := clientMock.getEngineUrlById(context.TODO(), engineId, accountId)
	if err != nil {
		t.Errorf("getEngineUrlById failed with: %s", err)
	}
	if len(engineUrl) == 0 {
		t.Errorf("getEngineUrlById succeed but returned a zero length account id")
	}
}

// TestGetEngineUrlByName test getEngineUrlByName function and its failure scenarios
func TestGetEngineUrlByName(t *testing.T) {
	accountId, _ := clientMock.getDefaultAccountID(context.TODO())
	engineUrl, err := clientMock.getEngineUrlByName(context.TODO(), engineNameMock, accountId)
	if err != nil {
		t.Errorf("getEngineUrlByName returned an error: %v", err)
	}
	if makeCanonicalUrl(engineUrl) != makeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}
	if res, err := clientMock.getEngineUrlByName(context.TODO(), "not_existing_engine", accountNameMock); err == nil || res != "" {
		t.Errorf("getEngineUrlByName didn't return an error with not existing engine")
	}
	if res, err := clientMock.getEngineUrlByName(context.TODO(), engineNameMock, "not_existing_account"); err == nil || res != "" {
		t.Errorf("getEngineUrlByName didn't return an error with not existing account")
	}
}

// TestGetEngineUrlByDatabase checks, that the url of the default engine returns properly
func TestGetEngineUrlByDatabase(t *testing.T) {
	accountId, _ := clientMock.getAccountIDByName(context.TODO(), accountNameMock)
	engineUrl, err := clientMock.getEngineUrlByDatabase(context.TODO(), databaseMock, accountId)
	if err != nil {
		t.Errorf("getEngineUrlByDatabase failed with: %v, %s", err, accountNameMock)
	}
	if makeCanonicalUrl(engineUrl) != makeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}

	if _, err = clientMock.getEngineUrlByDatabase(context.TODO(), "not_existing_database", accountNameMock); err == nil {
		t.Errorf("getEngineUrlByDatabase didn't return an error with not existing database")
	}
	if _, err = clientMock.getEngineUrlByDatabase(context.TODO(), databaseMock, "not_existing_account"); err == nil {
		t.Errorf("getEngineUrlByDatabase didn't return an error with not existing account")
	}
}

// TestQuery tests simple query
func TestQuery(t *testing.T) {
	queryResponse, err := clientMock.Query(context.TODO(), engineUrlMock, "SELECT 1", map[string]string{"database": databaseMock}, connectionControl{})
	if err != nil {
		t.Errorf("Query returned an error: %v", err)
	}
	if queryResponse.Rows != 1 {
		t.Errorf("Query response has an invalid number of rows %d != %d", queryResponse.Rows, 1)
	}

	if queryResponse.Data[0][0].(float64) != 1 {
		t.Errorf("queryResponse data is not correct")
	}
}

// TestQuery with set statements
func TestQuerySetStatements(t *testing.T) {
	query := "SELECT '2024-01-01 00:00:00'::timestamptz"
	resp, err := clientMock.Query(
		context.TODO(),
		engineUrlMock,
		query,
		map[string]string{"time_zone": "America/New_York", "database": databaseMock},
		connectionControl{},
	)
	if err != nil {
		t.Errorf("Query returned an error: %v", err)
	}

	date, err := rows.parseValue("timestamptz", resp.Data[0][0])
	if err != nil {
		t.Errorf("Error parsing date: %v", err)
	}
	if date.(time.Time).UTC().Hour() != 5 {
		t.Errorf("Invalid date returned: %s", date)
	}
}

func TestGetDefaultAccountID(t *testing.T) {
	var defaultAccountId, accountIdFromName string
	var err error

	if defaultAccountId, err = clientMock.getDefaultAccountID(context.TODO()); err != nil {
		t.Errorf("getting default id returned an error: %v", err)
	}

	if accountIdFromName, err = clientMock.getAccountIDByName(context.TODO(), accountNameMock); err != nil {
		t.Errorf("getting account id by name resulted into an error: %v", err)
	}

	if defaultAccountId != accountIdFromName {
		t.Errorf("default account id is not equal to account id returned by name: '%s' != '%s'",
			defaultAccountId, accountIdFromName)
	}
}
