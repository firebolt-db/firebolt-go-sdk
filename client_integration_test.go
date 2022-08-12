//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"testing"
)

// TestGetAccountId test getting account ID with existing and not existing accounts
func TestGetAccountId(t *testing.T) {
	accountId, err := clientMock.GetAccountIdByName(context.TODO(), accountNameMock)
	if err != nil {
		t.Errorf("GetAccountIdByName failed with: %s", err)
	}
	if len(accountId) == 0 {
		t.Errorf("returned empty accountId")
	}

	_, err = clientMock.GetAccountIdByName(context.TODO(), "firebolt_not_existing_account")
	if err == nil {
		t.Errorf("GetAccountIdByName didn't failed with not-existing account")
	}
}

// TestGetEnginePropsByName test getting engine url by name step by step
func TestGetEnginePropsByName(t *testing.T) {
	accountId, err := clientMock.GetAccountIdByName(context.TODO(), "firebolt")
	if err != nil {
		t.Errorf("GetAccountIdByName failed with: %s", err)
	}

	engineId, err := clientMock.GetEngineIdByName(context.TODO(), engineNameMock, accountId)
	if err != nil {
		t.Errorf("GetEngineIdByName failed with: %s", err)
	}
	if len(engineId) == 0 {
		t.Errorf("GetEngineIdByName succeed but returned a zero length account id")
	}

	engineUrl, err := clientMock.GetEngineUrlById(context.TODO(), engineId, accountId)
	if err != nil {
		t.Errorf("GetEngineUrlById failed with: %s", err)
	}
	if len(engineUrl) == 0 {
		t.Errorf("GetEngineUrlById succeed but returned a zero length account id")
	}
}

// TestGetEngineUrlByName test GetEngineUrlByName function and its failure scenarios
func TestGetEngineUrlByName(t *testing.T) {
	accountId, _ := clientMock.GetDefaultAccountId(context.TODO())
	engineUrl, err := clientMock.GetEngineUrlByName(context.TODO(), engineNameMock, accountId)
	if err != nil {
		t.Errorf("GetEngineUrlByName returned an error: %v", err)
	}
	if makeCanonicalUrl(engineUrl) != makeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}
	if res, err := clientMock.GetEngineUrlByName(context.TODO(), "not_existing_engine", accountNameMock); err == nil || res != "" {
		t.Errorf("GetEngineUrlByName didn't return an error with not existing engine")
	}
	if res, err := clientMock.GetEngineUrlByName(context.TODO(), engineNameMock, "not_existing_account"); err == nil || res != "" {
		t.Errorf("GetEngineUrlByName didn't return an error with not existing account")
	}
}

// TestGetEngineUrlByDatabase checks, that the url of the default engine returns properly
func TestGetEngineUrlByDatabase(t *testing.T) {
	accountId, _ := clientMock.GetAccountIdByName(context.TODO(), accountNameMock)
	engineUrl, err := clientMock.GetEngineUrlByDatabase(context.TODO(), databaseMock, accountId)
	if err != nil {
		t.Errorf("GetEngineUrlByDatabase failed with: %v, %s", err, accountNameMock)
	}
	if makeCanonicalUrl(engineUrl) != makeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}

	if _, err = clientMock.GetEngineUrlByDatabase(context.TODO(), "not_existing_database", accountNameMock); err == nil {
		t.Errorf("GetEngineUrlByDatabase didn't return an error with not existing database")
	}
	if _, err = clientMock.GetEngineUrlByDatabase(context.TODO(), databaseMock, "not_existing_account"); err == nil {
		t.Errorf("GetEngineUrlByDatabase didn't return an error with not existing account")
	}
}

// TestQuery tests simple query
func TestQuery(t *testing.T) {
	queryResponse, err := clientMock.Query(context.TODO(), engineUrlMock, databaseMock, "SELECT 1", nil)
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
	query := "SELECT * FROM information_schema.tables"
	if _, err := clientMock.Query(context.TODO(), engineUrlMock, databaseMock, query, &map[string]string{"use_standard_sql": "1"}); err != nil {
		t.Errorf("Query returned an error: %v", err)
	}
	if _, err := clientMock.Query(context.TODO(), engineUrlMock, databaseMock, query, &map[string]string{"use_standard_sql": "0"}); err == nil {
		t.Errorf("Query didn't return an error, but should")
	}
}

func TestGetDefaultAccountId(t *testing.T) {
	var defaultAccountId, accountIdFromName string
	var err error

	if defaultAccountId, err = clientMock.GetDefaultAccountId(context.TODO()); err != nil {
		t.Errorf("getting default id returned an error: %v", err)
	}

	if accountIdFromName, err = clientMock.GetAccountIdByName(context.TODO(), accountNameMock); err != nil {
		t.Errorf("getting account id by name resulted into an error: %v", err)
	}

	if defaultAccountId != accountIdFromName {
		t.Errorf("default account id is not equal to account id returned by name: '%s' != '%s'",
			defaultAccountId, accountIdFromName)
	}
}
