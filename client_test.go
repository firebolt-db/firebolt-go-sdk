package fireboltgosdk

import (
	"testing"
)

// TestGetAccountId test getting account ID with existing and not existing accounts
func TestGetAccountId(t *testing.T) {
	markIntegrationTest(t)

	accountId, err := clientMock.GetAccountIdByName("firebolt")
	if err != nil {
		t.Errorf("GetAccountIdByName failed with: %s", err)
	}
	if len(accountId) == 0 {
		t.Errorf("returned empty accountId")
	}

	_, err = clientMock.GetAccountIdByName("firebolt_not_existing_account")
	if err == nil {
		t.Errorf("GetAccountIdByName didn't failed with not-existing account")
	}
}

// TestGetEnginePropsByName test getting engine url by name step by step
func TestGetEnginePropsByName(t *testing.T) {
	markIntegrationTest(t)

	accountId, err := clientMock.GetAccountIdByName("firebolt")
	if err != nil {
		t.Errorf("GetAccountIdByName failed with: %s", err)
	}

	engineId, err := clientMock.GetEngineIdByName(engineNameMock, accountId)
	if err != nil {
		t.Errorf("GetEngineIdByName failed with: %s", err)
	}
	if len(engineId) == 0 {
		t.Errorf("GetEngineIdByName succeed but returned a zero length account id")
	}

	engineUrl, err := clientMock.GetEngineUrlById(engineId, accountId)
	if err != nil {
		t.Errorf("GetEngineUrlById failed with: %s", err)
	}
	if len(engineUrl) == 0 {
		t.Errorf("GetEngineUrlById succeed but returned a zero length account id")
	}
}

// TestGetEngineUrlByName test GetEngineUrlByName function and its failure scenarios
func TestGetEngineUrlByName(t *testing.T) {
	markIntegrationTest(t)

	engineUrl, err := clientMock.GetEngineUrlByName(engineNameMock, accountNameMock)
	if err != nil {
		t.Errorf("GetEngineUrlByName returned an error: %v", err)
	}
	if makeCanonicalUrl(engineUrl) != makeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}
	if _, err = clientMock.GetEngineUrlByName("not_existing_engine", accountNameMock); err == nil {
		t.Errorf("GetEngineUrlByName didn't return an error with not existing engine")
	}
	if _, err = clientMock.GetEngineUrlByName(engineNameMock, "not_existing_account"); err == nil {
		t.Errorf("GetEngineUrlByName didn't return an error with not existing account")
	}
}

// TestGetEngineUrlByDatabase checks, that the url of the default engine returns properly
func TestGetEngineUrlByDatabase(t *testing.T) {
	markIntegrationTest(t)

	engineUrl, err := clientMock.GetEngineUrlByDatabase(databaseMock, accountNameMock)
	if err != nil {
		t.Errorf("GetEngineUrlByDatabase failed with: %v, %s", err, accountNameMock)
	}
	if makeCanonicalUrl(engineUrl) != makeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}

	if _, err = clientMock.GetEngineUrlByDatabase("not_existing_database", accountNameMock); err == nil {
		t.Errorf("GetEngineUrlByDatabase didn't return an error with not existing database")
	}
	if _, err = clientMock.GetEngineUrlByDatabase(databaseMock, "not_existing_account"); err == nil {
		t.Errorf("GetEngineUrlByDatabase didn't return an error with not existing account")
	}
}

// TestQuery tests simple query
func TestQuery(t *testing.T) {
	markIntegrationTest(t)

	var queryResponse QueryResponse
	if err := clientMock.Query(engineUrlMock, databaseMock, "SELECT 1", &queryResponse); err != nil {
		t.Errorf("Query returned an error: %v", err)
	}
	if queryResponse.Rows != 1 {
		t.Errorf("Query response has an invalid number of rows %d != %d", queryResponse.Rows, 1)
	}
}
