package fireboltgosdk

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestDriverOptions(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"
	accountID := "account-id"
	token := "1234567890"
	userAgent := "UA Test"

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithClientParams(accountID, token, userAgent),
	)

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	cl, ok := conn.client.(*client.ClientImpl)
	utils.AssertEqual(ok, true, t, "client is not *ClientImpl")

	connectionAccountID := conn.cachedParameters["account_id"]
	utils.AssertEqual(connectionAccountID, accountID, t, "accountID is invalid")
	utils.AssertEqual(cl.UserAgent, userAgent, t, "userAgent is invalid")

	tok, err := cl.AccessTokenGetter()
	if err != nil {
		t.Errorf("token getter returned an error: %v", err)
	}

	utils.AssertEqual(tok, token, t, "token getter returned wrong token")
}

func TestDriverOptionsSeparateClientParams(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"
	accountID := "account-id"
	token := "1234567890"
	userAgent := "UA Test"

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithAccountID(accountID),
		WithToken(token),
		WithUserAgent(userAgent),
	)

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	cl, ok := conn.client.(*client.ClientImpl)
	utils.AssertEqual(ok, true, t, "client is not *ClientImpl")

	connectionAccountID := conn.cachedParameters["account_id"]
	utils.AssertEqual(connectionAccountID, accountID, t, "accountID is invalid")
	utils.AssertEqual(cl.UserAgent, userAgent, t, "userAgent is invalid")

	tok, err := cl.AccessTokenGetter()
	if err != nil {
		t.Errorf("token getter returned an error: %v", err)
	}

	utils.AssertEqual(tok, token, t, "token getter returned wrong token")
}

func TestWithDefaultQueryParams(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"

	defaultParams := map[string]string{
		"pgfire_dbname": "account@db@engine",
		"advanced_mode": "true",
	}

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithDefaultQueryParams(defaultParams),
	)

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	// Check that default params are in cachedParameters
	if conn.cachedParameters["pgfire_dbname"] != "account@db@engine" {
		t.Errorf("default param pgfire_dbname not set correctly, got %s want account@db@engine", conn.cachedParameters["pgfire_dbname"])
	}
	if conn.cachedParameters["advanced_mode"] != "true" {
		t.Errorf("default param advanced_mode not set correctly, got %s want true", conn.cachedParameters["advanced_mode"])
	}
}

func TestWithDefaultQueryParamsDoesNotOverrideExisting(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"

	// First set database via WithDatabaseName, then try to override with default params
	defaultParams := map[string]string{
		"database":      "should_not_override",
		"pgfire_dbname": "account@db@engine",
	}

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithDefaultQueryParams(defaultParams),
	)

	// Database should not be overridden
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "database should not be overridden by default params")

	// But pgfire_dbname should be set
	if conn.cachedParameters["pgfire_dbname"] != "account@db@engine" {
		t.Errorf("default param pgfire_dbname not set correctly")
	}
}
