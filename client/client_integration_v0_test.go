//go:build integration_v0
// +build integration_v0

package client

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

var (
	usernameMock    string
	passwordMock    string
	databaseMock    string
	engineUrlMock   string
	engineNameMock  string
	accountNameMock string
	clientMock      *ClientImplV0
)

func init() {
	usernameMock = os.Getenv("USER_NAME")
	passwordMock = os.Getenv("PASSWORD")
	databaseMock = os.Getenv("DATABASE_NAME")
	engineNameMock = os.Getenv("ENGINE_NAME")
	engineUrlMock = os.Getenv("ENGINE_URL")
	accountNameMock = os.Getenv("ACCOUNT_NAME")

	client, err := ClientFactory(&types.FireboltSettings{
		ClientID:     usernameMock,
		ClientSecret: passwordMock,
		NewVersion:   false,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Errorf("Error authenticating with username password %s: %v", usernameMock, err))
	}
	clientMock = client.(*ClientImplV0)
}

// TestGetAccountId test getting account ID with existing and not existing accounts
func TestGetAccountId(t *testing.T) {
	accountId, err := clientMock.getAccountIDByName(context.TODO(), accountNameMock)
	if err != nil {
		t.Errorf("GetAccountID failed with: %s", err)
	}
	if len(accountId) == 0 {
		t.Errorf("returned empty accountId")
	}

	_, err = clientMock.getAccountIDByName(context.TODO(), "firebolt_not_existing_account")
	if err == nil {
		t.Errorf("GetAccountID didn't failed with not-existing account")
	}
}

// TestGetEnginePropsByName test getting engine url by name step by step
func TestGetEnginePropsByName(t *testing.T) {
	accountId, err := clientMock.getAccountIDByName(context.TODO(), "firebolt")
	if err != nil {
		t.Errorf("GetAccountID failed with: %s", err)
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
	if MakeCanonicalUrl(engineUrl) != MakeCanonicalUrl(engineUrlMock) {
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
	if MakeCanonicalUrl(engineUrl) != MakeCanonicalUrl(engineUrlMock) {
		t.Errorf("Returned engine url is not equal to a mocked engine url %s != %s", engineUrl, engineUrlMock)
	}

	if _, err = clientMock.getEngineUrlByDatabase(context.TODO(), "not_existing_database", accountNameMock); err == nil {
		t.Errorf("getEngineUrlByDatabase didn't return an error with not existing database")
	}
	if _, err = clientMock.getEngineUrlByDatabase(context.TODO(), databaseMock, "not_existing_account"); err == nil {
		t.Errorf("getEngineUrlByDatabase didn't return an error with not existing account")
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

// TestQuery with set statements
func TestQuerySetStatements(t *testing.T) {
	query := "SELECT '2024-01-01 00:00:00'::timestamptz"
	response, err := clientMock.Query(
		context.TODO(),
		engineUrlMock,
		query,
		// We use time_zone parameter, in comparison to v1 timezone
		map[string]string{"time_zone": "America/New_York", "database": databaseMock},
		ConnectionControl{},
	)
	if err != nil {
		t.Errorf("Query returned an error: %v", err)
		t.FailNow()
	}

	queryResponse, err := parseResponse(t, response, 1)
	if err != nil {
		t.Errorf("Error parsing response: %v", err)
		t.FailNow()
	}

	date, err := time.Parse("2006-01-02 15:04:05-07", queryResponse.Data[0][0].(string))
	if err != nil {
		t.Errorf("Error parsing date: %v", err)
		t.FailNow()
	}
	if date.UTC().Hour() != 5 {
		t.Errorf("Invalid date returned: %s", date)
	}
}
