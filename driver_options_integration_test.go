//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"testing"
)

func testFireboltConnectorWithOptions(t *testing.T, engineUrl, databaseName, accountID, token, userAgent string) {
	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseMock),
		WithClientParams(accountID, token, userAgent),
	)

	resp, err := conn.client.Query(context.Background(), conn.engineUrl, "SELECT 1", nil, connectionControl{})
	if err != nil {
		t.Errorf("failed unexpectedly with: %v", err)
	}
	assert(len(resp.Data), 1, t, "result data length is not 1")
	assert(len(resp.Data[0]), 1, t, "result value is invalid")
	assert(resp.Data[0][0].(float64), float64(1), t, "result is not 1")
}

func TestFireboltConnectorWithOptionsAccountV1(t *testing.T) {
	accountID := clientMockWithAccount.AccountID
	userAgent := "test user agent"
	token, err := getAccessTokenServiceAccount(clientIdMock, clientSecretMock, GetHostNameURL(), userAgent)
	if err != nil {
		t.Errorf("failed to get access token: %v", err)
	}

	engineUrl, _, err := clientMockWithAccount.getSystemEngineURLAndParameters(context.TODO(), accountNameV1Mock, "")
	if err != nil {
		t.Errorf("failed to get system engine url: %v", err)
	}

	testFireboltConnectorWithOptions(t, engineUrl, databaseMock, accountID, token, userAgent)
}

func TestFireboltConnectorWithOptionsAccountV2(t *testing.T) {
	userAgent := "test user agent"
	token, err := getAccessTokenServiceAccount(clientIdMock, clientSecretMock, GetHostNameURL(), userAgent)
	if err != nil {
		t.Errorf("failed to get access token: %v", err)
	}

	engineUrl, engineParameters, err := clientMockWithAccount.getSystemEngineURLAndParameters(context.TODO(), accountNameV2Mock, "")
	if err != nil {
		t.Errorf("failed to get system engine url: %v", err)
	}

	accountID, _ := engineParameters["account_id"]

	testFireboltConnectorWithOptions(t, engineUrl, databaseMock, accountID, token, userAgent)
}
