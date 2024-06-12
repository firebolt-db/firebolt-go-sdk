//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"testing"
)

func TestFireboltConnectorWithOptions(t *testing.T) {
	userAgent := "test user agent"
	token, err := getAccessTokenServiceAccount(clientIdMock, clientSecretMock, GetHostNameURL(), userAgent)
	if err != nil {
		t.Errorf("failed to get access token: %v", err)
	}

	engineUrl, engineParameters, err := clientMockWithAccount.getSystemEngineURLAndParameters(context.TODO(), accountName, "")
	if err != nil {
		t.Errorf("failed to get system engine url: %v", err)
	}

	accountID, _ := engineParameters["account_id"]

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
