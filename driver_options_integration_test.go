//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/types"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestFireboltConnectorWithOptions(t *testing.T) {
	cl, err := client.ClientFactory(&types.FireboltSettings{
		ClientID:     clientIdMock,
		ClientSecret: clientSecretMock,
		AccountName:  accountName,
		EngineName:   engineNameMock,
		Database:     databaseMock,
		NewVersion:   true,
	}, client.GetHostNameURL())
	if err != nil {
		t.Errorf("failed to authenticate with client id %s: %v", clientIdMock, err)
		t.FailNow()
	}
	token, err := cl.(*client.ClientImpl).AccessTokenGetter()
	if err != nil {
		t.Errorf("failed to get access token: %v", err)
		t.FailNow()
	}

	userAgent := "test user agent"

	engineUrl, engineParameters, err := cl.GetConnectionParameters(context.TODO(), engineNameMock, databaseMock)
	if err != nil {
		t.Errorf("failed to get system engine url: %v", err)
		t.FailNow()
	}

	accountID, _ := engineParameters["account_id"]

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseMock),
		WithClientParams(accountID, token, userAgent),
	)

	resp, err := conn.client.Query(context.Background(), conn.engineUrl, "SELECT 1", nil, client.ConnectionControl{})
	if err != nil {
		t.Errorf("failed unexpectedly with: %v", err)
		t.FailNow()
	}
	utils.AssertEqual(len(resp.Data), 1, t, "result data length is not 1")
	utils.AssertEqual(len(resp.Data[0]), 1, t, "result value is invalid")
	utils.AssertEqual(resp.Data[0][0].(float64), float64(1), t, "result is not 1")
}
