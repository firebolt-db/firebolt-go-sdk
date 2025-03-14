//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"testing"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"

	"github.com/firebolt-db/firebolt-go-sdk/rows"

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

	rows := rows.InMemoryRows{}
	if err := rows.ProcessAndAppendResponse(resp); err != nil {
		t.Errorf("failed to process response: %v", err)
		t.FailNow()
	}

	var values []driver.Value = make([]driver.Value, len(rows.Columns()))

	if err := rows.Next(values); err != nil {
		t.Errorf("failed to get result: %v", err)
		t.FailNow()
	}

	utils.AssertEqual(len(values), 1, t, "returned more that one value")
	utils.AssertEqual(values[0], int32(1), t, "result is not 1")
}

func makeConnectionWithSeparatedOptions(t *testing.T) *FireboltConnector {
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

	return FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseMock),
		WithAccountID(accountID),
		WithToken(token),
		WithUserAgent(userAgent),
	)
}

func TestFireboltConnectorWithASeparatedOptions(t *testing.T) {
	conn := makeConnectionWithSeparatedOptions(t)

	resp, err := conn.client.Query(context.Background(), conn.engineUrl, "SELECT 1", nil, client.ConnectionControl{})
	if err != nil {
		t.Errorf("failed unexpectedly with: %v", err)
		t.FailNow()
	}

	rows := rows.InMemoryRows{}
	if err := rows.ProcessAndAppendResponse(resp); err != nil {
		t.Errorf("failed to process response: %v", err)
		t.FailNow()
	}

	var values []driver.Value = make([]driver.Value, len(rows.Columns()))

	if err := rows.Next(values); err != nil {
		t.Errorf("failed to get result: %v", err)
		t.FailNow()
	}

	utils.AssertEqual(len(values), 1, t, "returned more that one value")
	utils.AssertEqual(values[0], int32(1), t, "result is not 1")
}

func TestFireboltConnectorStreamingWithOptions(t *testing.T) {
	db := makeConnectionWithSeparatedOptions(t)

	c, err := db.Connect(context.Background())
	if err != nil {
		t.Errorf("failed to connect: %v", err)
		t.FailNow()
	}

	conn := c.(*fireboltConnection)

	rows, err := conn.QueryContext(contextUtils.WithStreaming(context.Background()), "SELECT 1", nil)
	if err != nil {
		t.Errorf("failed to query: %v", err)
		t.FailNow()
	}

	var values []driver.Value = make([]driver.Value, len(rows.Columns()))

	if err := rows.Next(values); err != nil {
		t.Errorf("failed to get result: %v", err)
		t.FailNow()
	}

	utils.AssertEqual(len(values), 1, t, "returned more that one value")
	utils.AssertEqual(values[0], int32(1), t, "result is not 1")
}
