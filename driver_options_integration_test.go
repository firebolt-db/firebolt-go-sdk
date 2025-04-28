//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"testing"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"

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
	if rows.Next(values) != io.EOF {
		t.Errorf("expected no more rows, but got additional data")
		t.FailNow()
	}
}

func TestFireboltConnectorWithOptionsInvalidToken(t *testing.T) {
	token := "invalid token"
	userAgent := "test user agent"

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

	engineUrl, engineParameters, err := cl.GetConnectionParameters(context.TODO(), engineNameMock, databaseMock)
	if err != nil {
		t.Errorf("failed to get system engine url: %v", err)
		t.FailNow()
	}

	accountID, _ := engineParameters["account_id"]

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseMock),
		WithAccountID(accountID),
		WithToken(token),
		WithUserAgent(userAgent),
	)

	_, err = conn.client.Query(context.Background(), conn.engineUrl, "SELECT 1", nil, client.ConnectionControl{})
	if err == nil {
		t.Errorf("expected to fail with invalid token")
		t.FailNow()
	}
	if !errors.Is(err, errorUtils.AuthorizationError) {
		t.Errorf("expected to fail with unauthorized error, got: %v", err)
	}
}

func TestFireboltConnectorWithOptionsWithErrors(t *testing.T) {
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

	conn, err := FireboltConnectorWithOptionsWithErrors(
		WithAccountName(accountName),
		NoError(WithUserAgent(userAgent)),
		NoError(WithToken(token)),
		WithDatabaseAndEngineName(databaseMock, engineNameMock),
	)
	if err != nil {
		t.Errorf("failed to create connector with options: %v", err)
		t.FailNow()
	}

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

func TestFireboltConnectorWithOptionsWithErrorsHandleErrors(t *testing.T) {
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

	_, err = FireboltConnectorWithOptionsWithErrors(
		WithDatabaseAndEngineName(databaseMock, engineNameMock),
		WithAccountName(accountName),
		NoError(WithUserAgent(userAgent)),
		NoError(WithToken(token)),
	)
	if err == nil {
		t.Errorf("expected to fail with uninitialized client")
		t.FailNow()
	}
	if err.Error() != "client must be initialized before setting database and engine name" {
		t.Errorf("expected to fail with uninitialized client, got: %v", err)
		t.FailNow()
	}

	_, err = FireboltConnectorWithOptionsWithErrors(
		WithAccountName(accountName+"invalid"),
		NoError(WithUserAgent(userAgent)),
		NoError(WithToken(token)),
		WithDatabaseAndEngineName(databaseMock, engineNameMock),
	)
	if !errors.Is(err, errorUtils.InvalidAccountError) {
		t.Errorf("expected to fail with invalid account name, got: %v", err)
		t.FailNow()
	}

}
