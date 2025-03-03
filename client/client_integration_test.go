//go:build integration
// +build integration

package client

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/types"

	"github.com/firebolt-db/firebolt-go-sdk/rows"
)

var (
	clientIdMock             string
	clientSecretMock         string
	databaseMock             string
	engineNameMock           string
	engineUrlMock            string
	accountName              string
	serviceAccountNoUserName string
	clientMock               *ClientImpl
	clientMockWithAccount    *ClientImpl
)

// init populates mock variables and client for integration tests
func init() {
	clientIdMock = os.Getenv("CLIENT_ID")
	clientSecretMock = os.Getenv("CLIENT_SECRET")
	databaseMock = os.Getenv("DATABASE_NAME")
	engineNameMock = os.Getenv("ENGINE_NAME")
	accountName = os.Getenv("ACCOUNT_NAME")

	var err error
	client, err := ClientFactory(&types.FireboltSettings{
		ClientID:     clientIdMock,
		ClientSecret: clientSecretMock,
		AccountName:  accountName,
		EngineName:   engineNameMock,
		Database:     databaseMock,
		NewVersion:   true,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Errorf("Error authenticating with client id %s: %v", clientIdMock, err))
	}
	clientMock = client.(*ClientImpl)
	clientWithAccount, err := ClientFactory(&types.FireboltSettings{
		ClientID:     clientIdMock,
		ClientSecret: clientSecretMock,
		AccountName:  accountName,
		Database:     databaseMock,
		NewVersion:   true,
	}, GetHostNameURL())
	if err != nil {
		panic(fmt.Sprintf("Authentication error: %v", err))
	}
	engineUrlMock, _, err = clientMock.GetConnectionParameters(context.TODO(), engineNameMock, databaseMock)
	if err != nil {
		panic(fmt.Errorf("Error getting connection parameters: %v", err))
	}
	clientMockWithAccount = clientWithAccount.(*ClientImpl)
	clientMockWithAccount.ConnectedToSystemEngine = true
	serviceAccountNoUserName = databaseMock + "_sa_no_user"
}

// TestGetEnginePropsByName test getting system engine url, as well as engine url, status and database by name
func TestGetEnginePropsByName(t *testing.T) {
	systemEngineURL, _, err := clientMockWithAccount.getSystemEngineURLAndParameters(context.TODO(), accountName, "")
	if err != nil {
		t.Errorf("Error returned by getSystemEngineURL: %s", err)
	}
	if len(systemEngineURL) == 0 {
		t.Errorf("Empty system engine url returned by getSystemEngineURL for account: %s", accountName)
	}
}

// TestQuery tests simple query
func TestQuery(t *testing.T) {
	queryResponse, err := clientMock.Query(context.TODO(), engineUrlMock, "SELECT 1", map[string]string{"database": databaseMock}, ConnectionControl{})
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
		map[string]string{"timezone": "America/New_York", "database": databaseMock},
		ConnectionControl{},
	)
	if err != nil {
		t.Errorf("Query returned an error: %v", err)
	}

	date, err := rows.ParseTimestampTz(resp.Data[0][0].(string))
	if err != nil {
		t.Errorf("Error parsing date: %v", err)
	}
	if date.(time.Time).UTC().Hour() != 5 {
		t.Errorf("Invalid date returned: %s", date)
	}
}
