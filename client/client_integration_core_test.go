//go:build integration_core
// +build integration_core

package client

import (
	"context"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

var (
	engineUrlMock string
	databaseMock  string
	clientMock    *ClientImplCore
)

// init populates mock variables and client for integration tests
func init() {
	engineUrlMock = "http://localhost:3473"
	databaseMock = "integration_test_db"

	client, _ := ClientFactory(&types.FireboltSettings{
		Database:   databaseMock,
		Url:        engineUrlMock,
		NewVersion: true,
	}, GetHostNameURL())
	clientMock = client.(*ClientImplCore)
}

// TestQuery with set statements
func TestQuerySetStatements(t *testing.T) {
	query := "SELECT '2024-01-01 00:00:00'::timestamptz"
	response, err := clientMock.Query(
		context.TODO(),
		engineUrlMock,
		query,
		// We use timezone parameter, in comparison to v0 time_zone
		map[string]string{"timezone": "America/New_York", "database": databaseMock},
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
