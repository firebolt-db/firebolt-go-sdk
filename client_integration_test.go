//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"testing"
	"time"
)

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
	queryResponse, err := clientMock.Query(context.TODO(), engineUrlMock, "SELECT 1", map[string]string{"database": databaseMock}, connectionControl{})
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
		map[string]string{"time_zone": "America/New_York", "database": databaseMock},
		connectionControl{},
	)
	if err != nil {
		t.Errorf("Query returned an error: %v", err)
	}

	date, err := parseValue("timestamptz", resp.Data[0][0])
	if err != nil {
		t.Errorf("Error parsing date: %v", err)
	}
	if date.(time.Time).UTC().Hour() != 5 {
		t.Errorf("Invalid date returned: %s", date)
	}
}
