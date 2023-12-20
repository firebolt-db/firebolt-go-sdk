//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"testing"
)

// TestGetEnginePropsByName test getting system engine url, as well as engine url, status and database by name
func TestGetEnginePropsByName(t *testing.T) {
	systemEngineURL, err := clientMockWithAccount.getSystemEngineURL(context.TODO(), accountNameMock)
	if err != nil {
		t.Errorf("Error returned by getSystemEngineURL: %s", err)
	}
	if len(systemEngineURL) == 0 {
		t.Errorf("Empty system engine url returned by getSystemEngineURL for account: %s", accountNameMock)
	}

	engineURL, status, dbName, err := clientMockWithAccount.getEngineUrlStatusDBByName(context.TODO(), engineNameMock, systemEngineURL)
	if err != nil {
		t.Errorf("Error returned by getEngineUrlStatusDBByName: %s", err)
	}
	if engineURL == "" {
		t.Errorf("Empty engine url returned by getEngineUrlStatusDBByName")
	}
	if status != "Running" {
		t.Errorf("Invalid status returned by getEngineUrlStatusDBByName. Got: %s, should be Running", status)
	}
	if dbName != databaseMock {
		t.Errorf("Invalid database returned by getEngineUrlStatusDBByName: expected %s, got %s", databaseMock, dbName)
	}

}

// TestQuery tests simple query
func TestQuery(t *testing.T) {
	queryResponse, err := clientMock.Query(context.TODO(), engineUrlMock, "SELECT 1", map[string]string{"database": databaseMock})
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
	query := "SELECT * FROM information_schema.tables"
	if _, err := clientMock.Query(context.TODO(), engineUrlMock, query, map[string]string{"use_standard_sql": "1", "database": databaseMock}); err != nil {
		t.Errorf("Query returned an error: %v", err)
	}
	if _, err := clientMock.Query(context.TODO(), engineUrlMock, query, map[string]string{"use_standard_sql": "0", "database": databaseMock}); err == nil {
		t.Errorf("Query didn't return an error, but should")
	}
}
