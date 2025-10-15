package fireboltgosdk

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

// TestConnectionPrepareStatement, tests that prepare statement doesn't result into an error
func TestConnectionPrepareStatement(t *testing.T) {
	emptyClient := client.ClientImplV0{}
	fireboltConnection := fireboltConnection{&emptyClient, "engine_url", map[string]string{}, nil}

	queryMock := "SELECT 1"
	_, err := fireboltConnection.Prepare(queryMock)
	if err != nil {
		t.Errorf("Prepare failed, but it shouldn't: %v", err)
	}
}

// TestConnectionClose, tests that connection close doesn't result an error
// and prepare statement on closed connection is not possible
func TestConnectionClose(t *testing.T) {
	emptyClient := client.ClientImplV0{}
	fireboltConnection := fireboltConnection{&emptyClient, "engine_url", map[string]string{}, nil}
	if err := fireboltConnection.Close(); err != nil {
		t.Errorf("Close failed with an err: %v", err)
	}

	_, err := fireboltConnection.Prepare("SELECT 1")
	if err == nil {
		t.Errorf("Prepare on closed connection didn't fail, but it should")
	}
}

func TestSetParameter(t *testing.T) {

	connector := FireboltConnector{}
	emptyClient := client.ClientImpl{} // Client version is irrelevant for this test
	fireboltConnection := fireboltConnection{&emptyClient, "engine_url", map[string]string{}, &connector}

	fireboltConnection.setParameter("key", "value")
	if fireboltConnection.parameters["key"] != "value" {
		t.Errorf("setParameter didn't set parameter correctly")
	}
	if connector.cachedParameters["key"] != "value" {
		t.Errorf("setParameter didn't set parameter in connector correctly")
	}
}

func TestResetParameters(t *testing.T) {
	connector := FireboltConnector{}
	connector.cachedParameters = map[string]string{
		"database":      "db",
		"engine":        "engine",
		"output_format": "output_format",
		"key":           "value",
	}
	emptyClient := client.ClientImpl{} // Client version is irrelevant for this test
	fireboltConnection := fireboltConnection{&emptyClient, "engine_url", map[string]string{}, &connector}

	fireboltConnection.parameters = map[string]string{
		"database":      "db",
		"engine":        "engine",
		"output_format": "output_format",
		"key":           "value",
	}

	fireboltConnection.resetParameters(nil)
	if len(fireboltConnection.parameters) != 3 {
		t.Errorf("resetParameters didn't remove parameters correctly")
	}
	if len(connector.cachedParameters) != 3 {
		t.Errorf("resetParameters didn't remove parameters in connector correctly")
	}
}

func TestResetParametersList(t *testing.T) {
	connector := FireboltConnector{}
	connector.cachedParameters = map[string]string{
		"database":      "db",
		"engine":        "engine",
		"output_format": "output_format",
		"key":           "value",
		"another_key":   "another_value",
	}
	emptyClient := client.ClientImpl{} // Client version is irrelevant for this test
	fireboltConnection := fireboltConnection{&emptyClient, "engine_url", map[string]string{}, &connector}

	fireboltConnection.parameters = map[string]string{
		"database":      "db",
		"engine":        "engine",
		"output_format": "output_format",
		"key":           "value",
		"different_key": "different_value",
	}

	fireboltConnection.resetParameters(&[]string{"key"})
	if len(fireboltConnection.parameters) != 4 {
		t.Errorf("resetParameters didn't remove specified parameter correctly")
	}
	if _, exists := connector.cachedParameters["key"]; exists {
		t.Errorf("resetParameters didn't remove specified parameter in connector correctly")
	}

	if _, exists := fireboltConnection.parameters["different_key"]; !exists {
		t.Errorf("resetParameters removed parameter that shouldn't have been removed")
	}
	if _, exists := connector.cachedParameters["another_key"]; !exists {
		t.Errorf("resetParameters removed parameter that shouldn't have been removed in connector")
	}
}

// TestDescribeResultStructure tests that the DescribeResult struct has the expected fields
func TestDescribeResultStructure(t *testing.T) {
	// Test that we can create and use a DescribeResult
	result := types.DescribeResult{
		ParameterTypes: map[string]string{"$1": "TEXT"},
		ResultColumns: []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}{
			{Name: "col1", Type: "INTEGER"},
		},
	}

	// Test that we can access the fields
	if len(result.ResultColumns) != 1 {
		t.Errorf("Expected 1 result column, got %d", len(result.ResultColumns))
	}

	if result.ResultColumns[0].Name != "col1" {
		t.Errorf("Expected column name 'col1', got '%s'", result.ResultColumns[0].Name)
	}

	if result.ResultColumns[0].Type != "INTEGER" {
		t.Errorf("Expected column type 'INTEGER', got '%s'", result.ResultColumns[0].Type)
	}

	if len(result.ParameterTypes) != 1 {
		t.Errorf("Expected 1 parameter type, got %d", len(result.ParameterTypes))
	}

	if result.ParameterTypes["$1"] != "TEXT" {
		t.Errorf("Expected parameter type 'TEXT', got '%s'", result.ParameterTypes["$1"])
	}
}
