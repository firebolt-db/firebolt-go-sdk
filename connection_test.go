package fireboltgosdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
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

// TestDescribeFunctionPreparedStatementsStyleValidation tests that the Describe function
// fails when the context doesn't have PreparedStatementsStyleFbNumeric
func TestDescribeFunctionPreparedStatementsStyleValidation(t *testing.T) {
	emptyClient := &client.ClientImpl{}
	fireboltConnection := fireboltConnection{emptyClient, "engine_url", map[string]string{}, nil}

	const testQuery = "SELECT 1 as col1, $1 as col2"

	// Test with default context (PreparedStatementsStyleNative)
	ctx := context.Background()
	_, err := fireboltConnection.Describe(ctx, testQuery, "text")
	if err == nil {
		t.Error("Expected Describe to fail with default context, but it didn't")
	}
	expectedError := "Describe function requires PreparedStatementsStyleFbNumeric context parameter"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	// Test with explicit PreparedStatementsStyleNative
	nativeCtx := contextUtils.WithPreparedStatementsStyle(ctx, contextUtils.PreparedStatementsStyleNative)
	_, err = fireboltConnection.Describe(nativeCtx, testQuery, "text")
	if err == nil {
		t.Error("Expected Describe to fail with PreparedStatementsStyleNative context, but it didn't")
	}
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}

	// Test with PreparedStatementsStyleFbNumeric - this should NOT fail at the validation step
	// (it may fail later for other reasons like missing mock setup, but validation should pass)
	fbNumericCtx := contextUtils.WithPreparedStatementsStyle(ctx, contextUtils.PreparedStatementsStyleFbNumeric)
	_, err = fireboltConnection.Describe(fbNumericCtx, testQuery, "text")
	// This should not fail due to prepared statements style validation
	if err != nil && err.Error() == expectedError {
		t.Error("Describe failed validation with PreparedStatementsStyleFbNumeric context, but it shouldn't")
	}
}

// mockClientForDescribe is a test client that returns a proper describe response
type mockClientForDescribe struct{}

func (m *mockClientForDescribe) Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control client.ConnectionControl) (*client.Response, error) {
	// Create a mock DescribeResult
	describeResult := types.DescribeResult{
		ParameterTypes: map[string]string{
			"$1": "text",
			"$2": "int",
		},
		ResultColumns: []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}{
			{Name: "col1", Type: "int"},
			{Name: "col2", Type: "text"},
		},
	}

	// Marshal the result to JSON
	jsonResult, err := json.Marshal(describeResult)
	if err != nil {
		return nil, err
	}

	// Create a QueryResponse that contains the JSON result
	queryResponse := types.QueryResponse{
		Meta: []types.Column{{Name: "describe_result", Type: "text"}},
		Data: [][]interface{}{{string(jsonResult)}},
		Rows: 1,
	}

	// Marshal the QueryResponse
	responseData, err := json.Marshal(queryResponse)
	if err != nil {
		return nil, err
	}

	// Create response body reader
	reader := io.NopCloser(bytes.NewReader(responseData))

	// Return a successful Response
	return client.MakeResponse(reader, 200, nil, nil), nil
}

func (m *mockClientForDescribe) GetConnectionParameters(ctx context.Context, engineName string, databaseName string) (string, map[string]string, error) {
	return "mock-engine-url", map[string]string{}, nil
}

func (m *mockClientForDescribe) IsNewVersion() bool {
	return true
}

// TestDescribeFunctionCorrectUsage tests the successful execution of the Describe function
// with proper context and mock client that returns a valid describe response
func TestDescribeFunctionCorrectUsage(t *testing.T) {
	mockClient := &mockClientForDescribe{}
	fireboltConnection := fireboltConnection{mockClient, "engine_url", map[string]string{}, nil}

	const testQuery = "SELECT 1 as col1, $1 as col2, $2 as col3"

	// Create context with PreparedStatementsStyleFbNumeric
	ctx := context.Background()
	fbNumericCtx := contextUtils.WithPreparedStatementsStyle(ctx, contextUtils.PreparedStatementsStyleFbNumeric)

	// Call Describe with proper context and parameters
	result, err := fireboltConnection.Describe(fbNumericCtx, testQuery, "text_value", 42)

	if err != nil {
		t.Errorf("Describe function failed unexpectedly: %v", err)
		return
	}

	if result == nil {
		t.Error("Describe function returned nil result")
		return
	}

	// Verify the result structure
	if len(result.ParameterTypes) != 2 {
		t.Errorf("Expected 2 parameter types, got %d", len(result.ParameterTypes))
	}

	if result.ParameterTypes["$1"] != "text" {
		t.Errorf("Expected parameter $1 to be text, got %s", result.ParameterTypes["$1"])
	}

	if result.ParameterTypes["$2"] != "int" {
		t.Errorf("Expected parameter $2 to be int, got %s", result.ParameterTypes["$2"])
	}

	if len(result.ResultColumns) != 2 {
		t.Errorf("Expected 2 result columns, got %d", len(result.ResultColumns))
	}

	if result.ResultColumns[0].Name != "col1" || result.ResultColumns[0].Type != "int" {
		t.Errorf("Expected first column to be (col1, int), got (%s, %s)",
			result.ResultColumns[0].Name, result.ResultColumns[0].Type)
	}

	if result.ResultColumns[1].Name != "col2" || result.ResultColumns[1].Type != "text" {
		t.Errorf("Expected second column to be (col2, text), got (%s, %s)",
			result.ResultColumns[1].Name, result.ResultColumns[1].Type)
	}
}
