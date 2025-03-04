package fireboltgosdk

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
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

func runProcessSetStatementFail(t *testing.T, value string) {
	emptyClient := client.ClientImpl{} // Client version is irrelevant for this test
	fireboltConnection := fireboltConnection{&emptyClient, "engine_url", map[string]string{}, nil}
	expectedError := "could not set parameter"

	_, err := processSetStatement(context.TODO(), &fireboltConnection, value)
	if err == nil {
		t.Errorf("processSetStatement didn't fail, but it should")
	} else if !strings.Contains(err.Error(), expectedError) {
		t.Errorf("processSetStatement failed with unexpected error, expected error to contain: %v got: %v", expectedError, err)
	}
}

func TestProcessSetStatement(t *testing.T) {
	runProcessSetStatementFail(t, "SET database=my_db")
	runProcessSetStatementFail(t, "SET engine=my_engine")
	runProcessSetStatementFail(t, "SET output_format='json'")
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

func TestMultipleSetParameters(t *testing.T) {
	connector := FireboltConnector{}
	emptyClient := client.MakeMockClient()

	fireboltConnection := fireboltConnection{emptyClient, "engine_url", map[string]string{}, &connector}
	var err error

	_, err = processSetStatement(context.TODO(), &fireboltConnection, "SET key1=value1")
	utils.RaiseIfError(t, err)
	_, err = processSetStatement(context.TODO(), &fireboltConnection, "SET key2=value")
	utils.RaiseIfError(t, err)
	// Check if parameters were set correctly
	if len(emptyClient.ParametersCalled) != 2 {
		t.Errorf("processSetStatement didn't set parameters correctly")
	}
	if _, ok := emptyClient.ParametersCalled[0]["key1"]; !ok {
		t.Errorf("processSetStatement didn't set parameter correctly")
	}
	if _, ok := emptyClient.ParametersCalled[1]["key2"]; !ok {
		t.Errorf("processSetStatement didn't set parameter correctly")
	}
	if _, ok := emptyClient.ParametersCalled[1]["key1"]; !ok {
		t.Errorf("processSetStatement didn't use previous parameters correctly")
	}
}

func TestFailingQueryDoesntSetParameter(t *testing.T) {
	connector := FireboltConnector{}
	emptyClient := client.MakeMockClientWithError(errors.New("dummy error"))

	fireboltConnection := fireboltConnection{emptyClient, "engine_url", map[string]string{}, &connector}
	var err error

	_, err = processSetStatement(context.TODO(), &fireboltConnection, "SET key1=value1")
	if err == nil {
		t.Errorf("processSetStatement didn't fail, but it should")
	}
	_, err = processSetStatement(context.TODO(), &fireboltConnection, "SET key2=value")
	if err == nil {
		t.Errorf("processSetStatement didn't fail, but it should")
	}
	// Check if parameters were set correctly
	if len(emptyClient.ParametersCalled) != 2 {
		t.Errorf("processSetStatement didn't set parameters correctly")
	}
	if _, ok := emptyClient.ParametersCalled[0]["key1"]; !ok {
		t.Errorf("processSetStatement didn't set parameter correctly")
	}
	if _, ok := emptyClient.ParametersCalled[1]["key2"]; !ok {
		t.Errorf("processSetStatement didn't set parameter correctly")
	}
	if _, ok := emptyClient.ParametersCalled[1]["key1"]; ok {
		t.Errorf("processSetStatement used previous parameter even though query failed")
	}
	if len(fireboltConnection.parameters) != 0 {
		t.Errorf("processSetStatement set parameters even though query failed")
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

	fireboltConnection.resetParameters()
	if len(fireboltConnection.parameters) != 3 {
		t.Errorf("resetParameters didn't remove parameters correctly")
	}
	if len(connector.cachedParameters) != 3 {
		t.Errorf("resetParameters didn't remove parameters in connector correctly")
	}
}
