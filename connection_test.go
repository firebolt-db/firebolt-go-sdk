package fireboltgosdk

import (
	"context"
	"strings"
	"testing"
)

// TestConnectionPrepareStatement, tests that prepare statement doesn't result into an error
func TestConnectionPrepareStatement(t *testing.T) {
	emptyClient := ClientImplV0{}
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
	emptyClient := ClientImplV0{}
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
	emptyClient := ClientImpl{} // Client version is irrelevant for this test
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
	runProcessSetStatementFail(t, "SET account_id=1")
	runProcessSetStatementFail(t, "SET output_format='json'")
}
