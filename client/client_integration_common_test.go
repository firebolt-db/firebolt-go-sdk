//go:build integration || integration_v0
// +build integration integration_v0

package client

import (
	"context"
	"encoding/json"
	errorUtils "errors"
	"fmt"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

func parseResponse(t *testing.T, response *Response, expectedValue interface{}) (*types.QueryResponse, error) {
	content, err := response.Content()
	if err != nil {
		return nil, fmt.Errorf("Error getting response content: %v", err)
	}

	// Unmarshal the response content
	var queryResponse types.QueryResponse
	// Response could be empty, which doesn't mean it is an error
	if len(content) != 0 {
		if err = json.Unmarshal(content, &queryResponse); err != nil {
			return nil, errors.ConstructNestedError("wrong response", errorUtils.New(string(content)))
		}
	}
	return &queryResponse, nil

}

// TestQuery tests simple query
func TestQuery(t *testing.T) {
	response, err := clientMock.Query(context.TODO(), engineUrlMock, "SELECT 1", map[string]string{"database": databaseMock}, ConnectionControl{})
	if err != nil {
		t.Errorf("Query returned an error: %v", err)
		t.FailNow()
	}
	queryResponse, err := parseResponse(t, response, 1)
	if err != nil {
		t.Errorf("Error parsing response: %v", err)
		t.FailNow()
	}
	if queryResponse.Rows != 1 {
		t.Errorf("Query response has an invalid number of rows %d != %d", queryResponse.Rows, 1)
		t.FailNow()
	}

	if queryResponse.Data[0][0].(float64) != 1 {
		t.Errorf("queryResponse data is not correct")
	}
}
