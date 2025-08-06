package rows

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"log"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
)

func TestAsyncRowsStandardMethods(t *testing.T) {
	asyncRows := &AsyncRows{}
	if err := asyncRows.Close(); err != nil {
		t.Errorf("AsyncRows Close failed with %v", err)
	}
	if cols := asyncRows.Columns(); len(cols) != 0 {
		t.Errorf("AsyncRows Columns returned %d columns, expected 0", len(cols))
	}
	if err := asyncRows.Next([]driver.Value{}); err != io.EOF {
		t.Error("AsyncRows Next should return io.EOF, got:", err)
	}
	if has := asyncRows.HasNextResultSet(); has {
		t.Error("AsyncRows HasNextResultSet should return false, got true")
	}
	if err := asyncRows.NextResultSet(); err != io.EOF {
		t.Error("AsyncRows NextResultSet should return io.EOF, got:", err)
	}
}

func mockAsyncResponse(token, monitorSQL string) *client.Response {
	response := AsyncResponse{
		Message:    "mock_message",
		MonitorSql: monitorSQL,
		Token:      token,
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}
	reader := io.NopCloser(bytes.NewReader(jsonData))
	return client.MakeResponse(reader, 202, nil, nil)
}

func TestAsyncRowsProcessAndAppendResponse(t *testing.T) {
	var rows AsyncRows

	if rows.result != nil {
		t.Error("Expected result to be nil, got:", rows.result)
	}

	token := "mock_token"
	monitorSQL := "mock_monitor_sql"
	response := mockAsyncResponse(token, monitorSQL)
	if err := rows.ProcessAndAppendResponse(response); err != nil {
		t.Errorf("ProcessAndAppendResponse failed with %v", err)
	}
	if rows.result == nil {
		t.Error("Expected result to be set, got nil")
	}
	if rows.result.token != token {
		t.Errorf("Expected token %s, got %s", token, rows.result.token)
	}
	if rows.result.monitorSQL != monitorSQL {
		t.Errorf("Expected monitorSQL %s, got %s", monitorSQL, rows.result.monitorSQL)
	}
}

func TestAsyncRowsProcessAndAppendResponseNonAsyncResponse(t *testing.T) {
	var rows AsyncRows

	response := client.MakeResponse(io.NopCloser(bytes.NewReader([]byte{})), 200, nil, nil)
	if err := rows.ProcessAndAppendResponse(response); err == nil {
		t.Error("Expected error for non-async response, got nil")
	} else if err.Error() != "expected to receive an async response, but got a regular response" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestAsyncRowsProcessAndAppendResponseAlreadyReturned(t *testing.T) {
	var rows AsyncRows
	token := "mock_token"
	monitorSQL := "mock_monitor_sql"
	response := mockAsyncResponse(token, monitorSQL)

	if err := rows.ProcessAndAppendResponse(response); err != nil {
		t.Errorf("ProcessAndAppendResponse failed with %v", err)
	}
	if err := rows.ProcessAndAppendResponse(response); err == nil {
		t.Error("Expected error for already returned async query, got nil")
	} else if err.Error() != "async query already returned a token" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestAsyncRowsProcessAndAppendResponseInvalidResponse(t *testing.T) {
	var rows AsyncRows

	// Create a response that does not match the expected structure
	response := client.MakeResponse(io.NopCloser(bytes.NewReader([]byte("invalid response"))), 202, nil, nil)
	if err := rows.ProcessAndAppendResponse(response); err == nil {
		t.Error("Expected error for invalid async response, got nil")
	} else if !errors.Is(err, errors.New("error during parsing async response")) {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestAsyncRowsResult(t *testing.T) {
	asyncRows := &AsyncRows{}

	token := "test_token"
	monitorSQL := "test_monitor_sql"
	response := mockAsyncResponse(token, monitorSQL)

	if err := asyncRows.ProcessAndAppendResponse(response); err != nil {
		t.Errorf("ProcessAndAppendResponse failed with %v", err)
	}

	result, err := asyncRows.Result()
	if err != nil {
		t.Errorf("AsyncRows Result failed with %v", err)
	}

	if asyncRes, ok := result.(*AsyncResult); !ok {
		t.Errorf("Expected AsyncResult, got %T", result)
	} else {
		if asyncRes.GetToken() != token {
			t.Errorf("Expected token %s, got %s", token, asyncRes.token)
		}
		if asyncRes.GetMonitorSQL() != monitorSQL {
			t.Errorf("Expected monitorSQL %s, got %s", monitorSQL, asyncRes.monitorSQL)
		}
	}
}

func TestAsyncRowsResultEmpty(t *testing.T) {
	asyncRows := &AsyncRows{}

	_, err := asyncRows.Result()
	if err == nil {
		t.Errorf("Expected error when calling Result on empty AsyncRows, got nil")
	} else if err.Error() != "async query didn't return a token" {
		t.Errorf("Unexpected error message: %v", err)
	}
}
