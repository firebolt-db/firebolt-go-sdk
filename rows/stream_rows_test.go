package rows

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/types"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

const queryId = "16FF2A0300ECA753"

func mockStreamRows(isMultiStatement bool) driver.RowsNextResultSet {

	rows := &StreamRows{}
	var responseFiles = []string{"fixtures/result1.jsonl", "fixtures/result2.jsonl"}
	for i := 0; i < 2; i += 1 {
		resultJson, err := os.ReadFile(responseFiles[i])
		if err != nil {
			log.Fatalf("Error reading file: %v", err)
		}
		if i != 0 && !isMultiStatement {
			break
		}
		reader := io.NopCloser(bytes.NewReader(resultJson))
		must(rows.ProcessAndAppendResponse(client.MakeResponse(reader, 200, nil, nil)))
	}

	return rows
}

func must(err error) {
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func stringPrt(s string) *string {
	return &s
}

func mockStreamRowsSingleValue(value interface{}, columnType string) driver.RowsNextResultSet {
	response := []types.JSONLinesRecord{
		{
			MessageType:   types.MessageTypeStart,
			ResultColumns: &[]types.Column{{Name: "single_col", Type: columnType}},
			QueryID:       stringPrt(queryId),
		},
		{
			MessageType: types.MessageTypeData,
			Data:        &[][]interface{}{{value}},
		},
		{
			MessageType: types.MessageTypeSuccess,
			Statistics:  nil,
		},
	}

	rows := &StreamRows{}
	responseBody := strings.Builder{}
	for _, record := range response {
		jsonData, err := json.Marshal(record)
		if err != nil {
			log.Fatalf("Error marshaling JSON: %v", err)
		}
		responseBody.WriteString(string(jsonData) + "\n")
	}
	reader := io.NopCloser(bytes.NewReader([]byte(responseBody.String())))
	must(rows.ProcessAndAppendResponse(client.MakeResponse(reader, 200, nil, nil)))
	// Convert them to json lines

	return rows
}

// testRowsColumns checks, that correct column names are returned
func TestStreamRowsColumns(t *testing.T) {
	testRowsColumns(t, mockStreamRows)
}

// testRowsClose checks Close method, and inability to use rows afterward
func TestStreamRowsClose(t *testing.T) {
	testRowsClose(t, mockStreamRows)

}

// testRowsNext check Next method
func TestStreamRowsNext(t *testing.T) {
	testRowsNext(t, mockStreamRows)
}

// testRowsNextSet check rows with multiple statements
func TestStreamRowsNextSet(t *testing.T) {
	testRowsNextSet(t, mockStreamRows)
}

func TestStreamRowsNextStructError(t *testing.T) {
	testRowsNextStructError(t, mockStreamRows)
}

func TestStreamRowsNextStructWithNestedSpaces(t *testing.T) {
	testRowsNextStructWithNestedSpaces(t, mockStreamRows)
}

func TestStreamRowsQuotedLong(t *testing.T) {
	testRowsQuotedLong(t, mockStreamRowsSingleValue)
}

func TestStreamRowsDecimalType(t *testing.T) {
	testRowsDecimalType(t, mockStreamRowsSingleValue)
}

func TestStreamRowsError(t *testing.T) {
	rows := &StreamRows{}
	responseFile := "fixtures/error.jsonl"
	resultJson, err := os.ReadFile(responseFile)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	reader := io.NopCloser(bytes.NewReader(resultJson))
	must(rows.ProcessAndAppendResponse(client.MakeResponse(reader, 200, nil, nil)))

	dest := make([]driver.Value, 1)

	// Check that the error is returned on third row
	for i := 0; i < 2; i += 1 {
		err = rows.Next(dest)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
	err = rows.Next(dest)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}

	// rows are closed now
	utils.AssertEqual(rows.Next(dest), io.EOF, t, "Expected io.EOF")
	utils.AssertEqual(rows.HasNextResultSet(), false, t, "Expected false for HasNextResultSet")
	utils.AssertEqual(rows.NextResultSet(), io.EOF, t, "Expected io.EOF for NextResultSet")
}
