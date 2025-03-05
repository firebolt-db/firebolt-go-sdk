package rows

import (
	"database/sql/driver"
	"encoding/json"
	errorUtils "errors"
	"io"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/logging"

	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type InMemoryRows struct {
	ColumnReader
	queryResponses    []types.QueryResponse
	cursorPosition    int
	resultSetPosition int
}

// Close makes the rows unusable
func (r *InMemoryRows) Close() error {
	r.resultSetPosition = len(r.queryResponses) - 1
	r.cursorPosition = len(r.queryResponses[r.resultSetPosition].Data)
	return nil
}

// Next fetches the values of the next row, returns io.EOF if it was the end
func (r *InMemoryRows) Next(dest []driver.Value) error {
	if r.cursorPosition == len(r.queryResponses[r.resultSetPosition].Data) {
		return io.EOF
	}

	for i, column := range r.queryResponses[r.resultSetPosition].Meta {
		var err error
		//log.Printf("Rows.Next: %s, %v", column.Type, r.queryResponses.Data[r.cursorPosition][i])
		if dest[i], err = parseValue(column.Type, r.queryResponses[r.resultSetPosition].Data[r.cursorPosition][i]); err != nil {
			return errors.ConstructNestedError("error during fetching Next result", err)
		}
	}

	r.cursorPosition++
	return nil
}

// HasNextResultSet reports whether there is another result set available
func (r *InMemoryRows) HasNextResultSet() bool {
	return len(r.queryResponses) > r.resultSetPosition+1
}

// NextResultSet advances to the next result set, if it is available, otherwise returns io.EOF
func (r *InMemoryRows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}

	r.cursorPosition = 0
	r.resultSetPosition += 1
	r.columns = r.queryResponses[r.resultSetPosition].Meta

	return nil
}

// AppendResponse appends the response to the InMemoryRows, parsing the response content
// and checking for errors in the response body
func (r *InMemoryRows) ProcessAndAppendResponse(response *client.Response) error {
	// Check for error in the Response body, despite the status code 200
	errorResponse := struct {
		Errors []types.ErrorDetails `json:"errors"`
	}{}
	content, err := response.Content()
	if err != nil {
		return errors.ConstructNestedError("error during reading response content", err)
	}
	if err := json.Unmarshal(content, &errorResponse); err == nil {
		if len(errorResponse.Errors) > 0 {
			return errors.NewStructuredError(errorResponse.Errors)
		}
	}

	// Unmarshal the response content
	var queryResponse types.QueryResponse
	// Response could be empty, which doesn't mean it is an error
	if len(content) != 0 {
		if err = json.Unmarshal(content, &queryResponse); err != nil {
			return errors.ConstructNestedError("wrong response", errorUtils.New(string(content)))
		}
		logging.Infolog.Printf("Query was successful")
	}

	r.queryResponses = append(r.queryResponses, queryResponse)
	if r.columns == nil {
		r.columns = r.queryResponses[r.resultSetPosition].Meta
	}

	return nil
}
