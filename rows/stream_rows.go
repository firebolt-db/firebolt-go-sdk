package rows

import (
	"bufio"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type StreamRows struct {
	responses         []*client.Response
	resultSetPosition int
	// current row
	columns          []types.Column
	rowScanner       *bufio.Scanner
	dataBuffer       [][]interface{}
	dataBufferCursor int
	consumedResponse bool
}

func readJsonLine(scanner *bufio.Scanner) (types.JSONLinesRecord, error) {
	var record types.JSONLinesRecord
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return record, errorUtils.ConstructNestedError("Error reading JSON line:", err)
	}
	err := json.Unmarshal(scanner.Bytes(), &record)
	if err != nil {
		return record, errorUtils.ConstructNestedError("JSON parse error:", err)
	}
	return record, nil
}

func (r *StreamRows) scanner() *bufio.Scanner {
	if r.rowScanner == nil {
		r.rowScanner = bufio.NewScanner(r.responses[r.resultSetPosition].Body())
	}
	return r.rowScanner
}

// Columns returns a list of Meta names in response
func (r *StreamRows) Columns() []string {
	numColumns := len(r.columns)
	result := make([]string, 0, numColumns)

	for _, column := range r.columns {
		result = append(result, column.Name)
	}

	return result
}

// Close makes the rows unusable
func (r *StreamRows) Close() error {
	for i := r.resultSetPosition; i < len(r.responses); i++ {
		err := r.responses[i].Body().Close()
		if err != nil {
			return errorUtils.ConstructNestedError("Error closing response body:", err)
		}
	}
	return nil
}

func (r *StreamRows) bufferHasMoreData() bool {
	return r.dataBuffer != nil && r.dataBufferCursor < len(r.dataBuffer)
}

func (r *StreamRows) populateDataBuffer() error {
	nextRecord, err := readJsonLine(r.scanner())
	if err != nil {
		return errorUtils.ConstructNestedError("Error reading JSON line:", err)
	}
	if nextRecord.MessageType == types.MessageTypeError {
		errors := make([]types.ErrorDetails, 0)
		if nextRecord.Errors != nil {
			errors = *nextRecord.Errors
		}
		return errorUtils.NewStructuredError(errors)
	} else if nextRecord.MessageType != types.MessageTypeSuccess {
		r.consumedResponse = true
		return io.EOF
	} else {
		if nextRecord.MessageType != types.MessageTypeData {
			return fmt.Errorf("unexpected message type returned from the server %s", nextRecord.MessageType)
		}
		r.dataBuffer = *nextRecord.Data
		r.dataBufferCursor = 0
	}
	return nil
}

// Next fetches the values of the next row, returns io.EOF if it was the end
func (r *StreamRows) Next(dest []driver.Value) error {
	if r.consumedResponse {
		return io.EOF
	}
	if !r.bufferHasMoreData() {
		if err := r.populateDataBuffer(); err != nil {
			return err
		}
	}

	for i, column := range r.columns {
		var err error
		//log.Printf("Rows.Next: %s, %v", column.Type, r.queryResponses.Data[r.cursorPosition][i])
		if dest[i], err = parseValue(column.Type, r.dataBuffer[r.dataBufferCursor][i]); err != nil {
			return errorUtils.ConstructNestedError("error during fetching Next result", err)
		}
	}
	r.dataBufferCursor++
	return nil
}

// HasNextResultSet reports whether there is another result set available
func (r *StreamRows) HasNextResultSet() bool {
	return r.resultSetPosition < len(r.responses)-1
}

// NextResultSet advances to the next result set, if it is available, otherwise returns io.EOF
func (r *StreamRows) NextResultSet() error {
	err := r.responses[r.resultSetPosition].Body().Close()
	if err != nil {
		return errorUtils.ConstructNestedError("Error closing response body:", err)
	}

	r.resultSetPosition++
	r.rowScanner = nil
	r.dataBuffer = nil
	r.dataBufferCursor = 0
	r.consumedResponse = false

	// Fetch metadata for the next result set
	if startRecord, err := readJsonLine(r.scanner()); err != nil {
		return err
	} else if startRecord.MessageType != types.MessageTypeStart {
		return fmt.Errorf("unexpected first message type returned from the server %s", startRecord.MessageType)
	} else if startRecord.ResultColumns == nil {
		return fmt.Errorf("no columns metadata returned from the server")
	} else {
		r.columns = *startRecord.ResultColumns
	}
	return nil
}

func (r *StreamRows) AppendResponse(response *client.Response) error {
	r.responses = append(r.responses, response)
	return nil
}
