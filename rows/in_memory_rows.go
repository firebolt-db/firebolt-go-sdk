package rows

import (
	"database/sql/driver"
	"io"

	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type InMemoryRows struct {
	queryResponses    []types.QueryResponse
	cursorPosition    int
	resultSetPosition int
}

// Columns returns a list of Meta names in response
func (r *InMemoryRows) Columns() []string {
	numColumns := len(r.queryResponses[r.resultSetPosition].Meta)
	result := make([]string, 0, numColumns)

	for _, column := range r.queryResponses[r.resultSetPosition].Meta {
		result = append(result, column.Name)
	}

	return result
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

	return nil
}

func (r *InMemoryRows) AppendResponse(response types.QueryResponse) {
	r.queryResponses = append(r.queryResponses, response)
}
