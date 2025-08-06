package rows

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
)

type AsyncResult struct {
	token      string
	monitorSQL string
}

func NewAsyncResult(token string, monitorSQL string) *AsyncResult {
	return &AsyncResult{
		token:      token,
		monitorSQL: monitorSQL,
	}
}

func (r AsyncResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r AsyncResult) RowsAffected() (int64, error) {
	return 0, nil
}

func (r AsyncResult) GetToken() string {
	return r.token
}

func (r AsyncResult) GetMonitorSQL() string {
	return r.monitorSQL
}

func (r AsyncResult) IsEmpty() bool {
	return r.token == ""
}

type AsyncResponse struct {
	Message    string `json:"message"`
	MonitorSql string `json:"monitorSql"`
	Token      string `json:"token"`
}

type AsyncRows struct {
	result *AsyncResult
}

// Close makes the rows unusable
func (r *AsyncRows) Close() error {
	return nil
}

// Columns returns the names of the columns in the result set
func (r *AsyncRows) Columns() []string {
	return []string{}
}

// Next fetches the values of the next row, returns io.EOF if it was the end
func (r *AsyncRows) Next(dest []driver.Value) error {
	return io.EOF
}

// HasNextResultSet reports whether there is another result set available
func (r *AsyncRows) HasNextResultSet() bool {
	return false
}

// NextResultSet advances to the next result set, if it is available, otherwise returns io.EOF
func (r *AsyncRows) NextResultSet() error {
	return io.EOF

}

// ProcessAndAppendResponse appends a response to the list of row streams
func (r *AsyncRows) ProcessAndAppendResponse(response *client.Response) error {
	if r.result != nil {
		return errors.New("async query already returned a token")
	}
	if !response.IsAsyncResponse() {
		return errors.New("expected to receive an async response, but got a regular response")
	}
	// parse
	var asyncResponse AsyncResponse
	content, err := response.Content()
	if err != nil {
		return errorUtils.ConstructNestedError("error during reading async response content", err)
	}
	if err := json.Unmarshal(content, &asyncResponse); err != nil {
		return errorUtils.ConstructNestedError("error during parsing async response", err)
	}
	r.result = NewAsyncResult(asyncResponse.Token, asyncResponse.MonitorSql)
	return nil
}

func (r *AsyncRows) Result() (driver.Result, error) {
	if r.result == nil {
		return nil, errors.New("async query didn't return a token")
	}
	return r.result, nil
}
