package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type QueryResponse struct {
	Query      interface{}     `json:"query"`
	Meta       []Column        `json:"meta"`
	Data       [][]interface{} `json:"data"`
	Rows       int             `json:"rows"`
	Statistics interface{}     `json:"statistics"`
}

type fireboltStmt struct {
	client       *Client
	query        string
	databaseName string
	engineUrl    string
}

// Close the statement makes it unusable anymore
func (stmt *fireboltStmt) Close() error {
	stmt.client = nil
	stmt.query = ""
	stmt.databaseName = ""
	stmt.engineUrl = ""
	return nil
}

// NumInput returns -1, parametrized queries are not implemented
func (stmt *fireboltStmt) NumInput() int {
	return -1
}

// Exec calls ExecContext with dummy context
func (stmt *fireboltStmt) Exec(args []driver.Value) (driver.Result, error) {
	if len(args) != 0 {
		panic("Prepared statements are not implemented")
	}
	return stmt.ExecContext(context.TODO(), make([]driver.NamedValue, 0))
}

// Query calls QueryContext with dummy context
func (stmt *fireboltStmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) != 0 {
		panic("Prepared statements are not implemented")
	}
	return stmt.QueryContext(context.TODO(), make([]driver.NamedValue, 0))
}

// QueryContext sends the query to the engine and returns fireboltRows
func (stmt *fireboltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		panic("Prepared statements are not implemented")
	}

	var queryResponse QueryResponse
	if err := stmt.client.Query(stmt.engineUrl, stmt.databaseName, stmt.query, &queryResponse); err != nil {
		return nil, fmt.Errorf("error during query execution: %v", err)
	}

	return &fireboltRows{queryResponse, 0}, nil
}

// ExecContext sends the query to the engine and returns empty fireboltResult
func (stmt *fireboltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) != 0 {
		panic("Prepared statements are not implemented")
	}
	var queryResponse QueryResponse
	if err := stmt.client.Query(stmt.engineUrl, stmt.databaseName, stmt.query, &queryResponse); err != nil {
		return nil, fmt.Errorf("error during query execution: %v", err)
	}

	return &FireboltResult{}, nil
}
