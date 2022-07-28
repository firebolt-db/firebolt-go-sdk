package fireboltgosdk

import (
	"context"
	"database/sql/driver"
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
	execer  driver.ExecerContext
	queryer driver.QueryerContext

	query string
}

// Close the statement makes it unusable anymore
func (stmt *fireboltStmt) Close() error {
	stmt.execer = nil
	stmt.queryer = nil

	stmt.query = ""
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
	return stmt.queryer.QueryContext(ctx, stmt.query, args)
}

// ExecContext sends the query to the engine and returns empty fireboltResult
func (stmt *fireboltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return stmt.execer.ExecContext(ctx, stmt.query, args)
}
