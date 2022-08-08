package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"errors"
)

type fireboltConnection struct {
	client        *Client
	databaseName  string
	engineUrl     string
	setStatements map[string]string
}

// Prepare returns a firebolt prepared statement
// returns an error if the connection isn't initialized or closed
func (c *fireboltConnection) Prepare(query string) (driver.Stmt, error) {
	if c.client != nil && len(c.databaseName) != 0 && len(c.engineUrl) != 0 {
		return &fireboltStmt{execer: c, queryer: c, query: query}, nil
	}
	return nil, errors.New("fireboltConnection isn't properly initialized")
}

// Close closes the connection, and make the fireboltConnection unusable
func (c *fireboltConnection) Close() error {
	c.client = nil
	c.databaseName = ""
	c.engineUrl = ""
	return nil
}

// Begin is not implemented, as firebolt doesn't support transactions
func (c *fireboltConnection) Begin() (driver.Tx, error) {
	panic("Transactions are not implemented in firebolt")
}

// ExecContext sends the query to the engine and returns empty fireboltResult
func (c *fireboltConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	query, err := prepareStatement(query, args)
	if err != nil {
		return nil, ConstructNestedError("error during preparing a statement", err)
	}
	if processSetStatement(c, query) {
		return &FireboltResult{}, nil
	}

	var queryResponse QueryResponse
	if err := c.client.Query(c.engineUrl, c.databaseName, query, &c.setStatements, &queryResponse); err != nil {
		return nil, ConstructNestedError("error during query execution", err)
	}

	return &FireboltResult{}, nil
}

// QueryContext sends the query to the engine and returns fireboltRows
func (c *fireboltConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	query, err := prepareStatement(query, args)
	if err != nil {
		return nil, ConstructNestedError("error during preparing a statement", err)
	}
	if processSetStatement(c, query) {
		return &fireboltRows{QueryResponse{}, 0}, nil
	}

	var queryResponse QueryResponse
	if err := c.client.Query(c.engineUrl, c.databaseName, query, &c.setStatements, &queryResponse); err != nil {
		return nil, ConstructNestedError("error during query execution", err)
	}

	return &fireboltRows{queryResponse, 0}, nil
}

// processSetStatement is an internal function for checking whether query is a valid set statement
// and updating set statement map of the fireboltConnection
func processSetStatement(c *fireboltConnection, query string) bool {
	if setKey, setValue, err := parseSetStatement(query); err == nil {
		if nil == c.client.Query(c.engineUrl, c.databaseName, "SELECT 1", &map[string]string{setKey: setValue}, &QueryResponse{}) {
			c.setStatements[setKey] = setValue
			return true
		}
	}
	return false
}
