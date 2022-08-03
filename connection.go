package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
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
	return nil, fmt.Errorf("Transactions are not implemented in firebolt")
}

// ExecContext sends the query to the engine and returns empty fireboltResult
func (c *fireboltConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, err := c.QueryContext(ctx, query, args)
	return &FireboltResult{}, err
}

// QueryContext sends the query to the engine and returns fireboltRows
func (c *fireboltConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("Prepared statements are not implemented")
	}

	if isSetStatement, err := processSetStatement(ctx, c, query); isSetStatement {
		if err == nil {
			return &fireboltRows{QueryResponse{}, 0}, nil
		} else {
			return nil, ConstructNestedError("statement recognized as an invalid set statement", err)
		}
	}

	queryResponse, err := c.client.Query(ctx, c.engineUrl, c.databaseName, query, &c.setStatements)
	if err != nil {
		return nil, ConstructNestedError("error during query execution", err)
	}

	return &fireboltRows{*queryResponse, 0}, nil
}

// processSetStatement is an internal function for checking whether query is a valid set statement
// and updating set statement map of the fireboltConnection
func processSetStatement(ctx context.Context, c *fireboltConnection, query string) (bool, error) {
	setKey, setValue, err := parseSetStatement(query)
	if err != nil {
		// if parsing of set statement returned an error, we will not handle the request as a set statement
		return false, nil
	}

	_, err = c.client.Query(ctx, c.engineUrl, c.databaseName, "SELECT 1", &map[string]string{setKey: setValue})
	if err == nil {
		c.setStatements[setKey] = setValue
		return true, nil
	}
	return true, err
}
