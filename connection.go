package fireboltgosdk

import (
	"database/sql/driver"
	"errors"
)

type fireboltConnection struct {
	client       *Client
	databaseName string
	engineUrl    string
}

// Prepare returns a firebolt prepared statement
// returns an error if the connection isn't initialized or closed
func (c *fireboltConnection) Prepare(query string) (driver.Stmt, error) {
	if c.client != nil && len(c.databaseName) != 0 && len(c.engineUrl) != 0 {
		return &fireboltStmt{client: c.client, query: query, databaseName: c.databaseName, engineUrl: c.engineUrl}, nil
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
