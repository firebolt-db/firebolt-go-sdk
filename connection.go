package fireboltgosdk

import (
	"database/sql/driver"
)

type fireboltConnection struct {
	client       Client
	databaseName string
	engineUrl    string
}

func (c *fireboltConnection) Prepare(query string) (driver.Stmt, error) {
	return fireboltStmt{client: &c.client, query: query}, nil
}

func (c *fireboltConnection) Close() error {
	return nil
}

func (c *fireboltConnection) Begin() (driver.Tx, error) {
	return nil, nil
}
