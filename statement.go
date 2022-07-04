package fireboltgosdk

import (
	"context"
	"database/sql/driver"
)

type fireboltStmt struct {
	client *Client
	query  string
}

func (stmt fireboltStmt) Close() error {
	return nil
}

func (stmt fireboltStmt) NumInput() int {
	return -1
}

func (stmt fireboltStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, nil
}

func (stmt fireboltStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, nil
}

func (stmt fireboltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return nil, nil
}

func (stmt fireboltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return nil, nil
}
