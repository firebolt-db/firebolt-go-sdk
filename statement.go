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
	client       *Client
	query        string
	databaseName string
	engineUrl    string
}

func (stmt fireboltStmt) Close() error {
	return nil
}

func (stmt fireboltStmt) NumInput() int {
	return -1
}

func (stmt fireboltStmt) Exec(args []driver.Value) (driver.Result, error) {
	params := make(map[string]string)
	params["database"] = stmt.databaseName
	params["output_format"] = "FB_JSONCompactLimited"

	_, err := stmt.client.Request("POST", stmt.engineUrl, params, stmt.query)
	return FireboltResult{}, err
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
