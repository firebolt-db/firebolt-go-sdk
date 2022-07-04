package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"encoding/json"
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
	params := make(map[string]string)
	params["database"] = "yury_test_db"
	params["output_format"] = "FB_JSONCompactLimited"

	var url_str = "https://yury-test-db-general-purpose.firebolt.us-east-1.app.firebolt.io/"

	body, err := stmt.client.Request("POST", url_str, params, stmt.query)
	if err != nil {
		return FireboltResult{}, nil
	}

	var response QueryResponse
	if err = json.Unmarshal(body, &response); err != nil {
		return FireboltResult{}, nil
	}

	return FireboltResult{}, nil
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
