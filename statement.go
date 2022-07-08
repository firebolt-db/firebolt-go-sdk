package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"encoding/json"
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

func (stmt fireboltStmt) Close() error {
	return nil
}

func (stmt fireboltStmt) NumInput() int {
	return -1
}

func (stmt fireboltStmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.ExecContext(context.TODO(), make([]driver.NamedValue, 0))
}

func (stmt fireboltStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.QueryContext(context.TODO(), make([]driver.NamedValue, 0))
}

func (stmt fireboltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	params := make(map[string]string)
	params["database"] = stmt.databaseName
	params["output_format"] = "FB_JSONCompactLimited"

	response, err := stmt.client.Request("POST", stmt.engineUrl, params, stmt.query)
	if err != nil {
		return nil, fmt.Errorf("error ducting query execution: %v", err)
	}

	var queryResponse QueryResponse
	if err = json.Unmarshal(response, &queryResponse); err != nil {
		return nil, fmt.Errorf("error ducting unmarshalling query response: %v", err)
	}

	return &fireboltRows{queryResponse, 0}, nil
}

func (stmt fireboltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	params := make(map[string]string)
	params["database"] = stmt.databaseName
	params["output_format"] = "FB_JSONCompactLimited"

	var queryResponse QueryResponse
	response, err := stmt.client.Request("POST", stmt.engineUrl, params, stmt.query)
	if err != nil {
		return nil, fmt.Errorf("error ducting query execution: %v", err)
	}

	if err = json.Unmarshal(response, &queryResponse); err != nil {
		return nil, fmt.Errorf("error during unmarshalling query response: %v", err)
	}

	return &FireboltResult{}, nil
}
