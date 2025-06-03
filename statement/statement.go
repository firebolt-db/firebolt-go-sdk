package statement

import (
	"context"
	"database/sql/driver"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
)

type Executor interface {
	ExecutePreparedQueries(ctx context.Context, queries []PreparedQuery, args []driver.NamedValue, isQuery bool) (driver.Rows, error)
}

type fireboltStmt struct {
	Queries  []PreparedQuery
	executor Executor
}

func MakeStmt(executor Executor, query string, style contextUtils.PreparedStatementsStyle) (*fireboltStmt, error) {
	preparedQueries, err := prepareQuery(query, style)
	if err != nil {
		return nil, err
	}

	return &fireboltStmt{
		Queries:  preparedQueries,
		executor: executor,
	}, nil
}

// Close the statement makes it unusable anymore
func (stmt *fireboltStmt) Close() error {
	stmt.executor = nil
	stmt.Queries = nil
	return nil
}

// NumInput returns the number of input parameters
func (stmt *fireboltStmt) NumInput() int {
	if len(stmt.Queries) == 0 {
		return -1
	}
	return stmt.Queries[0].GetNumParams()
}

// Exec calls ExecContext with dummy context
func (stmt *fireboltStmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.ExecContext(context.TODO(), valueToNamedValue(args))
}

// Query calls QueryContext with dummy context
func (stmt *fireboltStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.QueryContext(context.TODO(), valueToNamedValue(args))
}

// QueryContext sends the query to the engine and returns fireboltRows
func (stmt *fireboltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return stmt.executor.ExecutePreparedQueries(ctx, stmt.Queries, args, true)
}

// ExecContext sends the query to the engine and returns empty fireboltResult
func (stmt *fireboltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	_, err := stmt.executor.ExecutePreparedQueries(ctx, stmt.Queries, args, true)
	if err != nil {
		return nil, err
	}
	return FireboltResult{}, nil
}
