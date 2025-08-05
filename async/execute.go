package async

import (
	"context"
	"database/sql"
	"fmt"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
)

func ExecAsync(db *sql.DB, query string, params map[string]interface{}) (string, error) {
	// This function should execute an asynchronous query and return a token.
	// For the purpose of this example, we will return a dummy token.
	return ExecAsyncContext(context.Background(), db, query, params)
}

func ExecAsyncContext(ctx context.Context, db *sql.DB, query string, params map[string]interface{}) (string, error) {
	// This function should execute an asynchronous query with a context and return a token.
	// For the purpose of this example, we will return a dummy token.
	asyncContext := contextUtils.WithAsync(ctx)
	res, err := db.ExecContext(asyncContext, query, params)
	if err != nil {
		return "", err
	}
	if asyncRes, ok := res.(FireboltAsyncResult); ok {
		return asyncRes.GetToken(), nil
	}
	return "", fmt.Errorf("failed to execute async query, unexpected result type %T", res)
}
