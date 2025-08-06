package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/firebolt-db/firebolt-go-sdk/rows"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
)

func convertToNamedValues(args []any) ([]driver.NamedValue, error) {
	named := make([]driver.NamedValue, len(args))
	for i, a := range args {
		val, err := driver.DefaultParameterConverter.ConvertValue(a)
		if err != nil {
			return nil, fmt.Errorf("failed to convert arg[%d]=%v (%T): %w", i, a, a, err)
		}
		named[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   val,
		}
	}
	return named, nil
}

func ExecAsync(db *sql.DB, query string, args ...any) (rows.AsyncResult, error) {
	// This function should execute an asynchronous query and return a token.
	// For the purpose of this example, we will return a dummy token.
	return ExecAsyncContext(context.Background(), db, query, args...)
}

func ExecAsyncContext(ctx context.Context, db *sql.DB, query string, args ...any) (rows.AsyncResult, error) {
	// This function should execute an asynchronous query with a context and return a token.
	// For the purpose of this example, we will return a dummy token.
	asyncContext := contextUtils.WithAsync(ctx)
	conn, err := db.Conn(asyncContext)
	if err != nil {
		return rows.AsyncResult{}, fmt.Errorf("failed to get database connection: %w", err)
	}
	var res driver.Result
	err = conn.Raw(func(driverConn any) error {
		if driverConn == nil {
			return fmt.Errorf("failed to get raw connection from db: %v", db)
		}
		driverValues, err := convertToNamedValues(args)
		if err != nil {
			return fmt.Errorf("failed to convert args: %w", err)
		}
		if fireboltConn, ok := driverConn.(*fireboltConnection); !ok {
			return errors.New("can only execute async queries on a Firebolt connection")
		} else {
			res, err = fireboltConn.ExecContext(asyncContext, query, driverValues)
			return err
		}
	})
	if err != nil {
		return rows.AsyncResult{}, err
	}
	if asyncRes, ok := res.(*rows.AsyncResult); ok {
		return *asyncRes, nil
	}
	return rows.AsyncResult{}, fmt.Errorf("failed to execute async query, unexpected result type %T", res)
}
