package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/firebolt-db/firebolt-go-sdk/statement"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/rows"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/utils"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
)

type fireboltConnection struct {
	client     client.Client
	engineUrl  string
	parameters map[string]string
	connector  *FireboltConnector
}

// Prepare returns a firebolt prepared statement
// returns an error if the connection isn't initialized or closed
func (c *fireboltConnection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.TODO(), query)
}

// PrepareContext returns a firebolt prepared statement
// returns an error if the connection isn't initialized or closed
func (c *fireboltConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.client != nil && len(c.engineUrl) != 0 {
		return statement.MakeStmt(c, query, contextUtils.GetPreparedStatementsStyle(ctx))
	}
	return nil, errors.New("firebolt connection isn't properly initialized")
}

// Close closes the connection, and make the fireboltConnection unusable
func (c *fireboltConnection) Close() error {
	c.client = nil
	c.parameters = make(map[string]string)
	c.engineUrl = ""
	return nil
}

// Begin is not implemented, as firebolt doesn't support transactions
func (c *fireboltConnection) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not implemented in firebolt")
}

// ExecContext sends the query to the engine and returns empty fireboltResult
func (c *fireboltConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt, err := statement.MakeStmt(c, query, contextUtils.GetPreparedStatementsStyle(ctx))
	if err != nil {
		return nil, errorUtils.ConstructNestedError("error during preparing a statement", err)
	}
	if rs, err := c.ExecutePreparedQueries(ctx, stmt.Queries, args, false); err != nil {
		return nil, errorUtils.Wrap(errorUtils.QueryExecutionError, err)
	} else {
		return rs.Result()
	}
}

// QueryContext sends the query to the engine and returns fireboltRows
func (c *fireboltConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := statement.MakeStmt(c, query, contextUtils.GetPreparedStatementsStyle(ctx))
	if err != nil {
		return nil, errorUtils.ConstructNestedError("error during preparing a statement", err)
	}
	return c.ExecutePreparedQueries(ctx, stmt.Queries, args, true)
}

func (c *fireboltConnection) makeRows(ctx context.Context) rows.ExtendableRowsWithResult {
	isAsync := contextUtils.IsAsync(ctx)
	if isAsync {
		return &rows.AsyncRows{}
	}
	isStreaming := contextUtils.IsStreaming(ctx)
	if isStreaming && isNewVersion(c) {
		return &rows.StreamRows{}
	}
	return &rows.InMemoryRows{}
}

func isNewVersion(c *fireboltConnection) bool {
	_, isV2 := c.client.(*client.ClientImpl)
	_, isCore := c.client.(*client.ClientImplCore)
	return isV2 || isCore
}

func (c *fireboltConnection) ExecutePreparedQueries(ctx context.Context, queries []statement.PreparedQuery, args []driver.NamedValue, isMultiStatementAllowed bool) (rows.ExtendableRowsWithResult, error) {
	if contextUtils.IsAsync(ctx) {
		if len(queries) > 1 {
			return nil, fmt.Errorf("multi statement queries cannot be executed assynchronously")
		} else if len(queries) == 1 {
			if _, ok := queries[0].(*statement.SetStatement); ok {
				return nil, fmt.Errorf("SET statements cannot be executed assynchronously")
			}
		}
	}

	if len(queries) > 1 && !isMultiStatementAllowed {
		return nil, fmt.Errorf("multistatement is not allowed")
	}

	var rowsInst = c.makeRows(ctx)

	connectionControl := client.ConnectionControl{
		UpdateParameters: c.setParameter,
		SetEngineURL:     c.setEngineURL,
		ResetParameters:  c.resetParameters,
	}

	for _, query := range queries {
		sql, additionalParameters, err := query.Format(args)
		if err != nil {
			return rowsInst, errorUtils.Wrap(errorUtils.QueryExecutionError, err)
		}
		parameters := mergeMaps(c.parameters, additionalParameters)
		if response, err := c.client.Query(ctx, c.engineUrl, sql, parameters, connectionControl); err != nil {
			return rowsInst, errorUtils.Wrap(errorUtils.QueryExecutionError, err)
		} else if err = rowsInst.ProcessAndAppendResponse(response); err != nil {
			return rowsInst, errorUtils.Wrap(errorUtils.QueryExecutionError, err)
		} else {
			query.OnSuccess(connectionControl)
		}
	}
	return rowsInst, nil
}

func (c *fireboltConnection) setParameter(key, value string) {
	if c.parameters == nil {
		c.parameters = make(map[string]string)
	}
	c.parameters[key] = value
	// Cache parameter in connector as well in case connection will be recreated by the pool
	if c.connector.cachedParameters == nil {
		c.connector.cachedParameters = make(map[string]string)
	}
	c.connector.cachedParameters[key] = value
}

func (c *fireboltConnection) setEngineURL(engineUrl string) {
	c.engineUrl = engineUrl
}

func (c *fireboltConnection) resetParameters() {
	ignoreParameters := append(statement.GetUseParametersList(), statement.GetDisallowedParametersList()...)
	if c.parameters != nil {
		for k := range c.parameters {
			if !utils.ContainsString(ignoreParameters, k) {
				delete(c.parameters, k)
			}
		}
	}
	if c.connector.cachedParameters != nil {
		for k := range c.connector.cachedParameters {
			if !utils.ContainsString(ignoreParameters, k) {
				delete(c.connector.cachedParameters, k)
			}
		}
	}
}
