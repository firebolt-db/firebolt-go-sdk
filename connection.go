package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

type fireboltConnection struct {
	client     Client
	engineUrl  string
	parameters map[string]string
	connector  *FireboltConnector
}

// Prepare returns a firebolt prepared statement
// returns an error if the connection isn't initialized or closed
func (c *fireboltConnection) Prepare(query string) (driver.Stmt, error) {
	if c.client != nil && len(c.engineUrl) != 0 {
		return &fireboltStmt{execer: c, queryer: c, query: query}, nil
	}
	return nil, errors.New("fireboltConnection isn't properly initialized")
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
	_, err := c.queryContextInternal(ctx, query, args, false)
	return &FireboltResult{}, err
}

// QueryContext sends the query to the engine and returns fireboltRows
func (c *fireboltConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.queryContextInternal(ctx, query, args, true)
}

func (c *fireboltConnection) queryContextInternal(ctx context.Context, query string, args []driver.NamedValue, isMultiStatementAllowed bool) (driver.Rows, error) {
	query, err := prepareStatement(query, args)
	if err != nil {
		return nil, ConstructNestedError("error during preparing a statement", err)
	}
	queries, err := SplitStatements(query)
	if err != nil {
		return nil, ConstructNestedError("error during splitting query", err)
	}
	if len(queries) > 1 && !isMultiStatementAllowed {
		return nil, fmt.Errorf("multistatement is not allowed")
	}

	var rows fireboltRows
	for _, query := range queries {
		if isSetStatement, err := processSetStatement(ctx, c, query); isSetStatement {
			if err == nil {
				rows.response = append(rows.response, QueryResponse{})
				continue
			} else {
				return &rows, ConstructNestedError("statement recognized as an invalid set statement", err)
			}
		}

		if response, err := c.client.Query(ctx, c.engineUrl, query, c.parameters, connectionControl{
			updateParameters: c.setParameter,
			setEngineURL:     c.setEngineURL,
			resetParameters:  c.resetParameters,
		}); err != nil {
			return &rows, ConstructNestedError("error during query execution", err)
		} else {
			rows.response = append(rows.response, *response)
		}
	}
	return &rows, nil
}

// processSetStatement is an internal function for checking whether query is a valid set statement
// and updating set statement map of the fireboltConnection
func processSetStatement(ctx context.Context, c *fireboltConnection, query string) (bool, error) {
	setKey, setValue, err := parseSetStatement(query)
	if err != nil {
		// if parsing of set statement returned an error, we will not handle the request as a set statement
		return false, nil
	}
	err = validateSetStatement(setKey)
	if err != nil {
		return false, err
	}

	// combine parameters from connection and set statement
	combinedParameters := make(map[string]string)
	for k, v := range c.parameters {
		combinedParameters[k] = v
	}
	combinedParameters[setKey] = setValue

	_, err = c.client.Query(ctx, c.engineUrl, "SELECT 1", combinedParameters, connectionControl{
		updateParameters: c.setParameter,
		setEngineURL:     c.setEngineURL,
		resetParameters:  c.resetParameters,
	})
	if err == nil {
		c.setParameter(setKey, setValue)
		return true, nil
	}
	return true, err
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
	ignoreParameters := append(getUseParametersList(), getDisallowedParametersList()...)
	if c.parameters != nil {
		for k := range c.parameters {
			if !contains(ignoreParameters, k) {
				delete(c.parameters, k)
			}
		}
	}
	if c.connector.cachedParameters != nil {
		for k := range c.connector.cachedParameters {
			if !contains(ignoreParameters, k) {
				delete(c.connector.cachedParameters, k)
			}
		}
	}
}
