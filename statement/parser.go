package statement

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	"github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/xwb1989/sqlparser"
)

type PreparedQuery interface {
	GetNumParams() int
	Format(args []driver.NamedValue) (string, map[string]string, error)
	OnSuccess(control client.ConnectionControl)
}

type SingleStatement struct {
	query           string
	paramsPositions []int
	parametersStyle context.PreparedStatementsStyle
}

func (s *SingleStatement) GetNumParams() int {
	if s.parametersStyle == context.PreparedStatementsStyleFbNumeric {
		// We don't know the number of parameters in the query
		return -1
	}
	return len(s.paramsPositions)
}

func makeQueryParameters(args []driver.NamedValue) (map[string]string, error) {
	if len(args) == 0 {
		return nil, nil
	}
	queryParameters := make([]map[string]*string, len(args))
	for i, arg := range args {
		var key string
		if arg.Name != "" {
			return nil, fmt.Errorf("named parameters are not supported for server-side prepared statements: %s", arg.Name)
		}
		key = fmt.Sprintf("$%d", arg.Ordinal)
		value, err := formatValueServerSide(arg.Value)
		if err != nil {
			return nil, err
		}
		queryParameters[i] = map[string]*string{"name": &key, "value": value}
	}
	// Encode the query parameters to JSON
	queryParametersJSON, err := json.Marshal(queryParameters)
	return map[string]string{"query_parameters": string(queryParametersJSON)}, err
}

func (s *SingleStatement) Format(args []driver.NamedValue) (string, map[string]string, error) {
	if s.parametersStyle == context.PreparedStatementsStyleFbNumeric {
		// Don't replace the parameters in the query, send them as `query_parameters`
		queryParameters, err := makeQueryParameters(args)
		return s.query, queryParameters, err
	}
	query, err := formatStatement(s.query, s.paramsPositions, args)
	return query, map[string]string{}, err

}

func (s *SingleStatement) OnSuccess(control client.ConnectionControl) {}

type SetStatement struct {
	key   string
	value string
}

func (s *SetStatement) GetNumParams() int {
	return -1
}

func (s *SetStatement) Format(args []driver.NamedValue) (string, map[string]string, error) {
	if len(args) != 0 {
		return "", map[string]string{}, fmt.Errorf("parameters are not supported in SET statements")
	}
	return "", map[string]string{s.key: s.value}, nil
}

func (s *SetStatement) OnSuccess(control client.ConnectionControl) {
	// Set parameters in the connection
	control.UpdateParameters(s.key, s.value)
}

// prepareQuery parses a query and returns a PreparedQuery object
func prepareQuery(query string, style context.PreparedStatementsStyle) ([]PreparedQuery, error) {
	queries, err := splitStatements(query)
	if err != nil {
		return nil, errors.ConstructNestedError("error during splitting query", err)
	}
	preparedQueries := make([]PreparedQuery, len(queries))
	for i, singleQuery := range queries {
		if strings.HasPrefix(strings.ToUpper(singleQuery), "SET") {
			key, value, err := parseSetStatement(singleQuery)
			if err != nil {
				return nil, err
			}
			preparedQueries[i] = &SetStatement{key, value}
		} else {
			var positions []int
			if style == context.PreparedStatementsStyleNative {
				positions, err = prepareStatement(singleQuery)
				if err != nil {
					return nil, err
				}
			} else {
				positions = nil
			}
			preparedQueries[i] = &SingleStatement{singleQuery, positions, style}
		}
	}

	return preparedQueries, nil
}

// parseSetStatement parses a single set statement and returns a key-value pair,
// or returns an error, if it isn't a set statement
func parseSetStatement(query string) (string, string, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(strings.ToUpper(query), "SET") {
		query = strings.TrimSpace(query[len("SET"):])
		values := strings.Split(query, "=")
		if len(values) < 2 {
			return "", "", fmt.Errorf("not a valid set statement, didn't find '=' sign")
		}
		key := strings.TrimSpace(values[0])
		value := strings.TrimSpace(values[1])

		if err := validateSetStatement(key); err != nil {
			return "", "", err
		}

		if key != "" && value != "" {
			return key, value, nil
		}
		return "", "", fmt.Errorf("either key or value is empty")
	}
	return "", "", fmt.Errorf("not a set statement")
}

// prepareStatement parses a query and finds all the positions of the value arguments
func prepareStatement(query string) ([]int, error) {
	r := sqlparser.NewStringTokenizer(query)
	var positions []int

	for {
		tokenId, _ := r.Scan()

		if r.LastError != nil {
			return []int{}, errors.ConstructNestedError("error during parsing query", r.LastError)
		}

		if tokenId == 0 {
			break
		}

		if tokenId == sqlparser.VALUE_ARG {
			positions = append(positions, r.Position-1)
		}
	}

	return positions, nil
}

// formatStatement replaces the value arguments in the query with the actual values
func formatStatement(query string, positions []int, params []driver.NamedValue) (string, error) {
	if len(positions) != len(params) {
		return "", fmt.Errorf("found '%d' value args in query, but '%d' arguments are provided", len(positions), len(params))
	}

	for i := len(positions) - 1; i >= 0; i -= 1 {
		res, err := formatValue(params[i].Value)
		if err != nil {
			return "", err
		}
		query = query[:positions[i]-1] + res + query[positions[i]:]
	}

	return query, nil
}

// splitStatements split multiple statements into a list of statements
func splitStatements(sql string) ([]string, error) {
	var queries []string

	for sql != "" {
		var err error
		var query string

		query, sql, err = sqlparser.SplitStatement(sql)
		if err != nil {
			return nil, errors.ConstructNestedError("error during splitting query", err)
		}
		if strings.Trim(query, " \t\n") == "" {
			continue
		}
		queries = append(queries, strings.Trim(query, " \t\n"))
	}

	if len(queries) == 0 {
		// Parser stripped all the symbols and found no meaningfully query
		// Consider it as empty query
		return []string{""}, nil
	}

	return queries, nil
}

func formatValueServerSide(value driver.Value) (*string, error) {
	// Server-side prepared statements don't support parameters
	// So we need to convert all the values to strings
	res, err := internalFormatValue(value, true)
	if res == "NULL" {
		return nil, err
	}
	return &res, err
}

func formatValue(value driver.Value) (string, error) {
	return internalFormatValue(value, false)
}

func internalFormatValue(value driver.Value, isServerSide bool) (string, error) {
	quote := func(s string) string {
		if isServerSide {
			return s
		}
		return "'" + s + "'"
	}

	switch v := value.(type) {
	case string:
		res := value.(string)
		res = strings.ReplaceAll(res, "\\", "\\\\")
		res = strings.ReplaceAll(res, "'", "\\'")
		return quote(res), nil
	case int64, uint64, int32, uint32, int16, uint16, int8, uint8, int, uint:
		return fmt.Sprintf("%d", value), nil
	case float64, float32:
		return fmt.Sprintf("%g", value), nil
	case bool:
		if value.(bool) {
			return "true", nil
		} else {
			return "false", nil
		}
	case time.Time:
		timeValue := value.(time.Time)
		layout := "2006-01-02 15:04:05.000000"
		// Subtract date part from value and check if remaining time part is zero
		// If it is, use date only format
		if timeValue.Sub(timeValue.Truncate(time.Hour*24)) == 0 {
			layout = "2006-01-02"
		} else if _, offset := timeValue.Zone(); offset != 0 {
			// If we have a timezone info, add it to format
			layout = "2006-01-02 15:04:05.000000-07:00"
		}
		return quote(timeValue.Format(layout)), nil
	case []byte:
		byteValue := value.([]byte)
		parts := make([]string, len(byteValue))
		if isServerSide {
			for i, b := range byteValue {
				parts[i] = fmt.Sprintf("%02x", b)
			}
			return fmt.Sprintf("\\x%s", strings.Join(parts, "")), nil
		} else {
			for i, b := range byteValue {
				parts[i] = fmt.Sprintf("\\x%02x", b)
			}
			return fmt.Sprintf("E'%s'", strings.Join(parts, "")), nil
		}
	case nil:
		return "NULL", nil
	default:
		return "", fmt.Errorf("not supported type: %v", v)
	}
}

func GetUseParametersList() []string {
	return []string{"database", "engine"}
}

func GetDisallowedParametersList() []string {
	return []string{"output_format"}
}

func validateSetStatement(key string) error {
	for _, denyKey := range GetUseParametersList() {
		if key == denyKey {
			return fmt.Errorf("could not set parameter. "+
				"Set parameter '%s' is not allowed. "+
				"Try again with 'USE %s' instead of SET", key, strings.ToUpper(key))
		}
	}

	for _, denyKey := range GetDisallowedParametersList() {
		if key == denyKey {
			return fmt.Errorf("could not set parameter. "+
				"Set parameter '%s' is not allowed. "+
				"Try again with a different parameter name", key)
		}
	}

	return nil
}
