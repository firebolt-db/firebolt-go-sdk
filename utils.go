package fireboltgosdk

import (
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/matishsiao/goInfo"
	"github.com/xwb1989/sqlparser"
)

var goInfoFunc = goInfo.GetInfo

func getUseParametersList() []string {
	return []string{"database", "engine"}
}

func getDisallowedParametersList() []string {
	return []string{"output_format"}
}

func ConstructNestedError(message string, err error) error {
	infolog.Printf("%s: %v", message, err)
	return fmt.Errorf("%s: %v", message, err)
}

func validateSetStatement(key string) error {
	for _, denyKey := range getUseParametersList() {
		if key == denyKey {
			return fmt.Errorf("could not set parameter. "+
				"Set parameter '%s' is not allowed. "+
				"Try again with 'USE %s' instead of SET", key, strings.ToUpper(key))
		}
	}

	for _, denyKey := range getDisallowedParametersList() {
		if key == denyKey {
			return fmt.Errorf("could not set parameter. "+
				"Set parameter '%s' is not allowed. "+
				"Try again with a different parameter name", key)
		}
	}

	return nil
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
		if key != "" && value != "" {
			return key, value, nil
		}
		return "", "", fmt.Errorf("Either key or value is empty")
	}
	return "", "", fmt.Errorf("Not a set statement")
}

var infolog = log.New(os.Stderr, "[firebolt-go-sdk]", log.Ldate|log.Ltime|log.Lshortfile)

func init() {
	infolog.SetOutput(ioutil.Discard)
}

// prepareStatement parses a query and substitude question marks with params
func prepareStatement(query string, params []driver.NamedValue) (string, error) {
	r := sqlparser.NewStringTokenizer(query)
	var positions []int

	for {
		tokenId, _ := r.Scan()

		if tokenId == 0 {
			break
		}

		if tokenId == sqlparser.VALUE_ARG {
			positions = append(positions, r.Position-1)
		}
	}

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

// SplitStatements split multiple statements into a list of statements
func SplitStatements(sql string) ([]string, error) {
	var queries []string

	for sql != "" {
		var err error
		var query string

		query, sql, err = sqlparser.SplitStatement(sql)
		if err != nil {
			return nil, ConstructNestedError("error during splitting query", err)
		}
		if strings.Trim(query, " \t\n") == "" {
			continue
		}
		queries = append(queries, query)
	}

	return queries, nil
}

func formatValue(value driver.Value) (string, error) {
	switch v := value.(type) {
	case string:
		res := value.(string)
		res = strings.Replace(res, "\\", "\\\\", -1)
		res = strings.Replace(res, "'", "\\'", -1)
		return fmt.Sprintf("'%s'", res), nil
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
		return fmt.Sprintf("'%s'", timeValue.Format(layout)), nil
	case []byte:
		byteValue := value.([]byte)
		parts := make([]string, len(byteValue))
		for i, b := range byteValue {
			parts[i] = fmt.Sprintf("\\x%02x", b)
		}
		return fmt.Sprintf("E'%s'", strings.Join(parts, "")), nil
	case nil:
		return "NULL", nil
	default:
		return "", fmt.Errorf("not supported type: %v", v)
	}
}

// GetHostNameURL returns a hostname url, either default or overwritten with the environment variable
func GetHostNameURL() string {
	if val := os.Getenv("FIREBOLT_ENDPOINT"); val != "" {
		return makeCanonicalUrl(val)
	}
	return "https://api.app.firebolt.io"
}

// ConstructUserAgentString returns a string with go, GoSDK and os type and versions
// additionally user can set "FIREBOLT_GO_DRIVERS" and "FIREBOLT_GO_CLIENTS" env variable,
// and they will be concatenated with the final user-agent string
func ConstructUserAgentString() (ua_string string) {
	defer func() {
		// ConstructUserAgentString is a non-essential function, used for statistic gathering
		// so carry on working if a failure occurs
		if err := recover(); err != nil {
			infolog.Printf("Unable to generate User Agent string")
			ua_string = "GoSDK"
		}
	}()
	osNameVersion := runtime.GOOS
	if gi, err := goInfoFunc(); err == nil {
		osNameVersion += " " + gi.Core
	}

	var isStringAllowed = regexp.MustCompile(`^[\w\d._\-/ ]+$`).MatchString

	goDrivers := os.Getenv("FIREBOLT_GO_DRIVERS")
	if !isStringAllowed(goDrivers) {
		goDrivers = ""
	}
	goClients := os.Getenv("FIREBOLT_GO_CLIENTS")
	if !isStringAllowed(goClients) {
		goClients = ""
	}

	ua_string = strings.TrimSpace(fmt.Sprintf("%s GoSDK/%s (Go %s; %s) %s", goClients, sdkVersion, runtime.Version(), osNameVersion, goDrivers))
	return ua_string
}

func valueToNamedValue(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		namedValues = append(namedValues, driver.NamedValue{Ordinal: i, Value: arg})
	}
	return namedValues
}
