package fireboltgosdk

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/types"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const dsnPattern = `^firebolt://(?:/(?P<database>\w+))?(?:\?(?P<parameters>[\w\.]+=[^=&]+(?:\&[\w\.]+=[^=&]+)*))?$`
const dsnPatternV0 = `^firebolt://(?P<username>.*@?.*):(?P<password>.*)@(?P<database>\w+)(?:/(?P<engine>[^?]+))?(?:\?(?P<parameters>[\w\.]+=[^=&]+(?:\&[\w\.]+=[^=&]+)*))?$`
const paramsPattern = `(?P<key>[\w\.]+)=(?P<value>[^=&]+)`

// ParseDSNString parses a dsn in a format: firebolt://username:password@db_name[/engine_name][?account_name=organization]
// returns a settings object where all parsed values are populated
// returns an error if required fields couldn't be parsed or if after parsing some characters were left unparsed
func ParseDSNString(dsn string) (*types.FireboltSettings, error) {
	dsnExpr := regexp.MustCompile(dsnPattern)
	dsnExprV0 := regexp.MustCompile(dsnPatternV0)

	logging.Infolog.Println("Parsing DSN")

	if dsnMatch := dsnExpr.FindStringSubmatch(dsn); len(dsnMatch) > 0 {
		return makeSettings(dsnMatch)
	} else if dsnMatch := dsnExprV0.FindStringSubmatch(dsn); len(dsnMatch) > 0 {
		return makeSettingsV0(dsnMatch)
	} else {
		return nil, errors.New("invalid connection string format")
	}
}

func makeSettings(dsnMatch []string) (*types.FireboltSettings, error) {
	var result types.FireboltSettings
	result.NewVersion = true
	result.DefaultQueryParams = make(map[string]string)

	// Set database if it's provided
	if len(dsnMatch[1]) > 0 {
		result.Database = dsnMatch[1]
	}
	for _, m := range parseParams(dsnMatch[2]) {
		key := m[1]
		value := m[2]

		// Decode URL-encoded value
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to URL decode parameter %s: %w", key, err)
		}

		// Check if this is a default_param.* prefixed parameter
		if strings.HasPrefix(key, "default_param.") {
			// Strip the prefix and add to DefaultQueryParams
			paramKey := strings.TrimPrefix(key, "default_param.")
			result.DefaultQueryParams[paramKey] = decodedValue
			continue
		}

		// Handle regular parameters
		switch key {
		case "account_name":
			result.AccountName = decodedValue
		case "engine":
			result.EngineName = decodedValue
		case "client_id":
			result.ClientID = decodedValue
		case "client_secret":
			result.ClientSecret = decodedValue
		case "url":
			result.Url = decodedValue
		default:
			return nil, fmt.Errorf("unknown parameter name %s", key)
		}
	}
	return &result, nil
}

func makeSettingsV0(dsnMatch []string) (*types.FireboltSettings, error) {
	var result types.FireboltSettings
	result.DefaultQueryParams = make(map[string]string)

	result.ClientID = dsnMatch[1]
	result.ClientSecret = dsnMatch[2]

	result.NewVersion = isServiceID(result.ClientID)

	result.Database = dsnMatch[3]
	if len(dsnMatch[4]) > 0 {
		// engine name was provided
		result.EngineName = dsnMatch[4]
	}

	for _, m := range parseParams(dsnMatch[5]) {
		key := m[1]
		value := m[2]

		// Decode URL-encoded value
		decodedValue, err := url.QueryUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("failed to URL decode parameter %s: %w", key, err)
		}

		// Check if this is a default_param.* prefixed parameter
		if strings.HasPrefix(key, "default_param.") {
			// Strip the prefix and add to DefaultQueryParams
			paramKey := strings.TrimPrefix(key, "default_param.")
			result.DefaultQueryParams[paramKey] = decodedValue
			continue
		}

		// Handle regular parameters
		switch key {
		case "account_name":
			result.AccountName = decodedValue
		default:
			return nil, fmt.Errorf("unknown parameter name %s", key)
		}
	}

	return &result, nil
}

func parseParams(paramsStr string) [][]string {
	if len(paramsStr) == 0 {
		return make([][]string, 0)
	}
	paramsExpr := regexp.MustCompile(paramsPattern)
	return paramsExpr.FindAllStringSubmatch(paramsStr, -1)
}

func isServiceID(username string) bool {
	return !strings.Contains(username, "@")
}
