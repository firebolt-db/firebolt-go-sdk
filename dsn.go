package fireboltgosdk

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/types"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const dsnPattern = `^firebolt://(?:/(?P<database>\w+))?(?:\?(?P<parameters>\w+\=[^=&]+(?:\&\w+=[^=&]+)*))?$`
const dsnPatternV0 = `^firebolt://(?P<username>.*@?.*):(?P<password>.*)@(?P<database>\w+)(?:/(?P<engine>[^?]+))?(?:\?(?P<parameters>\w+\=[^=&]+(?:\&\w+=[^=&]+)*))?$`
const paramsPattern = `(?P<key>\w+)=(?P<value>[^=&]+)`

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

	// Set database if it's provided
	if len(dsnMatch[1]) > 0 {
		result.Database = dsnMatch[1]
	}
	for _, m := range parseParams(dsnMatch[2]) {
		switch m[1] {
		case "account_name":
			result.AccountName = m[2]
		case "engine":
			result.EngineName = m[2]
		case "client_id":
			result.ClientID = m[2]
		case "client_secret":
			result.ClientSecret = m[2]
		default:
			return nil, fmt.Errorf("unknown parameter name %s", m[1])
		}
	}
	return &result, nil
}

func makeSettingsV0(dsnMatch []string) (*types.FireboltSettings, error) {
	var result types.FireboltSettings

	result.ClientID = dsnMatch[1]
	result.ClientSecret = dsnMatch[2]

	result.NewVersion = isServiceID(result.ClientID)

	result.Database = dsnMatch[3]
	if len(dsnMatch[4]) > 0 {
		// engine name was provided
		result.EngineName = dsnMatch[4]
	}

	for _, m := range parseParams(dsnMatch[5]) {
		switch m[1] {
		case "account_name":
			result.AccountName = m[2]
		default:
			return nil, fmt.Errorf("unknown parameter name %s", m[1])
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
