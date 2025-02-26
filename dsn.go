package fireboltgosdk

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

type fireboltSettings struct {
	clientID     string
	clientSecret string
	database     string
	engineName   string
	accountName  string
	newVersion   bool
}

const dsnPattern = `^firebolt://(?:/(?P<database>\w+))?(?:\?(?P<parameters>\w+\=[^=&]+(?:\&\w+=[^=&]+)*))?$`
const dsnPatternV0 = `^firebolt://(?P<username>.*@?.*):(?P<password>.*)@(?P<database>\w+)(?:/(?P<engine>[^?]+))?(?:\?(?P<parameters>\w+\=[^=&]+(?:\&\w+=[^=&]+)*))?$`
const paramsPattern = `(?P<key>\w+)=(?P<value>[^=&]+)`

// ParseDSNString parses a dsn in a format: firebolt://username:password@db_name[/engine_name][?account_name=organization]
// returns a settings object where all parsed values are populated
// returns an error if required fields couldn't be parsed or if after parsing some characters were left unparsed
func ParseDSNString(dsn string) (*fireboltSettings, error) {
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

func makeSettings(dsnMatch []string) (*fireboltSettings, error) {
	var result fireboltSettings
	result.newVersion = true

	// Set database if it's provided
	if len(dsnMatch[1]) > 0 {
		result.database = dsnMatch[1]
	}
	for _, m := range parseParams(dsnMatch[2]) {
		switch m[1] {
		case "account_name":
			result.accountName = m[2]
		case "engine":
			result.engineName = m[2]
		case "client_id":
			result.clientID = m[2]
		case "client_secret":
			result.clientSecret = m[2]
		default:
			return nil, fmt.Errorf("unknown parameter name %s", m[1])
		}
	}
	return &result, nil
}

func makeSettingsV0(dsnMatch []string) (*fireboltSettings, error) {
	var result fireboltSettings

	result.clientID = dsnMatch[1]
	result.clientSecret = dsnMatch[2]

	result.newVersion = isServiceID(result.clientID)

	result.database = dsnMatch[3]
	if len(dsnMatch[4]) > 0 {
		// engine name was provided
		result.engineName = dsnMatch[4]
	}

	for _, m := range parseParams(dsnMatch[5]) {
		switch m[1] {
		case "account_name":
			result.accountName = m[2]
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
