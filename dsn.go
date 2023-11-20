package fireboltgosdk

import (
	"errors"
	"fmt"
	"regexp"
)

type fireboltSettings struct {
	username    string
	password    string
	database    string
	engineName  string
	accountName string
}

const dsnPattern = `^firebolt://(?P<username>.*@?.*):(?P<password>.*)@(?P<database>\w+)(?:/(?P<engine>[^?]+))?(?:\?(?P<parameters>\w+\=[^=&]+(?:\&\w+=[^=&]+)*))?$`
const paramsPattern = `(?P<key>\w+)=(?P<value>[^=&]+)`

// ParseDSNString parses a dsn in a format: firebolt://username:password@db_name[/engine_name][?account_name=organization]
// returns a settings object where all parsed values are populated
// returns an error if required fields couldn't be parsed or if after parsing some characters were left unparsed
func ParseDSNString(dsn string) (*fireboltSettings, error) {
	var result fireboltSettings
	dsnExpr := regexp.MustCompile(dsnPattern)
	paramsExpr := regexp.MustCompile(paramsPattern)

	infolog.Println("Parsing DSN")
	dsnMatch := dsnExpr.FindStringSubmatch(dsn)

	if len(dsnMatch) == 0 {
		return nil, errors.New("invalid connection string format")
	}

	result.username = dsnMatch[1]
	result.password = dsnMatch[2]
	result.database = dsnMatch[3]
	if len(dsnMatch[4]) > 0 {
		// engine name was provided
		result.engineName = dsnMatch[4]
	}

	paramsStr := dsnMatch[5]
	if len(paramsStr) > 0 {
		paramsMatch := paramsExpr.FindAllStringSubmatch(paramsStr, -1)
		for _, m := range paramsMatch {
			switch m[1] {
			case "account_name":
				result.accountName = m[2]
			default:
				return nil, fmt.Errorf("unknown parameter name %s", m[1])
			}
		}
	}

	return &result, nil
}
