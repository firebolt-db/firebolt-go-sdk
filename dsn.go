package fireboltgosdk

import (
	"errors"
	"fmt"
	"regexp"
)

type fireboltSettings struct {
	database     string
	accountName  string
	engine       string
	clientId     string
	clientSecret string
}

const dsnPattern = `^firebolt://(?:/(?P<database>\w+))?(?:\?(?P<parameters>\w+\=[^=&]+(?:\&\w+=[^=&]+)*))?$`
const paramsPattern = `(?P<key>\w+)=(?P<value>[^=&]+)`

// ParseDSNString parses a dsn in a format: firebolt://[/database][?param=value[&param=value]]
// Accepted parameters are: account_name, engine, client_id, client_secret
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

	// Set database if it's provided
	if len(dsnMatch[1]) > 0 {
		result.database = dsnMatch[1]
	}

	paramsStr := dsnMatch[2]
	if len(paramsStr) > 0 {
		paramsMatch := paramsExpr.FindAllStringSubmatch(paramsStr, -1)
		for _, m := range paramsMatch {
			switch m[1] {
			case "account_name":
				result.accountName = m[2]
			case "engine":
				result.engine = m[2]
			case "client_id":
				result.clientId = m[2]
			case "client_secret":
				result.clientSecret = m[2]
			default:
				return nil, fmt.Errorf("unknown parameter name %s", m[1])
			}
		}
	}

	return &result, nil
}
