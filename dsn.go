package fireboltgosdk

import (
	"fmt"
	"strings"
)

type fireboltSettings struct {
	username    string
	password    string
	database    string
	engineName  string
	accountName string
}

// splits string into two string, when first split char is encountered
// return two strings:
//    - first string before the encountered character, with removed backtick escape
//    - second string is remaining string, including the character on which it was split
func splitString(str string, splitChars []uint8) (string, string) {
	var res string
	for i := 0; i < len(str); i++ {
		if str[i] == '\\' {
			res += string(str[i+1])
			i += 1
			continue
		}
		for _, v := range splitChars {
			if str[i] == v {
				return res, str[i:]
			}
		}
		res += string(str[i])
	}
	return res, ""
}

func parseRemainingDSN(dsn string, expectedPrefix string, stopChars []uint8) (string, string, error) {
	if !strings.HasPrefix(dsn, expectedPrefix) {
		return "", dsn, fmt.Errorf("expected prefix not found: %s", expectedPrefix)
	}

	dsn = dsn[len(expectedPrefix):]
	res, dsn := splitString(dsn, stopChars)
	return res, dsn, nil
}

func ParseDSNString(dsn string) (*fireboltSettings, error) {

	var result fireboltSettings
	var err error

	result.username, dsn, err = parseRemainingDSN(dsn, "firebolt://", []uint8{':'})
	if err != nil {
		return nil, err
	}

	result.password, dsn, err = parseRemainingDSN(dsn, ":", []uint8{'@'})
	if err != nil {
		return nil, err
	}

	result.database, dsn, err = parseRemainingDSN(dsn, "@", []uint8{'/', '?'})
	if err != nil {
		return nil, err
	}

	result.engineName, dsn, _ = parseRemainingDSN(dsn, "/", []uint8{'?'})
	result.accountName, dsn, _ = parseRemainingDSN(dsn, "?account_name", []uint8{})

	if len(dsn) != 0 {
		return nil, fmt.Errorf("unparsed characters were found: %s", dsn)
	}

	return &result, nil
}
