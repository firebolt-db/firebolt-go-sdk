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

const (
	usernameState = iota
	passwordState
	databaseState
	engineState
	accountKeywordState
	accountKeywordValue
)

func ParseDSNString(dsn string) (*fireboltSettings, error) {

	const expectedPrefix = "firebolt://"
	if !strings.HasPrefix(dsn, expectedPrefix) {
		return nil, fmt.Errorf("dsn missing '%s' prefix", expectedPrefix)
	}

	var result fireboltSettings
	var keyword string
	state := usernameState
	for i := len(expectedPrefix); i < len(dsn); i++ {
		isSpecialChar := true
		if dsn[i] == '\\' {
			i++
			isSpecialChar = false
		}
		char := dsn[i]

		switch {
		case state == usernameState:
			if char == ':' && isSpecialChar {
				state = passwordState
			} else {
				result.username += string(char)
			}

		case state == passwordState:
			if char == '@' && isSpecialChar {
				state = databaseState
			} else {
				result.password += string(char)
			}

		case state == databaseState:
			if char == '/' && isSpecialChar {
				state = engineState
			} else if char == '?' && isSpecialChar {
				state = accountKeywordState
			} else {
				result.database += string(char)
			}

		case state == engineState:
			if char == '?' {
				state = accountKeywordState
			} else {
				result.engineName += string(char)
			}

		case state == accountKeywordState:
			if dsn[i] == '=' && keyword != "account_name" {
				return nil, fmt.Errorf("dsn contains an unknown argument: '%s'", keyword)
			} else if dsn[i] == '=' {
				state = accountKeywordValue
				keyword = ""
			} else {
				keyword += string(dsn[i])
			}

		case state == accountKeywordValue:
			result.accountName += string(dsn[i])
		}
	}
	if state != accountKeywordValue && state != engineState && state != databaseState {
		return nil, fmt.Errorf("dsn is not complete")
	}
	return &result, nil
}
