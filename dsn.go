package fireboltgosdk

import "errors"

type fireboltSettings struct {
	username     string
	password     string
	hostname     string
	database     string
	engine_name  string
	account_name string
}

const (
	usernameState = iota
	passwordState
	databaseState
	engineState
	accountKeywordState
	accountKeywordValue
)

func ParseDSNString(dsn string) (fireboltSettings, error) {

	expected_prefix := "firebolt://"
	if dsn[:len(expected_prefix)] != expected_prefix {
		return fireboltSettings{}, errors.New("Wrong argument")
	}

	var result fireboltSettings
	var keyword string
	state := usernameState
	for i := len(expected_prefix); i < len(dsn); i++ {
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
				result.engine_name += string(char)
			}

		case state == accountKeywordState:
			if dsn[i] == '=' && keyword != "account_name" {
				return fireboltSettings{}, errors.New("Unknown argument")
			} else if dsn[i] == '=' {
				state = accountKeywordValue
				keyword = ""
			} else {
				keyword += string(dsn[i])
			}

		case state == accountKeywordValue:
			result.account_name += string(dsn[i])
		}
	}
	if state != accountKeywordValue && state != engineState && state != databaseState {
		return fireboltSettings{}, errors.New("Wrong argument")
	}
	return result, nil
}
