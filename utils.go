package fireboltgosdk

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func ConstructNestedError(message string, err error) error {
	infolog.Printf("%s: %v", message, err)
	return fmt.Errorf("%s: %v", message, err)
}

// parseSetStatement parses a single set statement and returns a key-value pair,
// or returns an error, if it isn't a set statement
func parseSetStatement(query string) (string, string, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(strings.ToUpper(query), "SET") {
		query = strings.TrimSpace(query[len("SET"):])
		key, value, found := strings.Cut(query, "=")
		if !found {
			return "", "", fmt.Errorf("not a valid set statement, didn't find '=' sign")
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
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
