package fireboltgosdk

import (
	"fmt"
	"log"
	"os"
	"strings"
)

func ConstructNestedError(message string, err error) error {
	log.Printf("%s: %v", message, err)
	return fmt.Errorf("%s: %v", message, err)
}

// parseSetStatement parses a single set statement and returns a key-value pair,
// or returns an error, if it isn't a set statement
func parseSetStatement(query string) (string, string, error) {
	query = strings.TrimSpace(query)
	if strings.HasPrefix(query, "SET") {
		query = strings.TrimSpace(query[len("SET"):])
		splitQuery := strings.Split(query, "=")
		if len(splitQuery) != 2 {
			return "", "", fmt.Errorf("not a valid set statement, found more then 1 '=' sign")
		}
		key := strings.TrimSpace(splitQuery[0])
		value := strings.TrimSpace(splitQuery[1])
		if key != "" && value != "" {
			return key, value, nil
		}
		return "", "", fmt.Errorf("Either key or value is empty")
	}
	return "", "", fmt.Errorf("Not a set statement")
}

// GetHostNameURL returns a hostname url, either default or overwritten with the environment variable
func GetHostNameURL() string {
	if val := os.Getenv("FIREBOLT_ENDPOINT"); val != "" {
		return makeCanonicalUrl(val)
	}
	return "https://api.app.firebolt.io"
}
