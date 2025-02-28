package client

import "os"

// GetHostNameURL returns a hostname url, either default or overwritten with the environment variable
func GetHostNameURL() string {
	if val := os.Getenv("FIREBOLT_ENDPOINT"); val != "" {
		return MakeCanonicalUrl(val)
	}
	return "https://api.app.firebolt.io"
}
