package fireboltgosdk

import (
	"fmt"
	"os"
	"testing"
)

func setup() (string, error) {

	username := os.Getenv("USER_NAME")
	password := os.Getenv("PASSWORD")
	database := os.Getenv("DATABASE_NAME")

	dsn := fmt.Sprintf("firebolt://%s:%s@%s", username, password, database)
	return dsn, nil
}

func TestAuthHappyPath(t *testing.T) {
	dsn, _ := setup()
	config, _ := ParseDSNString(dsn)
	client, err := Authenticate(config.username, config.password)
	if err != nil {
		t.Errorf("Authentication failed with: %s", err)
	}
	if len(client.AccessToken) == 0 {
		t.Errorf("Token is not set properly")
	}
}
