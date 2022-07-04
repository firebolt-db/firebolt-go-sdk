package fireboltgosdk

import (
	"testing"
)

func TestAuthHappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	config, _ := ParseDSNString(dsn)
	client, err := Authenticate(config.username, config.password)
	if err != nil {
		t.Errorf("Authentication failed with: %s", err)
	}
	if len(client.AccessToken) == 0 {
		t.Errorf("Token is not set properly")
	}
}
