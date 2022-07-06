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

func TestGetAccountId(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	config, _ := ParseDSNString(dsn)
	client, err := Authenticate(config.username, config.password)
	if err != nil {
		t.Errorf("Authentication failed with: %s", err)
	}
	accountId, err := client.GetAccountIdByName("firebolt")
	if err != nil {
		t.Errorf("GetAccountIdByName failed with: %s", err)
	}
	if len(accountId) == 0 {
		t.Errorf("returned empty accountId")
	}

	_, err = client.GetAccountIdByName("firebolt_not_existing_account")
	if err == nil {
		t.Errorf("GetAccountIdByName didn't failed with not-existing account")
	}
}

func TestGetEngineUrlByName(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	config, _ := ParseDSNString(dsn)
	client, err := Authenticate(config.username, config.password)
	if err != nil {
		t.Errorf("Authentication failed with: %s", err)
	}

	accountId, err := client.GetAccountIdByName("firebolt")
	if err != nil {
		t.Errorf("GetAccountIdByName failed with: %s", err)
	}

	engineId, err := client.GetEngineIdByName(config.engineName, accountId)
	if err != nil {
		t.Errorf("GetEngineIdByName failed with: %s", err)
	}
	if len(engineId) == 0 {
		t.Errorf("GetEngineIdByName succeed but returned a zero length account id")
	}

	engineUrl, err := client.GetEngineUrlById(engineId, accountId)
	if err != nil {
		t.Errorf("GetEngineUrlById failed with: %s", err)
	}
	if len(engineUrl) == 0 {
		t.Errorf("GetEngineUrlById succeed but returned a zero length account id")
	}
}

func TestGetEngineUrlByDatabase(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	config, _ := ParseDSNString(dsn)
	client, err := Authenticate(config.username, config.password)
	if err != nil {
		t.Errorf("Authentication failed with: %s", err)
	}

	engineUrl, err := client.GetEngineUrlByDatabase(config.database, config.accountName)
	if err != nil {
		t.Errorf("GetEngineUrlByDatabase failed with: %s", err)
	}
	if len(engineUrl) == 0 {
		t.Errorf("GetEngineUrlById succeed but returned a zero length account id")
	}
}
