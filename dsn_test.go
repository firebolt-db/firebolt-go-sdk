package fireboltgosdk

import (
	"testing"
)

func runDSNTest(t *testing.T, input string, expectedSettings fireboltSettings) {
	settings, err := ParseDSNString(input)

	if err != nil {
		t.Errorf("Unexpected failure for %s: %s", input, err)
	}

	if settings.username != expectedSettings.username {
		t.Errorf("got %s want %s", settings.username, expectedSettings.username)
	}

	if settings.password != expectedSettings.password {
		t.Errorf("got %s want %s", settings.password, expectedSettings.password)
	}

	if settings.database != expectedSettings.database {
		t.Errorf("got %s want %s", settings.database, expectedSettings.database)
	}

	if settings.engineName != expectedSettings.engineName {
		t.Errorf("got %s want %s", settings.engineName, expectedSettings.engineName)
	}
}

func runDSNTestFail(t *testing.T, input string) {
	_, err := ParseDSNString(input)
	if err == nil {
		t.Errorf("expected to fail with %s, but didn't", input)
	}
}

func TestDSNHappyPath(t *testing.T) {
	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name",
		fireboltSettings{username: "user@firebolt.io", password: "password", database: "db_name"})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_name",
		fireboltSettings{username: "user@firebolt.io", password: "password", database: "db_name", engineName: "engine_name"})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_name",
		fireboltSettings{username: "user@firebolt.io", password: "password", database: "db_name", engineName: "engine_name"})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_url.firebolt.io",
		fireboltSettings{username: "user@firebolt.io", password: "password", database: "db_name", engineName: "engine_url.firebolt.io"})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/https://engine_url.firebolt.io",
		fireboltSettings{username: "user@firebolt.io", password: "password", database: "db_name", engineName: "https://engine_url.firebolt.io"})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name?account_name=firebolt_account",
		fireboltSettings{username: "user@firebolt.io", password: "password", database: "db_name", accountName: "firebolt_account"})

	runDSNTest(t, "firebolt://user@fire:bolt.io:passwo@rd@db_name?account_name=firebolt_account",
		fireboltSettings{username: "user@fire:bolt.io", password: "passwo@rd", database: "db_name", accountName: "firebolt_account"})

	runDSNTest(t, "firebolt://client_id:client_secret@db_name?account_name=firebolt_account",
		fireboltSettings{username: "client_id", password: "client_secret", database: "db_name", accountName: "firebolt_account"})
}

// TestDSNFailed test different failure scenarios for ParseDSNString
func TestDSNFailed(t *testing.T) {
	runDSNTestFail(t, "")
	runDSNTestFail(t, "firebolt://")
	runDSNTestFail(t, "firebolt://user:yury_db")
	runDSNTestFail(t, "jdbc://user:yury_db@db_name")
	runDSNTestFail(t, "firebolt://yury_db@dn_name?account_name=firebolt_account")
}
