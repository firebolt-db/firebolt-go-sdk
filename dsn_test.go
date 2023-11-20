package fireboltgosdk

import "testing"

func runDSNTest(t *testing.T, input string, expectedSettings fireboltSettings) {
	settings, err := ParseDSNString(input)

	if err != nil {
		t.Errorf("ParseDSNString unexpectedly failed: %v", err)
	}

	if settings.accountName != expectedSettings.accountName {
		t.Errorf("for account_name got %s want %s", settings.accountName, expectedSettings.accountName)
	}

	if settings.engineName != expectedSettings.engineName {
		t.Errorf("for engine got %s want %s", settings.engineName, expectedSettings.engineName)
	}

	if settings.clientID != expectedSettings.clientID {
		t.Errorf("for client_id got %s want %s", settings.clientID, expectedSettings.clientID)
	}

	if settings.clientSecret != expectedSettings.clientSecret {
		t.Errorf("for client_secret got %s want %s", settings.clientSecret, expectedSettings.clientSecret)
	}

	if settings.database != expectedSettings.database {
		t.Errorf("for database got %s want %s", settings.database, expectedSettings.database)
	}

	if settings.newVersion != expectedSettings.newVersion {
		t.Errorf("for newVersion got %t want %t", settings.newVersion, expectedSettings.newVersion)
	}
}

func runDSNTestFail(t *testing.T, input string) {
	_, err := ParseDSNString(input)
	if err == nil {
		t.Errorf("expected to fail with %s, but didn't", input)
	}
}

func TestDSNHappyPath(t *testing.T) {
	runDSNTest(t, "firebolt://", fireboltSettings{newVersion: true})

	runDSNTest(t, "firebolt:///test_db", fireboltSettings{database: "test_db", newVersion: true})

	runDSNTest(t, "firebolt://?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		fireboltSettings{accountName: "test_acc", engineName: "test_eng", clientID: "test_cid", clientSecret: "test_cs", newVersion: true})

	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		fireboltSettings{database: "test_db", accountName: "test_acc", engineName: "test_eng", clientID: "test_cid", clientSecret: "test_cs", newVersion: true})

	// special characters
	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_*-()@\\.",
		fireboltSettings{database: "test_db", accountName: "test_acc", engineName: "test_eng", clientID: "test_cid", clientSecret: "test_*-()@\\.", newVersion: true})
}

// TestDSNFailed test different failure scenarios for ParseDSNString
func TestDSNFailed(t *testing.T) {
	runDSNTestFail(t, "")
	runDSNTestFail(t, "other_db://")
	runDSNTestFail(t, "firebolt://db")      // another / is needed before a db
	runDSNTestFail(t, "firebolt://?k=v")    // unknown parameter name
	runDSNTestFail(t, "firebolt:///db?k=v") // unknown parameter name
}

func TestDSNV0HappyPath(t *testing.T) {
	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name",
		fireboltSettings{clientID: "user@firebolt.io", clientSecret: "password", database: "db_name", newVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_name",
		fireboltSettings{clientID: "user@firebolt.io", clientSecret: "password", database: "db_name", engineName: "engine_name", newVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_name",
		fireboltSettings{clientID: "user@firebolt.io", clientSecret: "password", database: "db_name", engineName: "engine_name", newVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_url.firebolt.io",
		fireboltSettings{clientID: "user@firebolt.io", clientSecret: "password", database: "db_name", engineName: "engine_url.firebolt.io", newVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/https://engine_url.firebolt.io",
		fireboltSettings{clientID: "user@firebolt.io", clientSecret: "password", database: "db_name", engineName: "https://engine_url.firebolt.io", newVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name?account_name=firebolt_account",
		fireboltSettings{clientID: "user@firebolt.io", clientSecret: "password", database: "db_name", accountName: "firebolt_account", newVersion: false})

	runDSNTest(t, "firebolt://user@fire:bolt.io:passwo@rd@db_name?account_name=firebolt_account",
		fireboltSettings{clientID: "user@fire:bolt.io", clientSecret: "passwo@rd", database: "db_name", accountName: "firebolt_account", newVersion: false})

	runDSNTest(t, "firebolt://client_id:client_secret@db_name?account_name=firebolt_account",
		fireboltSettings{clientID: "client_id", clientSecret: "client_secret", database: "db_name", accountName: "firebolt_account", newVersion: false})
}

// TestDSNFailed test different failure scenarios for ParseDSNString
func TestDSNV0Failed(t *testing.T) {
	runDSNTestFail(t, "")
	runDSNTestFail(t, "firebolt://user:yury_db")
	runDSNTestFail(t, "jdbc://user:yury_db@db_name")
	runDSNTestFail(t, "firebolt://yury_db@dn_name?account_name=firebolt_account")
}
