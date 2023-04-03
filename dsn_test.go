package fireboltgosdk

import (
	"testing"
)

func runDSNTest(t *testing.T, input string, expectedSettings fireboltSettings) {
	settings, err := ParseDSNString(input)

	if err != nil {
		t.Errorf("ParseDSNString unexpectedly failed: %v", err)
	}

	if settings.account_name != expectedSettings.account_name {
		t.Errorf("for account_name got %s want %s", settings.account_name, expectedSettings.account_name)
	}

	if settings.engine != expectedSettings.engine {
		t.Errorf("for engine got %s want %s", settings.engine, expectedSettings.engine)
	}

	if settings.clientId != expectedSettings.clientId {
		t.Errorf("for client_id got %s want %s", settings.clientId, expectedSettings.clientId)
	}

	if settings.clientSecret != expectedSettings.clientSecret {
		t.Errorf("for client_secret got %s want %s", settings.clientSecret, expectedSettings.clientSecret)
	}
}

func runDSNTestFail(t *testing.T, input string) {
	_, err := ParseDSNString(input)
	if err == nil {
		t.Errorf("expected to fail with %s, but didn't", input)
	}
}

func TestDSNHappyPath(t *testing.T) {
	runDSNTest(t, "firebolt://", fireboltSettings{})

	runDSNTest(t, "firebolt:///test_db", fireboltSettings{database: "test_db"})

	runDSNTest(t, "firebolt://?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		fireboltSettings{account_name: "test_acc", engine: "test_eng", clientId: "test_cid", clientSecret: "test_cs"})

	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		fireboltSettings{database: "test_db", account_name: "test_acc", engine: "test_eng", clientId: "test_cid", clientSecret: "test_cs"})
}

// TestDSNFailed test different failure scenarios for ParseDSNString
func TestDSNFailed(t *testing.T) {
	runDSNTestFail(t, "")
	runDSNTestFail(t, "other_db://")
	runDSNTestFail(t, "firebolt://db")      // another / is needed before a db
	runDSNTestFail(t, "firebolt://?k=v")    // unknown parameter name
	runDSNTestFail(t, "firebolt:///db?k=v") // unknown parameter name
}
