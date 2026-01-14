package fireboltgosdk

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

const mockUserName = "user@firebolt.io"

func runDSNTest(t *testing.T, input string, expectedSettings types.FireboltSettings) {
	settings, err := ParseDSNString(input)

	if err != nil {
		t.Errorf("ParseDSNString unexpectedly failed: %v", err)
	}

	if settings.AccountName != expectedSettings.AccountName {
		t.Errorf("for account_name got %s want %s", settings.AccountName, expectedSettings.AccountName)
	}

	if settings.EngineName != expectedSettings.EngineName {
		t.Errorf("for engine got %s want %s", settings.EngineName, expectedSettings.EngineName)
	}

	if settings.ClientID != expectedSettings.ClientID {
		t.Errorf("for client_id got %s want %s", settings.ClientID, expectedSettings.ClientID)
	}

	if settings.ClientSecret != expectedSettings.ClientSecret {
		t.Errorf("for client_secret got %s want %s", settings.ClientSecret, expectedSettings.ClientSecret)
	}

	if settings.Database != expectedSettings.Database {
		t.Errorf("for Database got %s want %s", settings.Database, expectedSettings.Database)
	}

	if settings.NewVersion != expectedSettings.NewVersion {
		t.Errorf("for NewVersion got %t want %t", settings.NewVersion, expectedSettings.NewVersion)
	}

	// Check DefaultQueryParams
	if len(settings.DefaultQueryParams) != len(expectedSettings.DefaultQueryParams) {
		t.Errorf("for DefaultQueryParams length got %d want %d", len(settings.DefaultQueryParams), len(expectedSettings.DefaultQueryParams))
	}
	for k, v := range expectedSettings.DefaultQueryParams {
		if settings.DefaultQueryParams[k] != v {
			t.Errorf("for DefaultQueryParams[%s] got %s want %s", k, settings.DefaultQueryParams[k], v)
		}
	}
}

func runDSNTestFail(t *testing.T, input string) {
	_, err := ParseDSNString(input)
	if err == nil {
		t.Errorf("expected to fail with %s, but didn't", input)
	}
}

func TestDSNHappyPath(t *testing.T) {
	runDSNTest(t, "firebolt://", types.FireboltSettings{NewVersion: true})

	runDSNTest(t, "firebolt:///test_db", types.FireboltSettings{Database: "test_db", NewVersion: true})

	runDSNTest(t, "firebolt://?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		types.FireboltSettings{AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_cs", NewVersion: true})

	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		types.FireboltSettings{Database: "test_db", AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_cs", NewVersion: true})

	// special characters
	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_*-()@\\.",
		types.FireboltSettings{Database: "test_db", AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_*-()@\\.", NewVersion: true})
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
		types.FireboltSettings{ClientID: mockUserName, ClientSecret: "password", Database: "db_name", NewVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_name",
		types.FireboltSettings{ClientID: mockUserName, ClientSecret: "password", Database: "db_name", EngineName: "engine_name", NewVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_name",
		types.FireboltSettings{ClientID: mockUserName, ClientSecret: "password", Database: "db_name", EngineName: "engine_name", NewVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/engine_url.firebolt.io",
		types.FireboltSettings{ClientID: mockUserName, ClientSecret: "password", Database: "db_name", EngineName: "engine_url.firebolt.io", NewVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name/https://engine_url.firebolt.io",
		types.FireboltSettings{ClientID: mockUserName, ClientSecret: "password", Database: "db_name", EngineName: "https://engine_url.firebolt.io", NewVersion: false})

	runDSNTest(t, "firebolt://user@firebolt.io:password@db_name?account_name=firebolt_account",
		types.FireboltSettings{ClientID: mockUserName, ClientSecret: "password", Database: "db_name", AccountName: "firebolt_account", NewVersion: false})

	runDSNTest(t, "firebolt://user@fire:bolt.io:passwo@rd@db_name?account_name=firebolt_account",
		types.FireboltSettings{ClientID: "user@fire:bolt.io", ClientSecret: "passwo@rd", Database: "db_name", AccountName: "firebolt_account", NewVersion: false})

	runDSNTest(t, "firebolt://client_id:client_secret@db_name?account_name=firebolt_account",
		types.FireboltSettings{ClientID: "client_id", ClientSecret: "client_secret", Database: "db_name", AccountName: "firebolt_account", NewVersion: true})
}

// TestDSNFailed test different failure scenarios for ParseDSNString
func TestDSNV0Failed(t *testing.T) {
	runDSNTestFail(t, "")
	runDSNTestFail(t, "firebolt://user:yury_db")
	runDSNTestFail(t, "jdbc://user:yury_db@db_name")
	runDSNTestFail(t, "firebolt://yury_db@dn_name?account_name=firebolt_account")
}

func TestDSNCoreHappyPath(t *testing.T) {
	runDSNTest(t, "firebolt://?url=http", types.FireboltSettings{Url: "http", NewVersion: true})

	runDSNTest(t, "firebolt:///test_db?url=http", types.FireboltSettings{Database: "test_db", Url: "http", NewVersion: true})

	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs&url=http",
		types.FireboltSettings{Database: "test_db", AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_cs", Url: "http", NewVersion: true})
	runDSNTest(t, "firebolt:///test_db?url=http://localhost:8080", types.FireboltSettings{Url: "http://localhost:8080", Database: "test_db", NewVersion: true})
	runDSNTest(t, "firebolt:///test_db?url=https://localhost:443", types.FireboltSettings{Url: "https://localhost:443", Database: "test_db", NewVersion: true})
}

func TestDSNCoreFailed(t *testing.T) {
	runDSNTestFail(t, "firebolt:///user:password?url=http")
	runDSNTestFail(t, "firebolt:///test_db?url=http&k=v")
}

func TestDSNWithDefaultParams(t *testing.T) {
	// Test with URL-encoded JSON defaultParams
	defaultParamsJSON := `{"pgfire_dbname":"account@db@engine","advanced_mode":"true"}`
	encodedParams := "defaultParams=" + defaultParamsJSON

	settings, err := ParseDSNString("firebolt:///test_db?account_name=test_acc&engine=test_eng&" + encodedParams)
	if err != nil {
		t.Errorf("ParseDSNString failed: %v", err)
		return
	}

	expectedParams := map[string]string{
		"pgfire_dbname": "account@db@engine",
		"advanced_mode": "true",
	}

	if len(settings.DefaultQueryParams) != len(expectedParams) {
		t.Errorf("DefaultQueryParams length got %d want %d", len(settings.DefaultQueryParams), len(expectedParams))
	}
	for k, v := range expectedParams {
		if settings.DefaultQueryParams[k] != v {
			t.Errorf("DefaultQueryParams[%s] got %s want %s", k, settings.DefaultQueryParams[k], v)
		}
	}
}

func TestDSNWithDefaultParamsURLEncoded(t *testing.T) {
	// Test with properly URL-encoded JSON defaultParams
	defaultParams := map[string]string{
		"pgfire_dbname": "account@db@engine",
		"advanced_mode": "true",
	}
	defaultParamsJSON, _ := json.Marshal(defaultParams)
	encodedParams := url.QueryEscape(string(defaultParamsJSON))

	dsn := "firebolt:///test_db?account_name=test_acc&engine=test_eng&defaultParams=" + encodedParams
	settings, err := ParseDSNString(dsn)
	if err != nil {
		t.Errorf("ParseDSNString failed: %v", err)
		return
	}

	if len(settings.DefaultQueryParams) != len(defaultParams) {
		t.Errorf("DefaultQueryParams length got %d want %d", len(settings.DefaultQueryParams), len(defaultParams))
	}
	for k, v := range defaultParams {
		if settings.DefaultQueryParams[k] != v {
			t.Errorf("DefaultQueryParams[%s] got %s want %s", k, settings.DefaultQueryParams[k], v)
		}
	}
}

func TestDSNWithInvalidDefaultParams(t *testing.T) {
	// Test with invalid JSON in defaultParams
	runDSNTestFail(t, "firebolt:///test_db?defaultParams=invalid_json")

	// Test with invalid URL encoding
	runDSNTestFail(t, "firebolt:///test_db?defaultParams=%")
}
