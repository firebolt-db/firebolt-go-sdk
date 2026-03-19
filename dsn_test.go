package fireboltgosdk

import (
	"net/url"
	"testing"
	"time"

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

	if settings.ClientSideLB != expectedSettings.ClientSideLB {
		t.Errorf("for ClientSideLB got %t want %t", settings.ClientSideLB, expectedSettings.ClientSideLB)
	}

	if settings.DNSTTL != expectedSettings.DNSTTL {
		t.Errorf("for DNSTTL got %v want %v", settings.DNSTTL, expectedSettings.DNSTTL)
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
	runDSNTest(t, "firebolt://", types.FireboltSettings{NewVersion: true, ClientSideLB: true})

	runDSNTest(t, "firebolt:///test_db", types.FireboltSettings{Database: "test_db", NewVersion: true, ClientSideLB: true})

	runDSNTest(t, "firebolt://?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		types.FireboltSettings{AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_cs", NewVersion: true, ClientSideLB: true})

	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs",
		types.FireboltSettings{Database: "test_db", AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_cs", NewVersion: true, ClientSideLB: true})

	// special characters
	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_*-()@\\.",
		types.FireboltSettings{Database: "test_db", AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_*-()@\\.", NewVersion: true, ClientSideLB: true})
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
	runDSNTest(t, "firebolt://?url=http", types.FireboltSettings{Url: "http", NewVersion: true, ClientSideLB: true})

	runDSNTest(t, "firebolt:///test_db?url=http", types.FireboltSettings{Database: "test_db", Url: "http", NewVersion: true, ClientSideLB: true})

	runDSNTest(t, "firebolt:///test_db?account_name=test_acc&engine=test_eng&client_id=test_cid&client_secret=test_cs&url=http",
		types.FireboltSettings{Database: "test_db", AccountName: "test_acc", EngineName: "test_eng", ClientID: "test_cid", ClientSecret: "test_cs", Url: "http", NewVersion: true, ClientSideLB: true})
	runDSNTest(t, "firebolt:///test_db?url=http://localhost:8080", types.FireboltSettings{Url: "http://localhost:8080", Database: "test_db", NewVersion: true, ClientSideLB: true})
	runDSNTest(t, "firebolt:///test_db?url=https://localhost:443", types.FireboltSettings{Url: "https://localhost:443", Database: "test_db", NewVersion: true, ClientSideLB: true})
}

func TestDSNCoreClientSideLB(t *testing.T) {
	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb=true",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: true})

	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb=false",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: false})

	// Omitting client_side_lb defaults to true.
	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: true})
}

func TestDSNCoreFailed(t *testing.T) {
	runDSNTestFail(t, "firebolt:///user:password?url=http")
	runDSNTestFail(t, "firebolt:///test_db?url=http&k=v")
}

func TestDSNCoreClientSideLBDNSTTL(t *testing.T) {
	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb_dns_ttl=10s",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: true, DNSTTL: 10 * time.Second})

	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb_dns_ttl=500ms",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: true, DNSTTL: 500 * time.Millisecond})

	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb_dns_ttl=2m",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: true, DNSTTL: 2 * time.Minute})

	// Omitting client_side_lb_dns_ttl leaves DNSTTL at zero (SDK uses its default 30s).
	runDSNTest(t, "firebolt:///test_db?url=http://my-svc:8080",
		types.FireboltSettings{Database: "test_db", Url: "http://my-svc:8080", NewVersion: true, ClientSideLB: true})
}

func TestDSNCoreClientSideLBDNSTTLInvalid(t *testing.T) {
	runDSNTestFail(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb_dns_ttl=bogus")
	runDSNTestFail(t, "firebolt:///test_db?url=http://my-svc:8080&client_side_lb_dns_ttl=")
}

func TestDSNWithDefaultParams(t *testing.T) {
	// Test with prefixed default_param.* parameters
	expectedParams := map[string]string{
		"pgfire_dbname": "account@db@engine",
		"advanced_mode": "true",
	}

	// Build DSN with prefixed params using url.Values for proper encoding
	dsn := "firebolt:///test_db?account_name=test_acc&engine=test_eng"
	values := url.Values{}
	for k, v := range expectedParams {
		values.Set("default_param."+k, v)
	}
	dsn += "&" + values.Encode()

	settings, err := ParseDSNString(dsn)
	if err != nil {
		t.Errorf("ParseDSNString failed: %v", err)
		return
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
	// Test with properly URL-encoded prefixed default_param.* parameters
	defaultParams := map[string]string{
		"pgfire_dbname": "account@db@engine",
		"advanced_mode": "true",
	}

	// Build DSN with prefixed params using url.Values for proper encoding
	dsn := "firebolt:///test_db?account_name=test_acc&engine=test_eng"
	values := url.Values{}
	for k, v := range defaultParams {
		values.Set("default_param."+k, v)
	}
	dsn += "&" + values.Encode()

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

func TestDSNWithDefaultParamsSpecialCharacters(t *testing.T) {
	// Test that special characters in default_param values are properly handled
	defaultParams := map[string]string{
		"application_name": "dbt",
		"query_timeout":    "60",
		"param_with_space": "value with spaces",
		"param_with_quote": "value\"with\"quotes",
		"param_with_at":    "account@db@engine",
	}

	// Build DSN with prefixed params using url.Values for proper encoding
	dsn := "firebolt:///test_db?account_name=test_acc"
	values := url.Values{}
	for k, v := range defaultParams {
		values.Set("default_param."+k, v)
	}
	dsn += "&" + values.Encode()

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
