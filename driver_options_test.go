package fireboltgosdk

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/client"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestDriverOptions(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"
	accountID := "account-id"
	token := "1234567890"
	userAgent := "UA Test"

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithClientParams(accountID, token, userAgent),
	)

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	cl, ok := conn.client.(*client.ClientImpl)
	utils.AssertEqual(ok, true, t, "client is not *ClientImpl")

	connectionAccountID := conn.cachedParameters["account_id"]
	utils.AssertEqual(connectionAccountID, accountID, t, "accountID is invalid")
	utils.AssertEqual(cl.UserAgent, userAgent, t, "userAgent is invalid")

	tok, err := cl.AccessTokenGetter()
	if err != nil {
		t.Errorf("token getter returned an error: %v", err)
	}

	utils.AssertEqual(tok, token, t, "token getter returned wrong token")
}

func TestDriverOptionsSeparateClientParams(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"
	accountID := "account-id"
	token := "1234567890"
	userAgent := "UA Test"

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithAccountID(accountID),
		WithToken(token),
		WithUserAgent(userAgent),
	)

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	cl, ok := conn.client.(*client.ClientImpl)
	utils.AssertEqual(ok, true, t, "client is not *ClientImpl")

	connectionAccountID := conn.cachedParameters["account_id"]
	utils.AssertEqual(connectionAccountID, accountID, t, "accountID is invalid")
	utils.AssertEqual(cl.UserAgent, userAgent, t, "userAgent is invalid")

	tok, err := cl.AccessTokenGetter()
	if err != nil {
		t.Errorf("token getter returned an error: %v", err)
	}

	utils.AssertEqual(tok, token, t, "token getter returned wrong token")
}

func TestWithDefaultQueryParams(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"

	defaultParams := map[string]string{
		"pgfire_dbname": "account@db@engine",
		"advanced_mode": "true",
	}

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithDefaultQueryParams(defaultParams),
	)

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	// Check that default params are in cachedParameters
	if conn.cachedParameters["pgfire_dbname"] != "account@db@engine" {
		t.Errorf("default param pgfire_dbname not set correctly, got %s want account@db@engine", conn.cachedParameters["pgfire_dbname"])
	}
	if conn.cachedParameters["advanced_mode"] != "true" {
		t.Errorf("default param advanced_mode not set correctly, got %s want true", conn.cachedParameters["advanced_mode"])
	}
}

func TestWithDefaultQueryParamsDoesNotOverrideExisting(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"

	// First set database via WithDatabaseName, then try to override with default params
	defaultParams := map[string]string{
		"database":      "should_not_override",
		"pgfire_dbname": "account@db@engine",
	}

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithDefaultQueryParams(defaultParams),
	)

	// Database should not be overridden
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "database should not be overridden by default params")

	// But pgfire_dbname should be set
	if conn.cachedParameters["pgfire_dbname"] != "account@db@engine" {
		t.Errorf("default param pgfire_dbname not set correctly")
	}
}

func TestDefaultTransportReturnsNewInstance(t *testing.T) {
	t1 := client.DefaultTransport()
	t2 := client.DefaultTransport()
	if t1 == t2 {
		t.Error("DefaultTransport() should return a new instance each time")
	}
	if t1.IdleConnTimeout != 90*time.Second {
		t.Errorf("expected IdleConnTimeout 90s, got %s", t1.IdleConnTimeout)
	}
	if t1.TLSHandshakeTimeout != 10*time.Second {
		t.Errorf("expected TLSHandshakeTimeout 10s, got %s", t1.TLSHandshakeTimeout)
	}
}

func TestWithTransportStoresOnDriver(t *testing.T) {
	transport := client.DefaultTransport()
	transport.IdleConnTimeout = 5 * time.Minute
	transport.DialContext = (&net.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext

	d := &FireboltDriver{}
	WithTransport(transport)(d)

	if d.transport != transport {
		t.Error("WithTransport should store the transport on the driver")
	}
	stored, ok := d.transport.(*http.Transport)
	if !ok {
		t.Fatal("expected stored transport to be *http.Transport")
	}
	if stored.IdleConnTimeout != 5*time.Minute {
		t.Errorf("expected IdleConnTimeout 5m, got %s", stored.IdleConnTimeout)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWithTransportAcceptsCustomRoundTripper(t *testing.T) {
	custom := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 418}, nil
	})

	d := &FireboltDriver{}
	WithTransport(custom)(d)

	if d.transport == nil {
		t.Fatal("expected transport to be set")
	}
	if _, ok := d.transport.(*http.Transport); ok {
		t.Error("expected transport NOT to be *http.Transport")
	}
}
