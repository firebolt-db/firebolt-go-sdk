package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
)

type FireboltDriver struct {
	engineUrl    string
	databaseName string
	client       *Client
	lastUsedDsn  string
}

// Open parses the dsn string, and if correct tries to establish a connection
func (d FireboltDriver) Open(dsn string) (driver.Conn, error) {
	infolog.Println("Opening firebolt driver")

	if d.lastUsedDsn != dsn || d.lastUsedDsn == "" {

		d.lastUsedDsn = "" //nolint
		infolog.Println("constructing new client")
		// parsing dsn string to get configuration settings
		settings, err := ParseDSNString(dsn)
		if err != nil {
			return nil, ConstructNestedError("error during parsing a dsn", err)
		}

		// authenticating and getting access token
		infolog.Println("dsn parsed correctly, trying to authenticate")
		d.client, err = Authenticate(settings.clientID, settings.clientSecret, GetHostNameURL())
		if err != nil {
			return nil, ConstructNestedError("error during authentication", err)
		}

		// getting accountId, either default, or by specified accountName
		var accountId string
		if settings.accountName == "" {
			infolog.Println("account name not specified, trying to get a default account id")
			accountId, err = d.client.GetDefaultAccountId(context.TODO())
		} else {
			accountId, err = d.client.GetAccountIdByName(context.TODO(), settings.accountName)
		}
		if err != nil {
			return nil, ConstructNestedError("error during getting account id", err)
		}

		// getting engineUrl either by using engineName if available,
		// if not using default engine for the database
		if settings.engineName != "" {
			if strings.Contains(settings.engineName, ".") {
				d.engineUrl, err = makeCanonicalUrl(settings.engineName), nil
			} else {
				d.engineUrl, err = d.client.GetEngineUrlByName(context.TODO(), settings.engineName, accountId)
			}
		} else {
			infolog.Println("engine name not set, trying to get a default engine")
			d.engineUrl, err = d.client.GetEngineUrlByDatabase(context.TODO(), settings.database, accountId)
		}
		if err != nil {
			return nil, ConstructNestedError("error during getting engine url", err)
		}
		d.databaseName = settings.database
		d.lastUsedDsn = dsn //nolint
	}

	infolog.Printf("firebolt connection is created")
	return &fireboltConnection{d.client, d.databaseName, d.engineUrl, map[string]string{}}, nil
}

// init registers a firebolt driver
func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
