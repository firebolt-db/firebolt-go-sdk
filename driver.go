package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

type FireboltDriver struct {
	engineUrl    string
	databaseName string
	client       Client
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
		d.client, err = Authenticate(settings, GetHostNameURL())
		if err != nil {
			return nil, ConstructNestedError("error during authentication", err)
		}

		d.engineUrl, d.databaseName, err = d.client.GetEngineUrlAndDB(context.TODO(), settings.engineName, settings.database)
		d.lastUsedDsn = dsn //nolint
	}

	infolog.Printf("firebolt connection is created")
	return &fireboltConnection{d.client, d.databaseName, d.engineUrl, map[string]string{}}, nil
}

// init registers a firebolt driver
func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
