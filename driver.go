package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

type FireboltDriver struct {
	engineUrl    string
	cachedParams map[string]string
	client       Client
	lastUsedDsn  string
}

// Open parses the dsn string, and if correct tries to establish a connection
func (d *FireboltDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return conn.Connect(context.Background())
}

func copyMap(original map[string]string) map[string]string {
	newMap := make(map[string]string)
	for k, v := range original {
		newMap[k] = v
	}
	return newMap
}

func (d *FireboltDriver) OpenConnector(dsn string) (driver.Connector, error) {
	logging.Infolog.Println("Opening firebolt connector")

	if d.lastUsedDsn != dsn || d.lastUsedDsn == "" {

		d.lastUsedDsn = "" //nolint
		logging.Infolog.Println("constructing new client")
		// parsing dsn string to get configuration settings
		settings, err := ParseDSNString(dsn)
		if err != nil {
			return nil, errors.ConstructNestedError("error during parsing a dsn", err)
		}

		// authenticating and getting access token
		logging.Infolog.Println("dsn parsed correctly, trying to authenticate")
		d.client, err = Authenticate(settings, GetHostNameURL())
		if err != nil {
			return nil, errors.ConstructNestedError("error during authentication", err)
		}

		d.engineUrl, d.cachedParams, err = d.client.GetConnectionParameters(context.TODO(), settings.engineName, settings.database)
		if err != nil {
			return nil, errors.ConstructNestedError("error during getting engine url", err)
		}
		d.lastUsedDsn = dsn //nolint
	}

	return &FireboltConnector{d.engineUrl, d.client, copyMap(d.cachedParams), d}, nil
}

// FireboltConnector is an intermediate type between a Connection and a Driver which stores session data
type FireboltConnector struct {
	engineUrl        string
	client           Client
	cachedParameters map[string]string
	driver           *FireboltDriver
}

// Connect returns a connection to the database
func (c *FireboltConnector) Connect(ctx context.Context) (driver.Conn, error) {
	logging.Infolog.Printf("firebolt connection is created")
	return &fireboltConnection{c.client, c.engineUrl, c.cachedParameters, c}, nil
}

// Driver returns the underlying driver of the Connector
func (c *FireboltConnector) Driver() driver.Driver {
	return c.driver
}

// init registers a firebolt connector
func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
