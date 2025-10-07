package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/firebolt-db/firebolt-go-sdk/client"

	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

type FireboltDriver struct {
	// engineUrl    string
	// cachedParams map[string]string
	// client       client.Client
	// lastUsedDsn  string
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

	// if d.lastUsedDsn != dsn || d.lastUsedDsn == "" {

	// d.lastUsedDsn = "" //nolintd
	logging.Infolog.Println("constructing new client")
	// parsing dsn string to get configuration settings
	settings, err := ParseDSNString(dsn)
	if err != nil {
		return nil, errors.Wrap(errors.DSNParseError, err)
	}

	// authenticating and getting access token
	logging.Infolog.Println("dsn parsed correctly, trying to authenticate")
	clientInstance, err := client.ClientFactory(settings, client.GetHostNameURL())
	if err != nil {
		return nil, errors.ConstructNestedError("error during initializing client", err)
	}

	engineUrl, cachedParams, err := clientInstance.GetConnectionParameters(context.TODO(), settings.EngineName, settings.Database)
	if err != nil {
		return nil, errors.ConstructNestedError("error during getting connection parameters", err)
	}
	// 	d.lastUsedDsn = dsn //nolint
	// }

	return &FireboltConnector{engineUrl, clientInstance, cachedParams, d}, nil
}

// FireboltConnector is an intermediate type between a Connection and a Driver which stores session data
type FireboltConnector struct {
	engineUrl        string
	client           client.Client
	cachedParameters map[string]string
	driver           *FireboltDriver
}

// Connect returns a connection to the database
func (c *FireboltConnector) Connect(ctx context.Context) (driver.Conn, error) {
	logging.Infolog.Printf("firebolt connection is created")
	return &fireboltConnection{c.client, c.engineUrl, copyMap(c.cachedParameters), c}, nil
}

// Driver returns the underlying driver of the Connector
func (c *FireboltConnector) Driver() driver.Driver {
	return c.driver
}

// init registers a firebolt connector
func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
