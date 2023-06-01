package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

type FireboltDriver struct {
	engineUrl    string
	databaseName string
	client       *Client
	lastUsedDsn  string
}

const engineStatusRunning = "Running"

// Open parses the dsn string, and if correct tries to establish a connection
func (d FireboltDriver) Open(dsn string) (driver.Conn, error) {
	infolog.Println("Opening firebolt driver")

	if d.lastUsedDsn != dsn || d.lastUsedDsn == "" {

		d.lastUsedDsn = "" //nolint
		infolog.Println("Creating a new client")
		settings, err := ParseDSNString(dsn)
		if err != nil {
			return nil, ConstructNestedError("error during parsing a dsn", err)
		}

		if err = validateSettings(settings); err != nil {
			return nil, ConstructNestedError("invalid connection string", err)
		}

		// authenticating and getting access token
		if d.client, err = Authenticate(settings.clientId, settings.clientSecret, GetHostNameURL()); err != nil {
			return nil, ConstructNestedError("authentication error", err)
		}

		systemEngineURL, err := d.client.GetSystemEngineURL(context.TODO(), settings.accountName)
		if err != nil {
			return nil, ConstructNestedError("error getting system engine url", err)
		}

		if len(settings.engine) == 0 {
			infolog.Println("Connected to a system engine")
			d.engineUrl = systemEngineURL + QueryUrl
			d.databaseName = settings.database
		} else {
			engineUrl, status, dbName, err := d.client.GetEngineUrlStatusDBByName(context.TODO(), settings.engine, systemEngineURL)
			if err != nil {
				return nil, ConstructNestedError("error during getting engine info", err)
			}
			if status != engineStatusRunning {
				return nil, fmt.Errorf("engine %s is not running", settings.engine)
			}
			if len(dbName) == 0 {
				return nil, fmt.Errorf("you don't have permission to access a database attached to an engine %s", settings.engine)
			}
			if len(settings.database) == 0 {
				d.databaseName = dbName
			} else if settings.database != dbName {
				return nil, fmt.Errorf("engine %s is not attached to database %s", settings.engine, settings.database)
			}
			d.engineUrl = engineUrl
			d.client.AccountId, err = d.client.GetAccountId(context.TODO(), settings.accountName)
			if err != nil {
				return nil, fmt.Errorf("error resolving account %s to an id: %v", settings.accountName, err)
			}
		}
		d.lastUsedDsn = dsn //nolint
	}

	infolog.Printf("Firebolt connection was created successfully")
	return &fireboltConnection{d.client, d.databaseName, d.engineUrl, map[string]string{}}, nil
}

func validateSettings(settings *fireboltSettings) error {
	if settings.accountName == "" {
		return errors.New("missing account_name parameter")
	}
	if settings.clientId == "" {
		return errors.New("missing client_id parameter")
	}
	if settings.clientSecret == "" {
		return errors.New("missing client_secret parameter")
	}
	return nil
}

// init registers a firebolt driver
func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
