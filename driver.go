package fireboltgosdk

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
)

type FireboltDriver struct {
}

// Open parses the dsn string, and if correct tries to establish a connection
func (d FireboltDriver) Open(dsn string) (driver.Conn, error) {
	settings, err := ParseDSNString(dsn)
	if err != nil {
		return nil, err
	}
	client, err := Authenticate(settings.username, settings.password)
	if err != nil {
		return nil, fmt.Errorf("error during authentication: %v", err)
	}

	// getting engineUrl either by using engineName if available,
	// if not using default engine for the database
	var engineUrl string
	if settings.engineName != "" {
		engineUrl, err = client.GetEngineUrlByName(settings.engineName, settings.accountName)
	} else {
		engineUrl, err = client.GetEngineUrlByDatabase(settings.database, settings.accountName)
	}
	if err != nil {
		return nil, fmt.Errorf("error during getting engine url: %v", err)
	}

	return &fireboltConnection{client, settings.database, engineUrl}, nil
}

// init registers a firebolt driver
func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
