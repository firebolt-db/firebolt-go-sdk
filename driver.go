package fireboltgosdk

import (
	"database/sql"
	"database/sql/driver"
)

type FireboltDriver struct {
}

func (d FireboltDriver) Open(dsn string) (driver.Conn, error) {
	settings, err := ParseDSNString(dsn)
	if err != nil {
		return nil, err
	}
	client, err := Authenticate(settings.username, settings.password)
	if err != nil {
		return nil, err
	}

	//getting engineUrl either by using engineName if available,
	//if not using default engine for the database
	var engineUrl string
	if settings.engineName != "" {
		engineUrl, err = client.GetEngineUrlByName(settings.engineName, settings.accountName)
	} else {
		engineUrl, err = client.GetEngineUrlByDatabase(settings.database, settings.accountName)
	}
	if err != nil {
		return nil, err
	}

	return &fireboltConnection{*client, settings.database, engineUrl}, nil
}

func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
