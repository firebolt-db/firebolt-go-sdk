package fireboltgosdk

import (
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"log"
)

type FireboltDriver struct {
}

// Open parses the dsn string, and if correct tries to establish a connection
func (d FireboltDriver) Open(dsn string) (driver.Conn, error) {
	log.Println("Opening firebolt driver")

	settings, err := ParseDSNString(dsn)
	if err != nil {
		return nil, err
	}

	log.Println("dsn parsed correctly, trying to authenticate")
	client, err := Authenticate(settings.username, settings.password)
	if err != nil {
		return nil, ConstructNestedError("error during authentication", err)
	}

	// getting engineUrl either by using engineName if available,
	// if not using default engine for the database
	var engineUrl string
	if settings.engineName != "" {
		engineUrl, err = client.GetEngineUrlByName(settings.engineName, settings.accountName)
	} else {
		log.Println("engine name not set, trying to get a default engine")
		engineUrl, err = client.GetEngineUrlByDatabase(settings.database, settings.accountName)
	}
	if err != nil {
		return nil, ConstructNestedError("error during getting engine url", err)
	}

	log.Printf("firebolt connection is created")
	return &fireboltConnection{client, settings.database, engineUrl}, nil
}

// init registers a firebolt driver
func init() {
	log.SetOutput(ioutil.Discard)
	sql.Register("firebolt", &FireboltDriver{})
}
