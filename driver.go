package fireboltgosdk

import (
	"database/sql"
	"database/sql/driver"
	"io/ioutil"
	"log"
	"strings"
)

type FireboltDriver struct {
}

// Open parses the dsn string, and if correct tries to establish a connection
func (d FireboltDriver) Open(dsn string) (driver.Conn, error) {
	log.Println("Opening firebolt driver")

	// parsing dsn string to get configuration settings
	settings, err := ParseDSNString(dsn)
	if err != nil {
		return nil, ConstructNestedError("error during parsing a dsn", err)
	}

	// authenticating and getting access token
	log.Println("dsn parsed correctly, trying to authenticate")
	client, err := Authenticate(settings.username, settings.password)
	if err != nil {
		return nil, ConstructNestedError("error during authentication", err)
	}

	// getting accountId, either default, or by specified accountName
	var accountId string
	if settings.accountName == "" {
		log.Println("account name not specified, trying to get a default account id")
		accountId, err = client.GetDefaultAccountId()
	} else {
		accountId, err = client.GetAccountIdByName(settings.accountName)
	}
	if err != nil {
		return nil, ConstructNestedError("error during getting account id", err)
	}

	// getting engineUrl either by using engineName if available,
	// if not using default engine for the database
	var engineUrl string
	if settings.engineName != "" {
		if strings.Contains(settings.engineName, ".") {
			engineUrl, err = makeCanonicalUrl(settings.engineName), nil
		} else {
			engineUrl, err = client.GetEngineUrlByName(settings.engineName, accountId)
		}
	} else {
		log.Println("engine name not set, trying to get a default engine")
		engineUrl, err = client.GetEngineUrlByDatabase(settings.database, accountId)
	}
	if err != nil {
		return nil, ConstructNestedError("error during getting engine url", err)
	}

	log.Printf("firebolt connection is created")
	return &fireboltConnection{client, settings.database, engineUrl, map[string]string{}}, nil
}

// init registers a firebolt driver
func init() {
	log.SetOutput(ioutil.Discard)
	sql.Register("firebolt", &FireboltDriver{})
}
