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
	return &fireboltConnection{*client}, nil
}

func init() {
	sql.Register("firebolt", &FireboltDriver{})
}
