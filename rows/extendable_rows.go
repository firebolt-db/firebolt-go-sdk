package rows

import (
	"database/sql/driver"

	"github.com/firebolt-db/firebolt-go-sdk/client"
)

type ExtendableRowsWithResult interface {
	driver.Rows
	ProcessAndAppendResponse(response *client.Response) error
	Result() (driver.Result, error)
}
