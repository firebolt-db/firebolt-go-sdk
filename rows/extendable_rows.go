package rows

import (
	"database/sql/driver"

	"github.com/firebolt-db/firebolt-go-sdk/client"
)

type ExtendableRows interface {
	driver.Rows
	AppendResponse(response *client.Response) error
}
