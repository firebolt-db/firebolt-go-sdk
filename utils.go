package fireboltgosdk

import (
	"io/ioutil"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

func init() {
	logging.Infolog.SetOutput(ioutil.Discard)
}
