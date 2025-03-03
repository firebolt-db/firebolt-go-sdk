package errors

import (
	"fmt"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

func ConstructNestedError(message string, err error) error {
	logging.Infolog.Printf("%s: %v", message, err)
	return fmt.Errorf("%s: %v", message, err)
}
