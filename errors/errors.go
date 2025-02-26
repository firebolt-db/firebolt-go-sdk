package errors

import (
	"errors"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

func ConstructNestedError(message string, err error) error {
	newErr := errors.Join(errors.New(message), err)
	logging.Infolog.Print(newErr)
	return newErr
}
