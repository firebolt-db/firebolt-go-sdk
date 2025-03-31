package errors

import "github.com/firebolt-db/firebolt-go-sdk/logging"

type nestedError struct {
	message string
	err     error
}

func (e *nestedError) Error() string {
	if e.err == nil {
		return e.message
	}
	return e.message + ": " + e.err.Error()
}

func (e *nestedError) Unwrap() error {
	return e.err
}

func (e *nestedError) Is(target error) bool {
	if nErr, ok := target.(*nestedError); ok {
		return nErr.message == e.message
	}
	return e.message == target.Error()
}

func ConstructNestedError(message string, err error) error {
	logging.Infolog.Printf("%s: %v", message, err)
	return &nestedError{message: message, err: err}
}

func Wrap(wrapper, inner error) error {
	if wrapper == nil {
		return inner
	}
	return ConstructNestedError(wrapper.Error(), inner)
}
