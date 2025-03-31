package errors

import (
	"errors"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestNestedErrorError(t *testing.T) {
	originalErr := errors.New("original error")
	nestedErr := ConstructNestedError("nested error", originalErr)
	utils.AssertEqual(nestedErr.Error(), "nested error: original error", t, "Error message should be nested")

	nestedErr2 := ConstructNestedError("nested error", nestedErr)
	utils.AssertEqual(nestedErr2.Error(), "nested error: nested error: original error", t, "Error message should be doubly nested")

	nestedErr = ConstructNestedError("nested error", nil)
	utils.AssertEqual(nestedErr.Error(), "nested error", t, "Error message should be nested")
}

func TestNestedErrorIs(t *testing.T) {
	originalErr := errors.New("original error")
	mediumErr := errors.New("medium error")
	topError := errors.New("top error")
	nestedErr := WrapWithError(WrapWithError(originalErr, mediumErr), topError)

	utils.AssertEqual(errors.Is(nestedErr, topError), true, t, "Top error should be found")
	utils.AssertEqual(errors.Is(nestedErr, mediumErr), true, t, "Medium error should be found")
	utils.AssertEqual(errors.Is(nestedErr, originalErr), true, t, "Original error should be found")

	utils.AssertEqual(errors.Is(nestedErr, errors.New("not found")), false, t, "Non-existent error should not be found")
}
