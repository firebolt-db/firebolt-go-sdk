package errors

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

func TestNewStructuredError(t *testing.T) {
	errorDetails := types.ErrorDetails{
		Severity:    "error",
		Name:        "TestError",
		Code:        "123",
		Description: "This is a test error",
		Source:      "TestSource",
		Resolution:  "Please fix the error",
		Location: types.Location{
			FailingLine: 10,
			StartOffset: 20,
			EndOffset:   30,
		},
		HelpLink: "https://example.com",
	}

	expectedMessage := "error: TestError (123) - This is a test error, TestSource, resolution: Please fix the error at {FailingLine:10 StartOffset:20 EndOffset:30}, see https://example.com"

	err := NewStructuredError([]types.ErrorDetails{errorDetails})

	if err.Message != expectedMessage {
		t.Errorf("NewStructuredError returned incorrect error message, got: %s, want: %s", err.Message, expectedMessage)
	}
}

func TestStructuredErrorWithMissingFields(t *testing.T) {
	errorDetails := types.ErrorDetails{
		Severity:    "error",
		Name:        "TestError",
		Code:        "123",
		Description: "This is a test error",
	}

	expectedMessage := "error: TestError (123) - This is a test error"

	err := NewStructuredError([]types.ErrorDetails{errorDetails})

	if err.Message != expectedMessage {
		t.Errorf("NewStructuredError returned incorrect error message, got: %s, want: %s", err.Message, expectedMessage)
	}
}

func TestStructuredErrorWithMultipleErrors(t *testing.T) {
	errorDetails := types.ErrorDetails{
		Severity:    "error",
		Name:        "TestError",
		Code:        "123",
		Description: "This is a test error",
		Source:      "TestSource",
		Resolution:  "Please fix the error",
		Location: types.Location{
			FailingLine: 10,
			StartOffset: 20,
			EndOffset:   30,
		},
		HelpLink: "https://example.com",
	}

	errorDetails2 := types.ErrorDetails{
		Severity:    "error",
		Name:        "TestError",
		Code:        "123",
		Description: "This is a test error",
		Source:      "TestSource",
		Resolution:  "Please fix the error",
	}

	expectedMessage := "error: TestError (123) - This is a test error, TestSource, resolution: Please fix the error at {FailingLine:10 StartOffset:20 EndOffset:30}, see https://example.com\nerror: TestError (123) - This is a test error, TestSource, resolution: Please fix the error"

	err := NewStructuredError([]types.ErrorDetails{errorDetails, errorDetails2})

	if err.Message != expectedMessage {
		t.Errorf("NewStructuredError returned incorrect error message, got: %s, want: %s", err.Message, expectedMessage)
	}
}
