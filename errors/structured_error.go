package errors

import (
	"fmt"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type StructuredError struct {
	Message string
}

func (e StructuredError) Error() string {
	return e.Message
}

func NewStructuredError(errorDetails []types.ErrorDetails) *StructuredError {
	// "{severity}: {name} ({code}) - {source}, {description}, resolution: {resolution} at {location} see {helpLink}"
	message := strings.Builder{}
	for _, error := range errorDetails {
		if message.Len() > 0 {
			message.WriteString("\n")
		}
		formatErrorDetails(&message, error)
	}
	return &StructuredError{
		Message: message.String(),
	}
}
func formatErrorDetails(message *strings.Builder, error types.ErrorDetails) string {
	if error.Severity != "" {
		message.WriteString(fmt.Sprintf("%s: ", error.Severity))
	}
	if error.Name != "" {
		message.WriteString(fmt.Sprintf("%s ", error.Name))
	}
	if error.Code != "" {
		message.WriteString(fmt.Sprintf("(%s) ", error.Code))
	}
	if error.Description != "" {
		addDelimiterIfNotEmpty(message, "-")
		message.WriteString(error.Description)
	}
	if error.Source != "" {
		addDelimiterIfNotEmpty(message, ",")
		message.WriteString(error.Source)
	}
	if error.Resolution != "" {
		addDelimiterIfNotEmpty(message, ",")
		message.WriteString(fmt.Sprintf("resolution: %s", error.Resolution))
	}
	if error.Location.FailingLine != 0 || error.Location.StartOffset != 0 || error.Location.EndOffset != 0 {
		addDelimiterIfNotEmpty(message, " at")
		message.WriteString(fmt.Sprintf("%+v", error.Location))
	}
	if error.HelpLink != "" {
		addDelimiterIfNotEmpty(message, ",")
		message.WriteString(fmt.Sprintf("see %s", error.HelpLink))
	}
	return message.String()
}

func addDelimiterIfNotEmpty(message *strings.Builder, delimiter string) {
	if message.Len() > 0 {
		message.WriteString(delimiter)
		message.WriteString(" ")
	}
}
