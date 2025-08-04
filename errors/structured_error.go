package errors

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/logging"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type StructuredError struct {
	Message string
}

func (e StructuredError) Error() string {
	return e.Message
}

func NewStructuredError(errorDetails []types.ErrorDetails) error {
	// "{severity}: {name} ({code}) - {source}, {description}, resolution: {resolution} at {location} see {helpLink}"
	message := strings.Builder{}
	for _, e := range errorDetails {
		currentMessage := strings.Builder{}
		if message.Len() > 0 {
			message.WriteString("\n")
		}
		if err := formatErrorDetails(&currentMessage, e); err != nil {
			logging.Errorlog.Printf("Failed to format error details: %v", err)
			if asJson, err := json.Marshal(e); err == nil {
				content := fmt.Sprintf("%v", e)
				message.WriteString("Failed to format error details: " + content + ", JSON: " + string(asJson))
			} else {
				// just write json encoded message
				message.WriteString(string(asJson))
			}
		} else {
			message.WriteString(currentMessage.String())
		}
	}
	return &StructuredError{
		Message: message.String(),
	}
}

type errorFieldConfig struct {
	value     string
	delimiter string
	format    string
	name      string
}

func formatErrorDetails(message *strings.Builder, error types.ErrorDetails) error {
	fields := []errorFieldConfig{
		{error.Severity, "", "%s: ", "severity"},
		{error.Name, "", "%s ", "name"},
		{error.Code, "", "(%s) ", "code"},
		{error.Description, "-", "%s", "description"},
		{error.Source, ",", "%s", "source"},
		{error.Resolution, ",", "resolution: %s", "resolution"},
	}
	for _, field := range fields {
		if field.value != "" {
			if field.delimiter != "" {
				addDelimiterIfNotEmpty(message, field.delimiter)
			}
			if _, err := fmt.Fprintf(message, field.format, field.value); err != nil {
				return fmt.Errorf("failed to format %s: %w", field.name, err)
			}
		}
	}
	if error.Location.FailingLine != 0 || error.Location.StartOffset != 0 || error.Location.EndOffset != 0 {
		addDelimiterIfNotEmpty(message, " at")
		if _, err := fmt.Fprintf(message, "%+v", error.Location); err != nil {
			return fmt.Errorf("failed to format failing line: %w", err)
		}
	}
	if error.HelpLink != "" {
		addDelimiterIfNotEmpty(message, ",")
		if _, err := fmt.Fprintf(message, "see %s", error.HelpLink); err != nil {
			return fmt.Errorf("failed to format help link: %w", err)
		}
	}
	return nil
}

func addDelimiterIfNotEmpty(message *strings.Builder, delimiter string) {
	if message.Len() > 0 {
		message.WriteString(delimiter)
		message.WriteString(" ")
	}
}
