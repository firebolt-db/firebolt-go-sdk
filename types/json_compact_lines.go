package types

type RecordMessageType string

const (
	MessageTypeStart   RecordMessageType = "START"
	MessageTypeData    RecordMessageType = "DATA"
	MessageTypeSuccess RecordMessageType = "FINISHED_WITH_SUCCESS"
	MessageTypeError   RecordMessageType = "FINISH_WITH_ERROR"
)

// JSONLinesRecord is a struct that represents any of the possible JSONLines records
type JSONLinesRecord struct {
	MessageType   RecordMessageType `json:"message_type"`
	ResultColumns *[]Column         `json:"result_columns,omitempty"`
	QueryID       *string           `json:"query_id,omitempty"`
	QueryLabel    *string           `json:"query_label,omitempty"`
	RequestID     *string           `json:"request_id,omitempty"`
	Data          *[][]interface{}  `json:"data,omitempty"`
	Errors        *[]ErrorDetails   `json:"errors,omitempty"`
	Statistics    *interface{}      `json:"statistics,omitempty"`
}
