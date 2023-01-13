package fireboltgosdk

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

const (
	uint8Type      = "UINT8"
	uint16Type     = "UINT16"
	uint32Type     = "UINT32"
	uint64Type     = "UINT64"
	int8Type       = "INT8"
	int16Type      = "INT16"
	int32Type      = "INT32"
	int64Type      = "INT64"
	float32Type    = "FLOAT32"
	float64Type    = "FLOAT64"
	stringType     = "STRING"
	datetimeType   = "DATETIME"
	datetime64Type = "DATETIME64"
	dateType       = "DATE"
	date32Type     = "DATE32"
	PGDate         = "PGDATE"
	TimestampNtz   = "TIMESTAMPNTZ"
	TimestampTz    = "TIMESTAMPTZ"
)

type fireboltRows struct {
	response          []QueryResponse
	resultSetPosition int // Position of the result set (for multiple statements)
	cursorPosition    int // Position of the cursor in current result set
}

// Columns returns a list of Meta names in response
func (f *fireboltRows) Columns() []string {
	numColumns := len(f.response[f.resultSetPosition].Meta)
	result := make([]string, 0, numColumns)

	for _, column := range f.response[f.resultSetPosition].Meta {
		result = append(result, column.Name)
	}

	return result
}

// Close makes the rows unusable
func (f *fireboltRows) Close() error {
	f.resultSetPosition = len(f.response) - 1
	f.cursorPosition = len(f.response[f.resultSetPosition].Data)
	return nil
}

// Next fetches the values of the next row, returns io.EOF if it was the end
func (f *fireboltRows) Next(dest []driver.Value) error {
	if f.cursorPosition == len(f.response[f.resultSetPosition].Data) {
		return io.EOF
	}

	for i, column := range f.response[f.resultSetPosition].Meta {
		var err error
		//log.Printf("Rows.Next: %s, %v", column.Type, f.response.Data[f.cursorPosition][i])
		if dest[i], err = parseValue(column.Type, f.response[f.resultSetPosition].Data[f.cursorPosition][i]); err != nil {
			return ConstructNestedError("error during fetching Next result", err)
		}
	}

	f.cursorPosition++
	return nil
}

// HasNextResultSet reports whether there is another result set available
func (f *fireboltRows) HasNextResultSet() bool {
	return len(f.response) > f.resultSetPosition+1
}

// NextResultSet advances to the next result set, if it is available, otherwise returns io.EOF
func (f *fireboltRows) NextResultSet() error {
	if !f.HasNextResultSet() {
		return io.EOF
	}

	f.cursorPosition = 0
	f.resultSetPosition += 1

	return nil
}

// checkTypeValue checks that val type could be changed to columnType
func checkTypeValue(columnType string, val interface{}) error {
	switch strings.ToUpper(columnType) {
	case uint8Type, int8Type, uint16Type, int16Type, uint32Type, int32Type, uint64Type, int64Type, float32Type, float64Type:
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("expected to convert a value to float64, but couldn't: %v", val)
		}
		return nil
	case stringType, datetimeType, dateType, date32Type, datetime64Type, PGDate, TimestampNtz, TimestampTz:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected to convert a value to string, but couldn't: %v", val)
		}
		return nil
	}
	return fmt.Errorf("unknown column type: %s", columnType)
}

// parseDateTimeValue parses different date types
func parseDateTimeValue(columnType string, value string) (driver.Value, error) {
	switch strings.ToUpper(columnType) {
	case datetimeType:
		// Go doesn't use yyyy-mm-dd layout. Instead, it uses the value: Mon Jan 2 15:04:05 MST 2006
		return time.Parse("2006-01-02 15:04:05", value)
	case datetime64Type:
		return time.Parse("2006-01-02 15:04:05.000000", value)
	case dateType, date32Type:
		return time.Parse("2006-01-02", value)
	case PGDate:
		return time.Parse("2006-01-02", value)
	case TimestampNtz:
		return time.Parse("2006-01-02 15:04:05.000000", value)
	case TimestampTz:
		res, err := time.Parse("2006-01-02 15:04:05.000000+00", value)
		if err != nil {
			// Try parsing half-timezones e.g. Asia/Calcutta as +05:30
			res, err = time.Parse("2006-01-02 15:04:05.000000-07:00", value)
		}
		return res, err
	}
	return nil, fmt.Errorf("type not known: %s", columnType)
}

// parseSingleValue parses all columns types except arrays
func parseSingleValue(columnType string, val interface{}) (driver.Value, error) {
	if err := checkTypeValue(columnType, val); err != nil {
		return nil, ConstructNestedError("error during value parsing", err)
	}

	switch strings.ToUpper(columnType) {
	case uint8Type:
		return uint8(val.(float64)), nil
	case int8Type:
		return int8(val.(float64)), nil
	case uint16Type:
		return uint16(val.(float64)), nil
	case int16Type:
		return int16(val.(float64)), nil
	case uint32Type:
		return uint32(val.(float64)), nil
	case int32Type:
		return int32(val.(float64)), nil
	case uint64Type:
		return uint64(val.(float64)), nil
	case int64Type:
		return int64(val.(float64)), nil
	case float32Type:
		return float32(val.(float64)), nil
	case float64Type:
		return val.(float64), nil
	case stringType:
		return val.(string), nil
	case datetime64Type, datetimeType, dateType, date32Type, PGDate, TimestampNtz, TimestampTz:
		return parseDateTimeValue(columnType, val.(string))
	}

	return nil, fmt.Errorf("type not known: %s", columnType)
}

// parseValue treating the val according to the column type and casts it to one of the go native types:
// uint8, uint32, uint64, int32, int64, float32, float64, string, Time or []driver.Value for arrays
func parseValue(columnType string, val interface{}) (driver.Value, error) {
	const (
		nullablePrefix   = "Nullable("
		arrayPrefix      = "Array("
		dateTime64Prefix = "DateTime64("
		decimalPrefix    = "Decimal("
		suffix           = ")"
	)

	// No need to parse type if the value is nil
	if val == nil {
		return nil, nil
	}

	if strings.HasPrefix(columnType, arrayPrefix) && strings.HasSuffix(columnType, suffix) {
		s := reflect.ValueOf(val)
		res := make([]driver.Value, s.Len())

		for i := 0; i < s.Len(); i++ {
			res[i], _ = parseValue(columnType[len(arrayPrefix):len(columnType)-len(suffix)], s.Index(i).Interface())
		}
		return res, nil
	} else if strings.HasPrefix(columnType, dateTime64Prefix) && strings.HasSuffix(columnType, suffix) {
		return parseSingleValue("DateTime64", val)
	} else if strings.HasPrefix(columnType, decimalPrefix) && strings.HasSuffix(columnType, suffix) {
		return parseSingleValue("Float64", val)
	} else if strings.HasPrefix(columnType, nullablePrefix) && strings.HasSuffix(columnType, suffix) {
		return parseSingleValue(columnType[len(nullablePrefix):len(columnType)-len(suffix)], val)
	}

	return parseSingleValue(columnType, val)
}
