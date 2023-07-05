package fireboltgosdk

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

const (
	intType    = "int"
	longType   = "long"
	floatType  = "float"
	doubleType = "double"

	textType = "text"

	dateType    = "date"
	pgDateType  = "pgdate"

	timestampType    = "timestamp"
	timestampNtzType = "timestampntz"
	timestampTzType  = "timestamptz"

	booleanType = "boolean"
	byteaType   = "bytea"
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
	switch columnType {
	case intType, longType, floatType, doubleType:
		if _, ok := val.(float64); !ok {
			return fmt.Errorf("expected to convert a value to float64, but couldn't: %v", val)
		}
		return nil
	case textType, dateType, pgDateType, timestampType, timestampNtzType, timestampTzType, byteaType:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected to convert a value to string, but couldn't: %v", val)
		}
		return nil
	case booleanType:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected to convert a value to bool, but couldn't: %v", val)
		}
		return nil

	}
	return fmt.Errorf("unknown column type: %s", columnType)
}

func parseTimestampTz(value string) (driver.Value, error) {
	formats := [...]string{"2006-01-02 15:04:05.000000-07", "2006-01-02 15:04:05.000000-07:00", "2006-01-02 15:04:05.000000-07:00:00",
		"2006-01-02 15:04:05-07", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05-07:00:00"}
	var res time.Time
	var err error
	for _, format := range formats {
		res, err = time.Parse(format, value)
		if err == nil {
			break
		}
	}
	return res, err
}

// parseDateTimeValue parses different date types
func parseDateTimeValue(columnType string, value string) (driver.Value, error) {
	switch columnType {
	case dateType, pgDateType:
		return time.Parse("2006-01-02", value)
	case timestampType:
		// Go doesn't use yyyy-mm-dd layout. Instead, it uses the value: Mon Jan 2 15:04:05 MST 2006
		return time.Parse("2006-01-02 15:04:05", value)
	case timestampNtzType:
		return time.Parse("2006-01-02 15:04:05.000000", value)
	case timestampTzType:
		return parseTimestampTz(value)
	}
	return nil, fmt.Errorf("type not known: %s", columnType)
}

// parseSingleValue parses all columns types except arrays
func parseSingleValue(columnType string, val interface{}) (driver.Value, error) {
	if err := checkTypeValue(columnType, val); err != nil {
		return nil, ConstructNestedError("error during value parsing", err)
	}

	switch columnType {
	case intType:
		return int32(val.(float64)), nil
	case longType:
		return int64(val.(float64)), nil
	case floatType:
		return float32(val.(float64)), nil
	case doubleType:
		return val.(float64), nil
	case textType:
		return val.(string), nil
	case dateType, pgDateType, timestampType, timestampNtzType, timestampTzType:
		return parseDateTimeValue(columnType, val.(string))
	case booleanType:
		return val.(bool), nil
	case byteaType:
		trimmedString := strings.TrimPrefix(val.(string), "\\x")
		decoded, err := hex.DecodeString(trimmedString)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse to hex value: %v", val)
		}
		return decoded, nil
	}

	return nil, fmt.Errorf("type not known: %s", columnType)
}

// parseValue treating the val according to the column type and casts it to one of the go native types:
// uint8, uint32, uint64, int32, int64, float32, float64, string, Time or []driver.Value for arrays
func parseValue(columnType string, val interface{}) (driver.Value, error) {
	const (
		nullableSuffix = " null"
		arrayPrefix    = "array("
		decimalPrefix  = "Decimal("
		suffix         = ")"
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
	} else if strings.HasPrefix(columnType, decimalPrefix) && strings.HasSuffix(columnType, suffix) {
		return parseSingleValue("double", val)
	} else if strings.HasSuffix(columnType, nullableSuffix) {
		return parseValue(columnType[0:len(columnType)-len(nullableSuffix)], val)
	}

	return parseSingleValue(columnType, val)
}
