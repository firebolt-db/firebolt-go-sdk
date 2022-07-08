package fireboltgosdk

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

type fireboltRows struct {
	response       QueryResponse
	cursorPosition int
}

func (f *fireboltRows) Columns() []string {
	numColumns := len(f.response.Meta)
	result := make([]string, 0, numColumns)

	for _, column := range f.response.Meta {
		result = append(result, column.Name)
	}

	return result
}

func (f *fireboltRows) Close() error {
	f.cursorPosition = len(f.response.Data)
	return nil
}

func (f *fireboltRows) Next(dest []driver.Value) error {
	if f.cursorPosition == len(f.response.Data) {
		return io.EOF
	}

	for i, column := range f.response.Meta {
		var err error
		if dest[i], err = parseValue(column.Type, f.response.Data[f.cursorPosition][i]); err != nil {
			return fmt.Errorf("error during fetching Next result: %v", err)
		}
	}

	f.cursorPosition++
	return nil
}

func parseValue(columnType string, val interface{}) (driver.Value, error) {
	switch columnType {
	case "UInt8":
		return uint8(val.(float64)), nil
	case "UInt32":
		return uint32(val.(float64)), nil
	case "Int32":
		return int32(val.(float64)), nil
	case "UInt64":
		return uint64(val.(float64)), nil
	case "Int64":
		return int64(val.(float64)), nil
	case "Float32":
		return float32(val.(float64)), nil
	case "Float64":
		return val.(float64), nil
	case "String":
		return val.(string), nil
	case "DateTime":
		// Go doesn't use yyyy-mm-dd layout. Instead, it uses the value: Mon Jan 2 15:04:05 MST 2006
		return time.Parse("2006-01-02 15:04:05", val.(string))
	case "Date":
		return time.Parse("2006-01-02", val.(string))
	}

	const (
		arrayPrefix = "Array("
		arraySuffix = ")"
	)

	if strings.HasPrefix(columnType, arrayPrefix) && strings.HasSuffix(columnType, arraySuffix) {
		s := reflect.ValueOf(val)
		res := make([]driver.Value, s.Len())

		for i := 0; i < s.Len(); i++ {
			res[i], _ = parseValue(columnType[len(arrayPrefix):len(columnType)-len(arraySuffix)], s.Index(i).Interface())
		}
		return res, nil
	}

	return nil, fmt.Errorf("type not known: %s", columnType)
}
