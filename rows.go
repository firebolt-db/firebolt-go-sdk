package fireboltgosdk

import (
	"database/sql/driver"
	"io"
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
		val := f.response.Data[f.cursorPosition][i]
		switch column.Type {
		case "Int32":
			dest[i] = int32(val.(float64))
		case "Int64":
			dest[i] = int64(val.(float64))
		case "Float32":
			dest[i] = float32(val.(float64))
		case "Float64":
			dest[i] = val.(float64)
		default:
			dest[i] = val
		}
	}

	f.cursorPosition++
	return nil
}
