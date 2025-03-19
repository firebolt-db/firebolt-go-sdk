package rows

import (
	"fmt"
	"reflect"
)

type FireboltArray []any

// Scan implements the sql.Scanner interface.
func (fa *FireboltArray) Scan(src interface{}) error {
	if src == nil {
		return fmt.Errorf("unexpected nil value for array")
	}
	t := reflect.ValueOf(src)
	if t.Kind() != reflect.Slice {
		return fmt.Errorf("unexpected array value type: %T", src)
	}

	*fa = make([]any, t.Len())
	for i := 0; i < t.Len(); i++ {
		(*fa)[i] = t.Index(i).Interface()
	}
	return nil
}

type FireboltNullArray struct {
	Array FireboltArray
	Valid bool
}

// Scan implements the sql.Scanner interface.
func (fna *FireboltNullArray) Scan(src interface{}) error {
	if src == nil {
		fna.Valid = false
		return nil
	}
	fna.Valid = true
	return fna.Array.Scan(src)
}
