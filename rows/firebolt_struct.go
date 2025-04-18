package rows

import (
	"fmt"
	"reflect"
)

type FireboltStruct map[string]interface{}

func (fs *FireboltStruct) Scan(src interface{}) error {
	if src == nil {
		return fmt.Errorf("unexpected nil value for struct")
	}
	t := reflect.ValueOf(src)
	if t.Kind() != reflect.Map {
		return fmt.Errorf("unexpected struct value type: %T", src)
	}

	*fs = make(map[string]interface{}, t.Len())
	for _, key := range t.MapKeys() {
		(*fs)[key.String()] = t.MapIndex(key).Interface()
	}
	return nil
}

type FireboltNullStruct struct {
	Struct FireboltStruct
	Valid  bool
}

func (fns *FireboltNullStruct) Scan(src interface{}) error {
	if src == nil {
		fns.Valid = false
		return nil
	}
	fns.Valid = true
	return fns.Struct.Scan(src)
}
