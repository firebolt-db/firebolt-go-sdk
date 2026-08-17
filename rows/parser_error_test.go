package rows

import (
	"strings"
	"testing"
)

func TestParseValueRejectsInvalidArrays(t *testing.T) {
	tests := []struct {
		name    string
		typ     string
		value   interface{}
		wantErr string
	}{
		{name: "element", typ: "array(int)", value: []interface{}{float64(1), "bad"}, wantErr: "array element 1"},
		{name: "nested element", typ: "array(array(int))", value: []interface{}{[]interface{}{float64(1), "bad"}}, wantErr: "array element 0: error parsing array element 1"},
		{name: "not an array", typ: "array(int)", value: "bad", wantErr: "unexpected value for array type"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseValue(test.typ, test.value)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("parseValue() error = %v, want error containing %q", err, test.wantErr)
			}
		})
	}
}
