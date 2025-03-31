package rows

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

const STRUCT_VALUES_DO_NOT_MATCH = "Struct values do not match"

func TestFireboltStructScan(t *testing.T) {
	value := map[string]interface{}{
		"key1": 1,
		"key2": "value",
	}
	fs := FireboltStruct{}
	if err := fs.Scan(value); err != nil {
		t.Fatalf("Error scanning struct: %v", err)
	}
	for key, val := range value {
		utils.AssertEqual(fs[key], val, t, STRUCT_VALUES_DO_NOT_MATCH)
	}

	if err := fs.Scan(nil); err == nil {
		t.Fatalf("Expected error scanning nil struct with FireboltStruct")
	}
}

func TestFireboltStructScanNested(t *testing.T) {
	value := map[string]interface{}{
		"key1": 1,
		"key2": map[string]string{
			"nested1": "a",
			"nested2": "b",
		},
	}
	fs := FireboltStruct{}
	if err := fs.Scan(value); err != nil {
		t.Fatalf("Unexpected error scanning nested struct")
	}
	for key, val := range value {
		if key == "key2" {
			nested := fs[key].(map[string]string)
			for nestedKey, nestedVal := range val.(map[string]string) {
				utils.AssertEqual(nested[nestedKey], nestedVal, t, "Nested struct values do not match")
			}
		} else {
			utils.AssertEqual(fs[key], val, t, STRUCT_VALUES_DO_NOT_MATCH)
		}
	}
}

func TestFireboltNullStructScan(t *testing.T) {
	value := map[string]interface{}{
		"key1": 1,
		"key2": "value",
	}
	fs := FireboltNullStruct{}
	if err := fs.Scan(value); err != nil {
		t.Fatalf("Error scanning struct: %v", err)
	}
	for key, val := range value {
		utils.AssertEqual(fs.Struct[key], val, t, STRUCT_VALUES_DO_NOT_MATCH)
	}
	utils.AssertEqual(fs.Valid, true, t, "Struct is invalid")

	if err := fs.Scan(nil); err != nil {
		t.Fatalf("Error scanning nil struct: %v", err)
	}
	utils.AssertEqual(fs.Valid, false, t, "Struct is not invalid")
}
