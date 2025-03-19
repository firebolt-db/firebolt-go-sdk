package rows

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

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
		utils.AssertEqual(fs[key], val, t, "Struct values do not match")
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
			utils.AssertEqual(fs[key], val, t, "Struct values do not match")
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
		utils.AssertEqual(fs.Struct[key], val, t, "Struct values do not match")
	}
	utils.AssertEqual(fs.Valid, true, t, "Struct is invalid")

	if err := fs.Scan(nil); err != nil {
		t.Fatalf("Error scanning nil struct: %v", err)
	}
	utils.AssertEqual(fs.Valid, false, t, "Struct is not invalid")
}
