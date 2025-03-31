package rows

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

const ARRAY_VALUES_DO_NOT_MATCH = "Array values do not match"

func TestFireboltArrayScan(t *testing.T) {
	value := []int{1, 2, 3}
	array := FireboltArray{}
	if err := array.Scan(value); err != nil {
		t.Fatalf("Error scanning array: %v", err)
	}
	for i, val := range value {
		utils.AssertEqual(array[i], val, t, ARRAY_VALUES_DO_NOT_MATCH)
	}

	if err := array.Scan(nil); err == nil {
		t.Fatalf("Expected error scanning nil array with FireboltArray")
	}
}

func TestFireboltArrayScanNested(t *testing.T) {
	value := [][]string{{"a", "b"}, {"c", "d"}}
	array := FireboltArray{}
	if err := array.Scan(value); err != nil {
		t.Fatalf("Unexpected error scanning nested array")
	}
	for i, val := range value {
		innerArray := array[i].([]string)
		for j, innerVal := range val {
			utils.AssertEqual(innerArray[j], innerVal, t, ARRAY_VALUES_DO_NOT_MATCH)
		}
	}
}

func TestFireboltNullableArrayScan(t *testing.T) {
	value := []int{1, 2, 3}
	array := FireboltNullArray{}
	if err := array.Scan(value); err != nil {
		t.Fatalf("Error scanning array: %v", err)
	}
	for i, val := range value {
		utils.AssertEqual(array.Array[i], val, t, ARRAY_VALUES_DO_NOT_MATCH)
	}

	if err := array.Scan(nil); err != nil {
		t.Fatalf("Error scanning nil array: %v", err)
	}
	utils.AssertEqual(array.Valid, false, t, "Array is not invalid")
}
