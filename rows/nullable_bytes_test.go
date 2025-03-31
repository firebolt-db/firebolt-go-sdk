package rows

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestNullableBytesScanning(t *testing.T) {
	// Test scanning a hex string
	hexValue := "\\x48656C6C6F" // "Hello" in hex
	nb := NullBytes{}
	if err := nb.Scan(hexValue); err != nil {
		t.Fatalf("Error scanning hex string: %v", err)
	}
	utils.AssertEqual(nb.Valid, true, t, "Valid flag should be true for non-null value")
	utils.AssertEqual(string(nb.Bytes), "Hello", t, "Byte values do not match")

	// Test scanning raw bytes
	rawBytes := []byte("World")
	if err := nb.Scan(rawBytes); err != nil {
		t.Fatalf("Error scanning raw bytes: %v", err)
	}
	utils.AssertEqual(nb.Valid, true, t, "Valid flag should be true for non-null value")
	utils.AssertEqual(string(nb.Bytes), "World", t, "Byte values do not match")

	// Test scanning nil
	if err := nb.Scan(nil); err != nil {
		t.Fatalf("Error scanning nil value: %v", err)
	}
	utils.AssertEqual(nb.Valid, false, t, "Valid flag should be false for null value")
	utils.AssertEqual(nb.Bytes, []byte(nil), t, "Bytes should be nil for null value")

	// Test scanning invalid hex string
	invalidHex := "\\xZZ"
	if err := nb.Scan(invalidHex); err == nil {
		t.Fatal("Expected error when scanning invalid hex string")
	}

	// Test scanning invalid type
	if err := nb.Scan(123); err == nil {
		t.Fatal("Expected error when scanning invalid type")
	}
}
