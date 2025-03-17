package rows

import (
	"testing"
	"unsafe"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestFireboltDecimalComposeDecompose(t *testing.T) {
	rawDecimal := "123.456"
	fd := &FireboltDecimal{}
	if err := fd.Scan(rawDecimal); err != nil {
		t.Fatalf("Error scanning decimal: %v", err)
	}
	newFd := &FireboltDecimal{}
	if err := newFd.Compose(fd.Decompose(nil)); err != nil {
		t.Fatalf("Error composing decimal: %v", err)
	}

	if newFd.Decimal.String() != rawDecimal {
		t.Fatalf("Expected %s, got %s", rawDecimal, newFd.Decimal.String())
	}

	if err := newFd.Scan(nil); err == nil {
		t.Fatalf("Expected error scanning nil decimal")
	}
}

func TestFireboltDecimalReuseBuf(t *testing.T) {
	rawDecimal := "123.456"
	fd := FireboltDecimal{}
	if err := fd.Scan(rawDecimal); err != nil {
		t.Fatalf("Error scanning decimal: %v", err)
	}
	buf := make([]byte, 20)
	if _, _, coeff, _ := fd.Decompose(buf); unsafe.Pointer(&buf[0]) != unsafe.Pointer(&coeff[0]) {
		t.Fatalf("Expected non-empty buffer after decomposing decimal")
	}
	buf = make([]byte, 1)
	if _, _, coeff, _ := fd.Decompose(buf); unsafe.Pointer(&buf[0]) == unsafe.Pointer(&coeff[0]) {
		t.Fatalf("Expected the buffer to not be reused when it's too small")
	}
}

func TestFireboltNullDecimalComposeDecompose(t *testing.T) {
	rawDecimal := "123.456"
	fnd := FireboltNullDecimal{}
	if err := fnd.Scan(rawDecimal); err != nil {
		t.Fatalf("Error scanning decimal: %v", err)
	}
	if fnd.Valid != true {
		t.Fatalf("Unexpected invalid decimal")
	}
	newFnd := FireboltNullDecimal{}
	if err := newFnd.Compose(fnd.Decompose(nil)); err != nil {
		t.Fatalf("Error composing decimal: %v", err)
	}

	utils.AssertEqual(newFnd.Valid, true, t, "Valid flag should be true for non-null value")

	if newFnd.Decimal.String() != rawDecimal {
		t.Fatalf("Expected %s, got %s", rawDecimal, newFnd.Decimal.String())
	}

	if err := newFnd.Scan(nil); err != nil {
		t.Fatalf("Unxpected error scanning nil decimal")
	}
	utils.AssertEqual(newFnd.Valid, false, t, "Valid flag should be false for null value")
}
