package rows

import (
	"testing"
	"unsafe"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

const ERROR_SCANNING_DECIMAL = "Error scanning decimal: %v"
const RAW_DECIMAL = "123.456"

func TestFireboltDecimalComposeDecompose(t *testing.T) {
	fd := &FireboltDecimal{}
	if err := fd.Scan(RAW_DECIMAL); err != nil {
		t.Fatalf(ERROR_SCANNING_DECIMAL, err)
	}
	newFd := &FireboltDecimal{}
	if err := newFd.Compose(fd.Decompose(nil)); err != nil {
		t.Fatalf("Error composing decimal: %v", err)
	}

	if newFd.String() != RAW_DECIMAL {
		t.Fatalf("Expected %s, got %s", RAW_DECIMAL, newFd.String())
	}

	if err := newFd.Scan(nil); err == nil {
		t.Fatalf("Expected error scanning nil decimal")
	}
}

func TestFireboltDecimalReuseBuf(t *testing.T) {
	fd := FireboltDecimal{}
	if err := fd.Scan(RAW_DECIMAL); err != nil {
		t.Fatalf(ERROR_SCANNING_DECIMAL, err)
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
	fnd := FireboltNullDecimal{}
	if err := fnd.Scan(RAW_DECIMAL); err != nil {
		t.Fatalf(ERROR_SCANNING_DECIMAL, err)
	}
	if fnd.Valid != true {
		t.Fatalf("Unexpected invalid decimal")
	}
	newFnd := FireboltNullDecimal{}
	if err := newFnd.Compose(fnd.Decompose(nil)); err != nil {
		t.Fatalf("Error composing decimal: %v", err)
	}

	utils.AssertEqual(newFnd.Valid, true, t, "Valid flag should be true for non-null value")

	if newFnd.Decimal.String() != RAW_DECIMAL {
		t.Fatalf("Expected %s, got %s", RAW_DECIMAL, newFnd.Decimal.String())
	}

	if err := newFnd.Scan(nil); err != nil {
		t.Fatalf("Unxpected error scanning nil decimal")
	}
	utils.AssertEqual(newFnd.Valid, false, t, "Valid flag should be false for null value")
}
