package utils

import (
	"bytes"
	"database/sql/driver"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const assertErrorMessage = "Expected: %v Got: %v"

func AssertEqual(testVal interface{}, expectedVal interface{}, t *testing.T, err string) {
	if m, ok := expectedVal.(map[string]driver.Value); ok {
		assertMaps(testVal.(map[string]driver.Value), m, t, err)
	} else if arr, ok := expectedVal.([]driver.Value); ok {
		assertArrays(testVal.([]driver.Value), arr, t, err)
	} else if d, ok := expectedVal.(decimal.Decimal); ok {
		assertDecimal(testVal.(decimal.Decimal), d, t, err)
	} else if b, ok := expectedVal.([]byte); ok {
		assertByte(testVal.([]byte), b, t, err)
	} else if date, ok := expectedVal.(time.Time); ok {
		assertDates(testVal.(time.Time), date, t, err)
	} else if testVal != expectedVal {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
}

func assertArrays(testVal []driver.Value, expectedVal []driver.Value, t *testing.T, err string) {
	// manually
	if len(testVal) != len(expectedVal) {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
	for i, value := range expectedVal {
		AssertEqual(testVal[i], value, t, err)
	}
}

func assertMaps(testVal map[string]driver.Value, expectedVal map[string]driver.Value, t *testing.T, err string) {
	// manually
	if len(testVal) != len(expectedVal) {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
	for key, value := range expectedVal {
		AssertEqual(testVal[key], value, t, err)
	}
}

func assertDates(testVal time.Time, expectedVal time.Time, t *testing.T, err string) {
	if !testVal.Equal(expectedVal) {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal.In(expectedVal.Location()))
	}
}

func assertByte(testVal []byte, expectedVal []byte, t *testing.T, err string) {
	if !bytes.Equal(testVal, expectedVal) {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
}

func assertDecimal(testVal decimal.Decimal, expectedVal decimal.Decimal, t *testing.T, err string) {
	if !testVal.Equal(expectedVal) {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
}

func RaiseIfError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Encountered error %s", err)
	}
}

func GetAuthResponse(expiry int) []byte {
	var response = `{
   "access_token": "aMysteriousToken",
   "refresh_token": "refresh",
   "scope": "offline_access",
   "expires_in": ` + strconv.Itoa(expiry) + `,
   "token_type": "Bearer"
}`
	return []byte(response)
}

func GetAuthResponseV0(expiry int) []byte {
	var response = `{
   "access_token": "aMysteriousToken",
   "refresh_token": "refresh",
   "scope": "offline_access",
   "expires_in": ` + strconv.Itoa(expiry) + `,
   "token_type": "Bearer"
}`
	return []byte(response)
}
