package utils

import (
	"bytes"
	"context"
	"database/sql/driver"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
	"time"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"

	"github.com/shopspring/decimal"
)

const assertErrorMessage = "Expected: %v Got: %v"

func AssertEqual(testVal interface{}, expectedVal interface{}, t *testing.T, err string) {
	if expectedVal == nil {
		if testVal != nil {
			t.Log(string(debug.Stack()))
			t.Errorf(err+assertErrorMessage, expectedVal, testVal)
		}
	} else if m, ok := expectedVal.(map[string]driver.Value); ok {
		assertMaps(testVal.(map[string]driver.Value), m, t, err)
	} else if reflect.TypeOf(expectedVal).Kind() == reflect.Slice {
		assertArrays(testVal, expectedVal, t, err)
	} else if d, ok := expectedVal.(decimal.Decimal); ok {
		assertDecimal(testVal, d, t, err)
	} else if b, ok := expectedVal.([]byte); ok {
		assertByte(testVal.([]byte), b, t, err)
	} else if date, ok := expectedVal.(time.Time); ok {
		assertDates(testVal.(time.Time), date, t, err)
	} else if testVal != expectedVal {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
}

func assertArrays(testVal any, expectedVal any, t *testing.T, err string) {
	// manually
	testValType := reflect.ValueOf(testVal)
	expectedValType := reflect.ValueOf(expectedVal)
	if testValType.Kind() != reflect.Slice || expectedValType.Kind() != reflect.Slice {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
	if testValType.Len() != expectedValType.Len() {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
	for i := 0; i < testValType.Len(); i++ {
		AssertEqual(testValType.Index(i).Interface(), expectedValType.Index(i).Interface(), t, err)
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

type decimalEqualer interface {
	Equal(decimal.Decimal) bool
}

type decimalGetter interface {
	GetDecimal() *decimal.Decimal
}

func assertDecimal(testVal interface{}, expectedVal decimal.Decimal, t *testing.T, err string) {
	var decimalTestVal decimalEqualer
	if eq, ok := testVal.(decimalEqualer); ok {
		decimalTestVal = eq
	} else if dg, ok := testVal.(decimalGetter); ok {
		dp := dg.GetDecimal()
		if dp == nil {
			t.Errorf(err+assertErrorMessage, expectedVal, testVal)
			return
		}
		decimalTestVal = *dp
	} else {
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
		return
	}
	if !decimalTestVal.Equal(expectedVal) {
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

func getCallerFunctionName() string {
	caller, _, _, ok := runtime.Caller(2)
	if !ok {
		panic("Failed to get caller function name")
	}
	strs := strings.Split(runtime.FuncForPC(caller).Name(), ".")
	return strs[len(strs)-1]
}

// RunInMemoryAndStream runs a test case with both in memory result and streamed result
func RunInMemoryAndStream(t *testing.T, testCase func(t *testing.T, ctx context.Context)) {
	ctx := context.Background()
	testName := getCallerFunctionName()
	t.Run(testName+"InMemory", func(t *testing.T) { testCase(t, ctx) })
	t.Run(testName+"Streaming", func(t *testing.T) { testCase(t, contextUtils.WithStreaming(ctx)) })
}

func GetQueryFromFile(fileName string) string {
	query, err := os.ReadFile(fileName)
	if err != nil {
		panic(err)
	}
	return string(query)
}
