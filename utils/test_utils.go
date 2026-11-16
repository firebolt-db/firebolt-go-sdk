package utils

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"math"
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

// AssertGeographyPoint compares a hex-encoded (E)WKB point by coordinates. The
// engine's geography round trip is not bit-exact: POINT(1 1) has come back with
// X as both 0.9999999999999998 and 0.9999999999999997 depending on the build.
func AssertGeographyPoint(testVal interface{}, expectedX, expectedY float64, t *testing.T, err string) {
	const tolerance = 1e-9
	x, y, parseErr := parseWKBPoint(testVal)
	if parseErr != "" {
		t.Log(string(debug.Stack()))
		t.Errorf("%s: %s: %v", err, parseErr, testVal)
		return
	}
	if math.Abs(x-expectedX) > tolerance || math.Abs(y-expectedY) > tolerance {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, [2]float64{expectedX, expectedY}, [2]float64{x, y})
	}
}

func parseWKBPoint(val interface{}) (float64, float64, string) {
	const (
		ewkbZFlag    = 0x80000000
		ewkbMFlag    = 0x40000000
		ewkbSRIDFlag = 0x20000000
	)
	str, ok := val.(string)
	if !ok {
		return 0, 0, "geography value is not a string"
	}
	raw, decodeErr := hex.DecodeString(str)
	if decodeErr != nil || len(raw) < 5 {
		return 0, 0, "geography value is not hex-encoded WKB"
	}
	var order binary.ByteOrder
	switch raw[0] {
	case 0:
		order = binary.BigEndian
	case 1:
		order = binary.LittleEndian
	default:
		return 0, 0, "geography value has an invalid WKB byte order"
	}
	geomType := order.Uint32(raw[1:5])
	offset := 5
	if geomType&ewkbSRIDFlag != 0 {
		offset += 4
	}
	if geomType&^ewkbSRIDFlag != 1 || geomType&(ewkbZFlag|ewkbMFlag) != 0 || len(raw) < offset+16 {
		return 0, 0, "geography value is not a 2D WKB point"
	}
	x := math.Float64frombits(order.Uint64(raw[offset : offset+8]))
	y := math.Float64frombits(order.Uint64(raw[offset+8 : offset+16]))
	return x, y, ""
}

func assertArrays(testVal interface{}, expectedVal interface{}, t *testing.T, err string) {
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

// RunClientAndServerPreparedStatements runs a test case with both client and server prepared statements
func RunClientAndServerPreparedStatements(t *testing.T, testCase func(t *testing.T, ctx context.Context)) {
	ctx := context.Background()
	testName := getCallerFunctionName()
	t.Run(testName+"ClientPreparedStatements", func(t *testing.T) { testCase(t, ctx) })
	t.Run(testName+"ServerPreparedStatements", func(t *testing.T) {
		testCase(t, contextUtils.WithPreparedStatementsStyle(ctx, contextUtils.PreparedStatementsStyleFbNumeric))
	})
}

func Must(err error) {
	if err != nil {
		panic(err)
	}
}
