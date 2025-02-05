package fireboltgosdk

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"runtime/debug"
	"strconv"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const assertErrorMessage = "Expected: %v Got: %v"

func assert(testVal interface{}, expectedVal interface{}, t *testing.T, err string) {
	if m, ok := expectedVal.(map[string]driver.Value); ok {
		assertMaps(testVal.(map[string]driver.Value), m, t, err)
	} else if arr, ok := expectedVal.([]driver.Value); ok {
		assertArrays(testVal.([]driver.Value), arr, t, err)
	} else if d, ok := expectedVal.(decimal.Decimal); ok {
		assertDecimal(testVal.(decimal.Decimal), d, t, err)

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
		assert(testVal[i], value, t, err)
	}
}

func assertMaps(testVal map[string]driver.Value, expectedVal map[string]driver.Value, t *testing.T, err string) {
	// manually
	if len(testVal) != len(expectedVal) {
		t.Log(string(debug.Stack()))
		t.Errorf(err+assertErrorMessage, expectedVal, testVal)
	}
	for key, value := range expectedVal {
		assert(testVal[key], value, t, err)
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

func mockRows(isMultiStatement bool) driver.RowsNextResultSet {
	resultJson := []string{
		`{
        "query":{"query_id":"16FF2A0300ECA753"},
        "meta":[
        	{"name":"int_col","type":"int null"},
        	{"name":"bigint_col","type":"long"},
        	{"name":"float_col","type":"float"},
        	{"name":"double_col","type":"double"},
        	{"name":"text_col","type":"text"},
        	{"name":"date_col","type":"date"},
        	{"name":"timestamp_col","type":"timestamp"},
        	{"name":"pgdate_col","type":"pgdate"},
        	{"name":"timestampntz_col","type":"timestampntz"},
        	{"name":"timestamptz_col","type":"timestamptz"},
        	{"name":"legacy_bool_col","type":"int"},
        	{"name":"array_col","type":"array(int)"},
        	{"name":"nested_array_col","type":"array(array(text))"},
        	{"name":"new_bool_col","type":"boolean"},
        	{"name":"decimal_col","type":"Decimal(38, 30) null"},
        	{"name":"decimal_array_col","type":"array(Decimal(38, 30) null)"},
        	{"name":"bytea_col","type":"bytea null"},
        	{"name":"geography_col","type":"geography null"},
            {"name":"struct_col","type":"struct(a int, s struct(a array(int), b text))"}
        ],
        "data":[
        	[null,1,0.312321,123213.321321,"text", "2080-12-31","1989-04-15 01:02:03","0002-01-01","1989-04-15 01:02:03.123456","1989-04-15 02:02:03.123456+00",1,[1,2,3],[[]],true, 123.12345678, [123.12345678], "\\x616263313233", "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F", {"a":1,"s":{"a":[1,2,3],"b":"text"}}],
        	[2,"37237366456",0.312321,123213.321321,"text","1970-01-01","1970-01-01 00:00:00","0001-01-01","1989-04-15 01:02:03.123457","1989-04-15 01:02:03.1234+05:30",1,[1,2,3],[[]],true, -123.12345678, [-123.12345678, 0.0], "\\x6162630A0AE3858D20E3858E5C", null, {"a": 2, "s": null}],
        	[3,null,"inf",123213.321321,"text","1970-01-01","1970-01-01 00:00:00","0001-01-01","1989-04-15 01:02:03.123458","1989-04-15 01:02:03+01",1,[5,2,3,2],[["TEST","TEST1"],["TEST3"]],false, 0.0, [0.0], null, null, null],
        	[2,1,"-inf",123213.321321,"text","1970-01-01","1970-01-01 00:00:00","0001-01-01","1989-04-15 01:02:03.123457","1111-01-05 17:04:42.123456+05:53:28",1,[1,2,3],[[]],false, 123456781234567812345678.123456781234567812345678, [123456781234567812345678.12345678123456781234567812345678], null, null, null],
    	    [2,1,"-nan",123213.321321,"text","1970-01-01","1970-01-01 00:00:00","0001-01-01","1989-04-15 01:02:03.123457","1989-04-15 02:02:03.123456-01",1,[1,2,3],[[]],null, null, [null], null, null, null, null]
        ],
        "rows":5,
        "statistics":{
        	"elapsed":0.001797702,
        	"rows_read":3,
        	"bytes_read":293,
        	"time_before_execution":0.001251613,
        	"time_to_execute":0.000544098,
        	"scanned_bytes_cache":2003,
            "scanned_bytes_storage":0
        }
    }`,

		`{
        "query":{"query_id":"16FF2A0300ECA753"},
        "meta":[{"name":"int_col","type":"int null"}],
        "data":[[3], [null]],
        "rows":2,
        "statistics":{
        	"elapsed":0.001797702,
        	"rows_read":2,
        	"bytes_read":293,
        	"time_before_execution":0.001251613,
        	"time_to_execute":0.000544098,
        	"scanned_bytes_cache":2003,
            "scanned_bytes_storage":0
        }
    }`,
	}

	var responses []QueryResponse
	for i := 0; i < 2; i += 1 {
		if i != 0 && !isMultiStatement {
			break
		}
		var response QueryResponse
		if err := json.Unmarshal([]byte(resultJson[i]), &response); err != nil {
			panic(err)
		} else {
			responses = append(responses, response)
		}
	}

	return &fireboltRows{responses, 0, 0}
}

func mockRowsSingleValue(value interface{}, columnType string) driver.RowsNextResultSet {
	response := QueryResponse{
		Query:      map[string]string{"query_id": "16FF2A0300ECA753"},
		Meta:       []Column{{Name: "single_col", Type: columnType}},
		Data:       [][]interface{}{{value}},
		Rows:       1,
		Errors:     []ErrorDetails{},
		Statistics: map[string]interface{}{},
	}

	return &fireboltRows{[]QueryResponse{response}, 0, 0}
}

// TestRowsColumns checks, that correct column names are returned
func TestRowsColumns(t *testing.T) {
	rows := mockRows(false)

	columnNames := []string{"int_col", "bigint_col", "float_col", "double_col", "text_col", "date_col", "timestamp_col", "pgdate_col", "timestampntz_col", "timestamptz_col", "legacy_bool_col", "array_col", "nested_array_col", "new_bool_col", "decimal_col", "decimal_array_col", "bytea_col", "geography_col", "struct_col"}
	if !reflect.DeepEqual(rows.Columns(), columnNames) {
		t.Errorf("column lists are not equal")
	}
}

// TestRowsClose checks Close method, and inability to use rows afterward
func TestRowsClose(t *testing.T) {
	rows := mockRows(false)
	if rows.Close() != nil {
		t.Errorf("Closing rows was not successful")
	}

	var dest []driver.Value
	if rows.Next(dest) != io.EOF {
		t.Errorf("Next of closed rows didn't return EOF")
	}
}

// TestRowsNext check Next method
func TestRowsNext(t *testing.T) {
	rows := mockRows(false)
	var dest = make([]driver.Value, 19)

	// First row
	err := rows.Next(dest)
	loc, _ := time.LoadLocation("UTC")

	assert(err, nil, t, "Next shouldn't return an error at row 1")
	assert(dest[0], nil, t, "results not equal for int32 at row 1")
	assert(dest[1], int64(1), t, "results not equal for int64 at row 1")
	assert(dest[2], float32(0.312321), t, "results not equal for float32 at row 1")
	assert(dest[3], float64(123213.321321), t, "results not equal for float64 at row 1")
	assert(dest[4], "text", t, "results not equal for string at row 1")
	assert(dest[5], time.Date(2080, 12, 31, 0, 0, 0, 0, loc), t, "results not equal for date at row 1")
	assert(dest[6], time.Date(1989, 04, 15, 1, 2, 3, 0, loc), t, "results not equal for datetime at row 1")
	assert(dest[7], time.Date(0002, 01, 01, 0, 0, 0, 0, loc), t, "results not equal for pgdate at row 1")
	assert(dest[8], time.Date(1989, 04, 15, 1, 2, 3, 123456000, loc), t, "results not equal for timestampntz at row 1")
	assertDates(dest[9].(time.Time), time.Date(1989, 04, 15, 2, 2, 3, 123456000, loc), t, " at row 1")
	assert(dest[13], true, t, "results not equal for boolean at row 1")
	assert(dest[14], decimal.NewFromFloat(123.12345678), t, "results not equal for decimal at row 1")
	assert(dest[15].([]driver.Value), []driver.Value{decimal.NewFromFloat(123.12345678)}, t, "results not equal for decimal array at row 1")
	assertByte(dest[16].([]byte), []byte("abc123"), t, "results not equal for bytes at row 1")
	assert(dest[17], "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F", t, "results not equal for geography at row 1")
	assert(dest[18].(map[string]driver.Value), map[string]driver.Value{"a": int32(1), "s": map[string]driver.Value{"a": []driver.Value{int32(1), int32(2), int32(3)}, "b": "text"}}, t, "results not equal for struct at row 1")

	// Second row
	err = rows.Next(dest)
	assert(err, nil, t, "Next shouldn't return an error at row 2")
	assert(dest[1], int64(37237366456), t, "results not equal for int64 at row 2")
	// Creating custom timezone with no name (e.g. Europe/Berlin)
	// similar to Firebolt's return format
	timezone, _ := time.LoadLocation("Asia/Calcutta")
	assertDates(dest[9].(time.Time), time.Date(1989, 04, 15, 1, 2, 3, 123400000, timezone), t, "results not equal for time at row 2")
	assert(dest[13], true, t, "results not equal for boolean at row 2")
	assert(dest[14], decimal.NewFromFloat(-123.12345678), t, "results not equal for decimal at row 2")
	assert(dest[15].([]driver.Value), []driver.Value{decimal.NewFromFloat(-123.12345678), decimal.NewFromFloat(0.0)}, t, "results not equal for decimal array at row 2")
	assertByte(dest[16].([]byte), []byte("abc\n\nㅍ ㅎ\\"), t, "results not equal for bytes at row 2")
	assert(dest[18].(map[string]driver.Value), map[string]driver.Value{"a": int32(2), "s": nil}, t, "results not equal for struct at row 2")

	// Third row
	err = rows.Next(dest)
	assert(err, nil, t, "Next shouldn't return an error at row 3")
	assert(dest[0], int32(3), t, "results not equal for int32 at row 3")
	assert(dest[1], nil, t, "results not equal for int64 at row 3")
	assert(dest[2], float32(math.Inf(1)), t, "results not equal for float32 at row 3")
	assert(dest[3], float64(123213.321321), t, "results not equal for float64 at row 3")
	assert(dest[4], "text", t, "results not equal for string at row 3")
	assert(dest[13], false, t, "results not equal for boolean at row 3")
	assert(dest[14], decimal.NewFromFloat(0.0), t, "results not equal for decimal at row 3")
	assert(dest[15].([]driver.Value), []driver.Value{decimal.NewFromFloat(0.0)}, t, "results not equal for decimal array at row 3")
	assert(dest[18], nil, t, "results not equal for struct at row 3")

	// Fourth row
	err = rows.Next(dest)
	assert(err, nil, t, "Next shouldn't return an error at row 4")
	assert(dest[2], float32(math.Inf(-1)), t, "results not equal for float32 at row 4")
	assertDates(dest[9].(time.Time), time.Date(1111, 01, 5, 11, 11, 14, 123456000, loc), t, " at row 4")
	assert(dest[13], false, t, "results not equal for boolean at row 4")
	var longDouble = decimal.NewFromFloat(123456781234567812345678.12345678123456781234567812345678)
	assert(dest[14], longDouble, t, "results not equal for decimal at row 4")
	assert(dest[15].([]driver.Value), []driver.Value{longDouble}, t, "results not equal for decimal array at row 4")

	// Fifth row
	err = rows.Next(dest)
	assert(err, nil, t, "Next shouldn't return an error at row 5")
	// Cannot do assert since NaN != NaN according to the standard
	// math.IsNaN only works for float64, converting float32 NaN to float64 results in 0
	if !(dest[2].(float32) != dest[2].(float32)) {
		t.Log(string(debug.Stack()))
		t.Errorf("results not equal for float32 Expected: NaN Got: %s", dest[2])
	}
	assertDates(dest[9].(time.Time), time.Date(1989, 4, 15, 3, 2, 3, 123456000, loc), t, " at row 5")
	assert(dest[13], nil, t, "results not equal for boolean at row 5")
	assert(dest[14], nil, t, "results not equal for decimal at row 5")
	assert(dest[15].([]driver.Value), []driver.Value{nil}, t, "results not equal for decimal array at row 5")

	// Sixth row (does not exist)
	assert(io.EOF, rows.Next(dest), t, "Next should return io.EOF if no data available anymore at row 6")

	// Seventh row (does not exist)
	assert(io.EOF, rows.Next(dest), t, "Next should return io.EOF if no data available anymore at row 7")

	assert(rows.HasNextResultSet(), false, t, "Has Next result set didn't return false")
	assert(rows.NextResultSet(), io.EOF, t, "Next result set didn't return false at the end")
}

// TestRowsNextSet check rows with multiple statements
func TestRowsNextSet(t *testing.T) {
	rows := mockRows(true)

	// check next result set functions
	assert(rows.HasNextResultSet(), true, t, "HasNextResultSet returned false, but shouldn't")
	assert(rows.NextResultSet(), nil, t, "NextResultSet returned an error, but shouldn't")
	assert(rows.HasNextResultSet(), false, t, "HasNextResultSet returned true, but shouldn't")

	// check columns of the next result set
	if !reflect.DeepEqual(rows.Columns(), []string{"int_col"}) {
		t.Errorf("Columns of the next result set are incorrect")
	}

	// check values of the next result set
	var dest = make([]driver.Value, 1)

	assert(rows.Next(dest), nil, t, "Next shouldn't return an error")
	assert(dest[0], int32(3), t, "results are not equal")

	assert(rows.Next(dest), nil, t, "Next shouldn't return an error")
	assert(dest[0], nil, t, "results are not equal")

	assert(io.EOF, rows.Next(dest), t, "Next should return io.EOF if no data available anymore")
}

func TestRowsNextStructError(t *testing.T) {
	rowsJson := `{
        "query":{"query_id":"16FF2A0300ECA753"},
        "meta":[{"name":"struct_col","type":"struct(a int, s struct(b timestamp))"}],
        "data":[[{"a": 1, "s": {"b": "invalid"}}]],
        "rows":1,
        "statistics":{}
    }`
	var response QueryResponse
	if err := json.Unmarshal([]byte(rowsJson), &response); err != nil {
		panic(err)
	}

	rows := &fireboltRows{[]QueryResponse{response}, 0, 0}
	var dest = make([]driver.Value, 1)
	if err := rows.Next(dest); err == nil {
		t.Errorf("Next should return an error")
	}
}

func TestRowsNextStructWithNestedSpaces(t *testing.T) {
	rowsJson := `{
        "query":{"query_id":"16FF2A0300ECA753"},
        "meta":[{"name":"struct_col","type":"struct(` + "`a b`" + ` int, s struct(` + "`c d`" + ` timestamp))"}],
        "data":[[{"a b": 1, "s": {"c d": "1989-04-15 01:02:03"}}]],
        "rows":1,
        "statistics":{}
    }`
	var response QueryResponse
	if err := json.Unmarshal([]byte(rowsJson), &response); err != nil {
		panic(err)
	}

	rows := &fireboltRows{[]QueryResponse{response}, 0, 0}
	var dest = make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Errorf("Next returned an error: %s", err)
	}

	if dest[0].(map[string]driver.Value)["a b"] != int32(1) {
		t.Errorf("results are not equal")
	}
	if dest[0].(map[string]driver.Value)["s"].(map[string]driver.Value)["c d"] != time.Date(1989, 04, 15, 1, 2, 3, 0, time.UTC) {
		t.Errorf("results are not equal")
	}
}

func TestRowsQuotedLong(t *testing.T) {
	intRaw := "-9223372036854775808"
	intValue, _ := strconv.ParseInt(intRaw, 10, 64)
	rows := mockRowsSingleValue(intRaw, "long")
	var dest = make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Errorf("Next returned an error: %s", err)
	}
	assert(dest[0], intValue, t, "results are not equal for long")
}

func TestRowsDecimalType(t *testing.T) {
	floatValue := 123456789.123456789
	stringValue := "1234567890123456789012345678901234567890"
	stringDecimalValue, _ := decimal.NewFromString(stringValue)
	cases := [][]interface{}{
		{nil, nil},
		{floatValue, decimal.NewFromFloat(floatValue)},
		{stringValue, stringDecimalValue},
	}
	for _, c := range cases {
		rows := mockRowsSingleValue(c[0], "Decimal(10, 35)")
		var dest = make([]driver.Value, 1)
		if err := rows.Next(dest); err != nil {
			t.Errorf("Next returned an error: %s", err)
		}
		assert(dest[0], c[1], t, fmt.Sprintf("results are not equal for %v", c[0]))
	}
}
