package rows

import (
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

	"github.com/firebolt-db/firebolt-go-sdk/types"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
	"github.com/shopspring/decimal"
)

const nextErrorMessage = "Next returned an error: %s"

// testRowsColumns checks, that correct column names are returned
func testRowsColumns(t *testing.T, rowsFactory func(isMultiStatement bool) driver.RowsNextResultSet) {
	rows := rowsFactory(false)

	columnNames := []string{"int_col", "bigint_col", "float_col", "double_col", "text_col", "date_col", "timestamp_col", "pgdate_col", "timestampntz_col", "timestamptz_col", "legacy_bool_col", "array_col", "nested_array_col", "new_bool_col", "decimal_col", "decimal_array_col", "bytea_col", "geography_col", "struct_col"}
	if !reflect.DeepEqual(rows.Columns(), columnNames) {
		t.Errorf("column lists are not equal")
	}
}

// testRowsClose checks Close method, and inability to use rows afterward
func testRowsClose(t *testing.T, rowsFactory func(isMultiStatement bool) driver.RowsNextResultSet) {
	rows := rowsFactory(false)
	if rows.Close() != nil {
		t.Errorf("Closing rows was not successful")
	}

	var dest []driver.Value
	if rows.Next(dest) != io.EOF {
		t.Errorf("Next of closed rows didn't return EOF")
	}
}

// testRowsNext check Next method
func testRowsNext(t *testing.T, rowsFactory func(isMultiStatement bool) driver.RowsNextResultSet) {
	rows := rowsFactory(false)
	var dest = make([]driver.Value, 19)

	// First row
	err := rows.Next(dest)
	loc, _ := time.LoadLocation("UTC")

	utils.AssertEqual(err, nil, t, "Next shouldn't return an error at row 1")
	utils.AssertEqual(dest[0], nil, t, "results not equal for int32 at row 1")
	utils.AssertEqual(dest[1], int64(1), t, "results not equal for int64 at row 1")
	utils.AssertEqual(dest[2], float32(0.312321), t, "results not equal for float32 at row 1")
	utils.AssertEqual(dest[3], float64(123213.321321), t, "results not equal for float64 at row 1")
	utils.AssertEqual(dest[4], "text", t, "results not equal for string at row 1")
	utils.AssertEqual(dest[5], time.Date(2080, 12, 31, 0, 0, 0, 0, loc), t, "results not equal for date at row 1")
	utils.AssertEqual(dest[6], time.Date(1989, 04, 15, 1, 2, 3, 0, loc), t, "results not equal for datetime at row 1")
	utils.AssertEqual(dest[7], time.Date(0002, 01, 01, 0, 0, 0, 0, loc), t, "results not equal for pgdate at row 1")
	utils.AssertEqual(dest[8], time.Date(1989, 04, 15, 1, 2, 3, 123456000, loc), t, "results not equal for timestampntz at row 1")
	utils.AssertEqual(dest[9].(time.Time), time.Date(1989, 04, 15, 2, 2, 3, 123456000, loc), t, " at row 1")
	utils.AssertEqual(dest[13], true, t, "results not equal for boolean at row 1")
	utils.AssertEqual(dest[14], decimal.NewFromFloat(123.12345678), t, "results not equal for decimal at row 1")
	utils.AssertEqual(dest[15].([]driver.Value), []driver.Value{decimal.NewFromFloat(123.12345678)}, t, "results not equal for decimal array at row 1")
	utils.AssertEqual(dest[16].([]byte), []byte("abc123"), t, "results not equal for bytes at row 1")
	utils.AssertEqual(dest[17], "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F", t, "results not equal for geography at row 1")
	utils.AssertEqual(dest[18].(map[string]driver.Value), map[string]driver.Value{"a": int32(1), "s": map[string]driver.Value{"a": []driver.Value{int32(1), int32(2), int32(3)}, "b": "text"}}, t, "results not equal for struct at row 1")

	// Second row
	err = rows.Next(dest)
	utils.AssertEqual(err, nil, t, "Next shouldn't return an error at row 2")
	utils.AssertEqual(dest[1], int64(37237366456), t, "results not equal for int64 at row 2")
	// Creating custom timezone with no name (e.g. Europe/Berlin)
	// similar to Firebolt's return format
	timezone, _ := time.LoadLocation("Asia/Calcutta")
	utils.AssertEqual(dest[9].(time.Time), time.Date(1989, 04, 15, 1, 2, 3, 123400000, timezone), t, "results not equal for time at row 2")
	utils.AssertEqual(dest[13], true, t, "results not equal for boolean at row 2")
	utils.AssertEqual(dest[14], decimal.NewFromFloat(-123.12345678), t, "results not equal for decimal at row 2")
	utils.AssertEqual(dest[15].([]driver.Value), []driver.Value{decimal.NewFromFloat(-123.12345678), decimal.NewFromFloat(0.0)}, t, "results not equal for decimal array at row 2")
	utils.AssertEqual(dest[16].([]byte), []byte("abc\n\nㅍ ㅎ\\"), t, "results not equal for bytes at row 2")
	utils.AssertEqual(dest[18].(map[string]driver.Value), map[string]driver.Value{"a": int32(2), "s": nil}, t, "results not equal for struct at row 2")

	// Third row
	err = rows.Next(dest)
	utils.AssertEqual(err, nil, t, "Next shouldn't return an error at row 3")
	utils.AssertEqual(dest[0], int32(3), t, "results not equal for int32 at row 3")
	utils.AssertEqual(dest[1], nil, t, "results not equal for int64 at row 3")
	utils.AssertEqual(dest[2], float32(math.Inf(1)), t, "results not equal for float32 at row 3")
	utils.AssertEqual(dest[3], float64(123213.321321), t, "results not equal for float64 at row 3")
	utils.AssertEqual(dest[4], "text", t, "results not equal for string at row 3")
	utils.AssertEqual(dest[13], false, t, "results not equal for boolean at row 3")
	utils.AssertEqual(dest[14], decimal.NewFromFloat(0.0), t, "results not equal for decimal at row 3")
	utils.AssertEqual(dest[15].([]driver.Value), []driver.Value{decimal.NewFromFloat(0.0)}, t, "results not equal for decimal array at row 3")
	utils.AssertEqual(dest[18], nil, t, "results not equal for struct at row 3")

	// Fourth row
	err = rows.Next(dest)
	utils.AssertEqual(err, nil, t, "Next shouldn't return an error at row 4")
	utils.AssertEqual(dest[2], float32(math.Inf(-1)), t, "results not equal for float32 at row 4")
	utils.AssertEqual(dest[9].(time.Time), time.Date(1111, 01, 5, 11, 11, 14, 123456000, loc), t, " at row 4")
	utils.AssertEqual(dest[13], false, t, "results not equal for boolean at row 4")
	var longDouble = decimal.NewFromFloat(123456781234567812345678.12345678123456781234567812345678)
	utils.AssertEqual(dest[14], longDouble, t, "results not equal for decimal at row 4")
	utils.AssertEqual(dest[15].([]driver.Value), []driver.Value{longDouble}, t, "results not equal for decimal array at row 4")

	// Fifth row
	err = rows.Next(dest)
	utils.AssertEqual(err, nil, t, "Next shouldn't return an error at row 5")
	if !math.IsNaN(float64(dest[2].(float32))) {
		t.Log(string(debug.Stack()))
		t.Errorf("results not equal for float32 Expected: NaN Got: %s", dest[2])
	}
	utils.AssertEqual(dest[9].(time.Time), time.Date(1989, 4, 15, 3, 2, 3, 123456000, loc), t, " at row 5")
	utils.AssertEqual(dest[13], nil, t, "results not equal for boolean at row 5")
	utils.AssertEqual(dest[14], nil, t, "results not equal for decimal at row 5")
	utils.AssertEqual(dest[15].([]driver.Value), []driver.Value{nil}, t, "results not equal for decimal array at row 5")

	// Sixth row (does not exist)
	utils.AssertEqual(io.EOF, rows.Next(dest), t, "Next should return io.EOF if no data available anymore at row 6")

	// Seventh row (does not exist)
	utils.AssertEqual(io.EOF, rows.Next(dest), t, "Next should return io.EOF if no data available anymore at row 7")

	utils.AssertEqual(rows.HasNextResultSet(), false, t, "Has Next result set didn't return false")
	utils.AssertEqual(rows.NextResultSet(), io.EOF, t, "Next result set didn't return false at the end")
}

// testRowsNextSet check rows with multiple statements
func testRowsNextSet(t *testing.T, rowsFactory func(isMultiStatement bool) driver.RowsNextResultSet) {
	rows := rowsFactory(true)

	// check next result set functions
	utils.AssertEqual(rows.HasNextResultSet(), true, t, "HasNextResultSet returned false, but shouldn't")
	utils.AssertEqual(rows.NextResultSet(), nil, t, "NextResultSet returned an error, but shouldn't")
	utils.AssertEqual(rows.HasNextResultSet(), false, t, "HasNextResultSet returned true, but shouldn't")

	// check columns of the next result set
	if !reflect.DeepEqual(rows.Columns(), []string{"int_col"}) {
		t.Errorf("Columns of the next result set are incorrect")
	}

	// check values of the next result set
	var dest = make([]driver.Value, 1)

	utils.AssertEqual(rows.Next(dest), nil, t, "Next shouldn't return an error")
	utils.AssertEqual(dest[0], int32(3), t, "results are not equal for int32")

	utils.AssertEqual(rows.Next(dest), nil, t, "Next shouldn't return an error")
	utils.AssertEqual(dest[0], nil, t, "results are not equal for final row")

	utils.AssertEqual(io.EOF, rows.Next(dest), t, "Next should return io.EOF if no data available anymore")
}

func testRowsNextStructError(t *testing.T, rowsFactory func(isMultiStatement bool) driver.RowsNextResultSet) {
	rowsJson := `{
        "query":{"query_id":"16FF2A0300ECA753"},
        "meta":[{"name":"struct_col","type":"struct(a int, s struct(b timestamp))"}],
        "data":[[{"a": 1, "s": {"b": "invalid"}}]],
        "rows":1,
        "statistics":{}
    }`
	var response types.QueryResponse
	if err := json.Unmarshal([]byte(rowsJson), &response); err != nil {
		panic(err)
	}

	rows := &InMemoryRows{[]types.QueryResponse{response}, 0, 0}
	var dest = make([]driver.Value, 1)
	if err := rows.Next(dest); err == nil {
		t.Errorf("Next should return an error")
	}
}

func testRowsNextStructWithNestedSpaces(t *testing.T, rowsFactory func(isMultiStatement bool) driver.RowsNextResultSet) {
	rowsJson := `{
        "query":{"query_id":"16FF2A0300ECA753"},
        "meta":[{"name":"struct_col","type":"struct(` + "`a b`" + ` int, s struct(` + "`c d`" + ` timestamp))"}],
        "data":[[{"a b": 1, "s": {"c d": "1989-04-15 01:02:03"}}]],
        "rows":1,
        "statistics":{}
    }`
	var response types.QueryResponse
	if err := json.Unmarshal([]byte(rowsJson), &response); err != nil {
		panic(err)
	}

	rows := &InMemoryRows{[]types.QueryResponse{response}, 0, 0}
	var dest = make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Errorf(nextErrorMessage, err)
	}

	if dest[0].(map[string]driver.Value)["a b"] != int32(1) {
		t.Errorf("results are not equal for struct field with a space in name")
	}
	if dest[0].(map[string]driver.Value)["s"].(map[string]driver.Value)["c d"] != time.Date(1989, 04, 15, 1, 2, 3, 0, time.UTC) {
		t.Errorf("results are not equal for nested struct field with a space in name")
	}
}

func testRowsQuotedLong(t *testing.T, rowsFactorySingleValue func(interface{}, string) driver.RowsNextResultSet) {
	intRaw := "-9223372036854775808"
	intValue, _ := strconv.ParseInt(intRaw, 10, 64)
	rows := rowsFactorySingleValue(intRaw, "long")
	var dest = make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Errorf(nextErrorMessage, err)
	}
	utils.AssertEqual(dest[0], intValue, t, "results are not equal for long")
}

func testRowsDecimalType(t *testing.T, rowsFactorySingleValue func(interface{}, string) driver.RowsNextResultSet) {
	floatValue := 123456789.123456789
	stringValue := "1234567890123456789012345678901234567890"
	stringDecimalValue, _ := decimal.NewFromString(stringValue)
	cases := [][]interface{}{
		{nil, nil},
		{floatValue, decimal.NewFromFloat(floatValue)},
		{stringValue, stringDecimalValue},
	}
	for _, c := range cases {
		rows := rowsFactorySingleValue(c[0], "Decimal(10, 35)")
		var dest = make([]driver.Value, 1)
		if err := rows.Next(dest); err != nil {
			t.Errorf(nextErrorMessage, err)
		}
		utils.AssertEqual(dest[0], c[1], t, fmt.Sprintf("results are not equal for %v", c[0]))
	}
}
