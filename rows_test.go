package fireboltgosdk

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"reflect"
	"testing"
	"time"
)

func assert(val bool, t *testing.T, err string) {
	if !val {
		t.Errorf(err)
	}
}

func mockRows(isMultiStatement bool) driver.RowsNextResultSet {
	resultJson := []string{"{" +
		"\"query\":{\"query_id\":\"16FF2A0300ECA753\"}," +
		"\"meta\":[" +
		"	{\"name\":\"int_col\",\"type\":\"Nullable(Int32)\"}," +
		"	{\"name\":\"bigint_col\",\"type\":\"Int64\"}," +
		"	{\"name\":\"float_col\",\"type\":\"Float32\"}," +
		"	{\"name\":\"double_col\",\"type\":\"Float64\"}," +
		"	{\"name\":\"text_col\",\"type\":\"String\"}," +
		"	{\"name\":\"date_col\",\"type\":\"Date\"}," +
		"	{\"name\":\"timestamp_col\",\"type\":\"DateTime\"}," +
		"	{\"name\":\"bool_col\",\"type\":\"UInt8\"}," +
		"	{\"name\":\"array_col\",\"type\":\"Array(Int32)\"}," +
		"	{\"name\":\"nested_array_col\",\"type\":\"Array(Array(String))\"}]," +
		"\"data\":[" +
		"	[null,1,0.312321,123213.321321,\"text\", \"2080-12-31\",\"1989-04-15 01:02:03\",1,[1,2,3],[[]]]," +
		"	[2,1,0.312321,123213.321321,\"text\",\"1970-01-01\",\"1970-01-01 00:00:00\",1,[1,2,3],[[]]]," +
		"	[3,5,0.312321,123213.321321,\"text\",\"1970-01-01\",\"1970-01-01 00:00:00\",1,[5,2,3,2],[[\"TEST\",\"TEST1\"],[\"TEST3\"]]]]," +
		"\"rows\":3," +
		"\"statistics\":{" +
		"	\"elapsed\":0.001797702," +
		"	\"rows_read\":3," +
		"	\"bytes_read\":293," +
		"	\"time_before_execution\":0.001251613," +
		"	\"time_to_execute\":0.000544098," +
		"	\"scanned_bytes_cache\":2003," +
		"	\"scanned_bytes_storage\":0}}", "" +
		"{" +
		"\"query\":{\"query_id\":\"16FF2A0300ECA753\"}," +
		"\"meta\":[{\"name\":\"int_col\",\"type\":\"Nullable(Int32)\"}]," +
		"\"data\":[[3], [null]]," +
		"\"rows\":2," +
		"\"statistics\":{" +
		"	\"elapsed\":0.001797702," +
		"	\"rows_read\":2," +
		"	\"bytes_read\":293," +
		"	\"time_before_execution\":0.001251613," +
		"	\"time_to_execute\":0.000544098," +
		"	\"scanned_bytes_cache\":2003," +
		"	\"scanned_bytes_storage\":0}}"}

	var responses []QueryResponse
	for i := 0; i < 2; i += 1 {
		if i != 0 && !isMultiStatement {
			break
		}
		var response QueryResponse
		if err := json.Unmarshal([]byte(resultJson[i]), &response); err != nil {
			panic("Error in test code")
		} else {
			responses = append(responses, response)
		}
	}

	return &fireboltRows{responses, 0, 0}
}

// TestRowsColumns checks, that correct column names are returned
func TestRowsColumns(t *testing.T) {
	rows := mockRows(false)

	columnNames := []string{"int_col", "bigint_col", "float_col", "double_col", "text_col", "date_col", "timestamp_col", "bool_col", "array_col", "nested_array_col"}
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
	var dest = make([]driver.Value, 10)
	err := rows.Next(dest)
	loc, _ := time.LoadLocation("UTC")

	assert(err == nil, t, "Next shouldn't return an error")
	assert(dest[0] == nil, t, "results not equal for int32")
	assert(dest[1] == int64(1), t, "results not equal for int64")
	assert(dest[2] == float32(0.312321), t, "results not equal for float32")
	assert(dest[3] == float64(123213.321321), t, "results not equal for float64")
	assert(dest[4] == "text", t, "results not equal for string")
	assert(dest[5] == time.Date(2080, 12, 31, 0, 0, 0, 0, loc), t, "results not equal for date")
	assert(dest[6] == time.Date(1989, 04, 15, 1, 2, 3, 0, loc), t, "results not equal for datetime")

	err = rows.Next(dest)
	assert(err == nil, t, "Next shouldn't return an error")

	err = rows.Next(dest)
	assert(err == nil, t, "Next shouldn't return an error")

	assert(dest[0] == int32(3), t, "results not equal for int32")
	assert(dest[1] == int64(5), t, "results not equal for int64")
	assert(dest[2] == float32(0.312321), t, "results not equal for float32")
	assert(dest[3] == float64(123213.321321), t, "results not equal for float64")
	assert(dest[4] == "text", t, "results not equal for string")

	assert(io.EOF == rows.Next(dest), t, "Next should return io.EOF if no data available anymore")

	assert(rows.HasNextResultSet() == false, t, "Has Next result set didn't return false")
	assert(rows.NextResultSet() == io.EOF, t, "Next result set didn't return false")
}

// TestRowsNextSet check rows with multiple statements
func TestRowsNextSet(t *testing.T) {
	rows := mockRows(true)

	// check next result set functions
	assert(rows.HasNextResultSet() == true, t, "HasNextResultSet returned false, but shouldn't")
	assert(rows.NextResultSet() == nil, t, "NextResultSet returned an error, but shouldn't")
	assert(rows.HasNextResultSet() == false, t, "HasNextResultSet returned true, but shouldn't")

	// check columns of the next result set
	assert(reflect.DeepEqual(rows.Columns(), []string{"int_col"}), t, "Columns of the next result set are incorrect")

	// check values of the next result set
	var dest = make([]driver.Value, 1)

	assert(rows.Next(dest) == nil, t, "Next shouldn't return an error")
	assert(dest[0] == int32(3), t, "results are not equal")

	assert(rows.Next(dest) == nil, t, "Next shouldn't return an error")
	assert(dest[0] == nil, t, "results are not equal")

	assert(io.EOF == rows.Next(dest), t, "Next should return io.EOF if no data available anymore")
}
