package rows

import (
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

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

	var responses []types.QueryResponse
	for i := 0; i < 2; i += 1 {
		if i != 0 && !isMultiStatement {
			break
		}
		var response types.QueryResponse
		if err := json.Unmarshal([]byte(resultJson[i]), &response); err != nil {
			panic(err)
		} else {
			responses = append(responses, response)
		}
	}

	return &InMemoryRows{responses, 0, 0}
}

func mockRowsSingleValue(value interface{}, columnType string) driver.RowsNextResultSet {
	response := types.QueryResponse{
		Query:      map[string]string{"query_id": "16FF2A0300ECA753"},
		Meta:       []types.Column{{Name: "single_col", Type: columnType}},
		Data:       [][]interface{}{{value}},
		Rows:       1,
		Errors:     []types.ErrorDetails{},
		Statistics: map[string]interface{}{},
	}

	return &InMemoryRows{[]types.QueryResponse{response}, 0, 0}
}

// testRowsColumns checks, that correct column names are returned
func TestInMemoryRowsColumns(t *testing.T) {
	testRowsColumns(t, mockRows)
}

// testRowsClose checks Close method, and inability to use rows afterward
func TestInMemoryRowsClose(t *testing.T) {
	testRowsClose(t, mockRows)

}

// testRowsNext check Next method
func TestInMemoryRowsNext(t *testing.T) {
	testRowsNext(t, mockRows)
}

// testRowsNextSet check rows with multiple statements
func TestInMemoryRowsNextSet(t *testing.T) {
	testRowsNextSet(t, mockRows)
}

func TestInMemoryRowsNextStructError(t *testing.T) {
	testRowsNextStructError(t, mockRows)
}

func TestInMemoryRowsNextStructWithNestedSpaces(t *testing.T) {
	testRowsNextStructWithNestedSpaces(t, mockRows)
}

func TestInMemoryRowsQuotedLong(t *testing.T) {
	testRowsQuotedLong(t, mockRowsSingleValue)
}

func TestInMemoryRowsDecimalType(t *testing.T) {
	testRowsDecimalType(t, mockRowsSingleValue)
}
