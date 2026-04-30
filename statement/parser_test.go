package statement

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
)

func runParseSetStatementSuccess(t *testing.T, value, expectedKey, expectedValue string) {
	key, value, err := parseSetStatement(value)
	if err != nil {
		t.Errorf("parseSetStatement returned an error, but shouldn't: %v", err)
	}

	if key != expectedKey {
		t.Errorf("parseSetStatement real and expected key are different '%s' != '%s'", key, expectedKey)
	}

	if value != expectedValue {
		t.Errorf("parseSetStatement real and expected value are different '%s' != '%s'", value, expectedValue)
	}
}

func runParseSetStatementFail(t *testing.T, value string) {
	_, _, err := parseSetStatement(value)
	if err == nil {
		t.Errorf("parseSetStatement didn't return an error for: %v", value)
	}
}

func TestParseSetStatementSuccess(t *testing.T) {
	runParseSetStatementSuccess(t, "SET time_zone=America/New_York", "time_zone", "America/New_York")
	runParseSetStatementSuccess(t, "     SET time_zone=America/New_York  ", "time_zone", "America/New_York")
	runParseSetStatementSuccess(t, "     SET      time_zone      =       America/New_York  ", "time_zone", "America/New_York")
	runParseSetStatementSuccess(t, "set time_zone=America/New_York", "time_zone", "America/New_York")
	runParseSetStatementSuccess(t, "sEt time_zone=America/New_York", "time_zone", "America/New_York")
}

func TestParseSetStatementFail(t *testing.T) {
	runParseSetStatementFail(t, "SELECT 1")
	runParseSetStatementFail(t, "SET")
	runParseSetStatementFail(t, "SET ==")
	runParseSetStatementFail(t, "SET =1")
	runParseSetStatementFail(t, "SET time_zone=")
}

func runPrepareStatementSuccess(t *testing.T, query string, params []driver.Value, expected string) {

	var namedParams []driver.NamedValue
	for _, param := range params {
		namedParams = append(namedParams, driver.NamedValue{Value: param})
	}

	positions, err := prepareStatement(query)
	if err != nil {
		t.Errorf("function returned an error, but it shouldn't: %v", err)
	}
	res, err := formatStatement(query, positions, namedParams)
	if err != nil {
		t.Errorf("function returned an error, but it shouldn't: %v", err)
	}
	if res != expected {
		t.Errorf("expected string is not equal to result '%s' != '%s'", expected, res)
	}
}

func runPrepareStatementFail(t *testing.T, query string, params []driver.Value) {
	var namedParams []driver.NamedValue
	for _, param := range params {
		namedParams = append(namedParams, driver.NamedValue{Value: param})
	}

	positions, err1 := prepareStatement(query)
	_, err2 := formatStatement(query, positions, namedParams)
	if err1 == nil && err2 == nil {
		t.Errorf("function didn't return an error, but it should")
	}

}

func TestPrepareStatement(t *testing.T) {
	runPrepareStatementSuccess(t, "select * from table", []driver.Value{}, "select * from table")
	runPrepareStatementSuccess(t, "select * from table where id == ?", []driver.Value{1}, "select * from table where id == 1")
	runPrepareStatementSuccess(t, "select * from table where id == '?'", []driver.Value{}, "select * from table where id == '?'")
	runPrepareStatementSuccess(t, "insert into table values (?, ?, '?')", []driver.Value{1, "1"}, "insert into table values (1, '1', '?')")
	runPrepareStatementSuccess(t, "select * from t where /*comment ?*/ id == ?", []driver.Value{"*/ 1 == 1 or /*"}, "select * from t where /*comment ?*/ id == '*/ 1 == 1 or /*'")
	runPrepareStatementSuccess(t, "select * from t where id == ?", []driver.Value{"' or '' == '"}, "select * from t where id == '\\' or \\'\\' == \\''")

	runPrepareStatementFail(t, "?", []driver.Value{})
	runPrepareStatementFail(t, "?", []driver.Value{1, 2})
}

func runTestFormatValue(t *testing.T, value driver.Value, expected string) {
	res, err := formatValue(value)
	if err != nil {
		t.Errorf("formatValue shouldn't return an error, but it did: %v", err)
	}
	if res != expected {
		t.Errorf("formatValue real and expected resutls are different: '%s' != '%s'", res, expected)
	}
}

func TestFormatValue(t *testing.T) {
	loc, _ := time.LoadLocation("Europe/Berlin")

	runTestFormatValue(t, "", "''")
	runTestFormatValue(t, "abcd", "'abcd'")
	runTestFormatValue(t, "test\\", "'test\\\\'")
	runTestFormatValue(t, "test' OR '1' == '1", "'test\\' OR \\'1\\' == \\'1'")

	runTestFormatValue(t, 1, "1")
	runTestFormatValue(t, 1.123, "1.123")
	runTestFormatValue(t, 1.123456, "1.123456")
	runTestFormatValue(t, 1.1234567, "1.1234567")
	runTestFormatValue(t, 1/float64(3), "0.3333333333333333")
	runTestFormatValue(t, true, "true")
	runTestFormatValue(t, false, "false")
	runTestFormatValue(t, -10, "-10")
	runTestFormatValue(t, nil, "NULL")
	runTestFormatValue(t, []byte("abcd"), "E'\\x61\\x62\\x63\\x64'")
	// Time
	runTestFormatValue(t, time.Date(2022, 01, 10, 1, 3, 2, 123000, time.UTC), "'2022-01-10 01:03:02.000123'")
	runTestFormatValue(t, time.Date(2022, 01, 10, 1, 3, 2, 123000, time.FixedZone("", 0)), "'2022-01-10 01:03:02.000123'")
	runTestFormatValue(t, time.Date(2022, 01, 10, 0, 0, 0, 0, time.UTC), "'2022-01-10'")
	runTestFormatValue(t, time.Date(2022, 01, 10, 0, 0, 0, 0, loc), "'2022-01-10 00:00:00.000000+01:00'")
	runTestFormatValue(t, time.Date(2022, 01, 10, 1, 3, 2, 123000, loc), "'2022-01-10 01:03:02.000123+01:00'")

}

func runSplitStatement(t *testing.T, value string, expected []string) {
	stmts, err := splitStatements(value)
	if err != nil {
		t.Errorf("splitStatements return an error for: %v", value)
	}

	if !reflect.DeepEqual(stmts, expected) {
		t.Errorf("splitStatements returned and expected are not equal: %v != %v", stmts, expected)
	}
}

func TestSplitStatements(t *testing.T) {
	runSplitStatement(t, "SELECT 1; SELECT 2;", []string{"SELECT 1", "SELECT 2"})
	runSplitStatement(t, "SELECT 1;", []string{"SELECT 1"})
	runSplitStatement(t, "SELECT 1; ", []string{"SELECT 1"})
	runSplitStatement(t, "SELECT 1", []string{"SELECT 1"})
	runSplitStatement(t, "SELECT 1; ; ; ; ", []string{"SELECT 1"})

	runSplitStatement(t, "SET time_zone=America/New_York; SELECT 2 /*some ; comment*/", []string{"SET time_zone=America/New_York", "SELECT 2 /*some ; comment*/"})
	runSplitStatement(t, "SET time_zone='America/New_York'; SELECT 2 /*some ; comment*/", []string{"SET time_zone='America/New_York'", "SELECT 2 /*some ; comment*/"})
	runSplitStatement(t, "SELECT 1; SELECT 2; SELECT 3; SELECT 4; SELECT 5; SELECT 6", []string{"SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4", "SELECT 5", "SELECT 6"})

	multistatement_with_line_endings := `
    SELECT 1;  

	SET a=b;   
	
	SELECT 2;   
    `
	// Make sure we trim line endings and spaces
	runSplitStatement(t, multistatement_with_line_endings, []string{"SELECT 1", "SET a=b", "SELECT 2"})
}

func runValidateSetStatementSuccess(t *testing.T, value string) {
	res := validateSetStatement(value)
	if res != nil {
		t.Errorf("validateSetStatement returned an error, but it shouldn't for: %s", value)
		return
	}
}

func runValidateSetStatementFailure(t *testing.T, value string, messagePart string) {
	res := validateSetStatement(value)
	if res == nil {
		t.Errorf("validateSetStatement didn't return an error for: %s", value)
	}
	if !strings.Contains(res.Error(), messagePart) {
		t.Errorf("validateSetStatement returned an error '%s', but it should contain '%s'", res.Error(), messagePart)
	}
}

func TestValidateSetStatement(t *testing.T) {
	runValidateSetStatementSuccess(t, "time_zone")
	runValidateSetStatementFailure(t, "engine", "Try again with 'USE ENGINE' instead of SET")
	runValidateSetStatementFailure(t, "database", "Try again with 'USE DATABASE' instead of SET")
	runValidateSetStatementFailure(t, "output_format", "Set parameter 'output_format' is not allowed")
}

func runPrepareQuerySuccess(t *testing.T, query string, style contextUtils.PreparedStatementsStyle, expected []PreparedQuery) {
	res, err := prepareQuery(query, style)
	if err != nil {
		t.Errorf("prepareQuery returned an error, but it shouldn't: %v", err)
	}
	for i, r := range res {
		// Use reflect to compare
		if !reflect.DeepEqual(r, expected[i]) {
			t.Errorf("prepareQuery result is not equal to expected at index %d: '%v' != '%v'", i, r, expected[i])
			return
		}
	}
}

func runPrepareQueryFail(t *testing.T, query string, style contextUtils.PreparedStatementsStyle) {
	res, err := prepareQuery(query, style)
	if err == nil {
		t.Errorf("prepareQuery should return an error for query '%s', but it didn't", query)
	}
	if res != nil {
		t.Errorf("prepareQuery should return nil for query '%s', but it returned: %v", query, res)
	}
}

func TestPrepareQuery(t *testing.T) {
	runPrepareQuerySuccess(t, "SELECT 1", contextUtils.PreparedStatementsStyleNative,
		[]PreparedQuery{&SingleStatement{
			query:           "SELECT 1",
			paramsPositions: nil,
			parametersStyle: contextUtils.PreparedStatementsStyleNative,
		}})
	runPrepareQuerySuccess(t, "SELECT 1;SELECT 2", contextUtils.PreparedStatementsStyleNative,
		[]PreparedQuery{
			&SingleStatement{
				query:           "SELECT 1",
				paramsPositions: nil,
				parametersStyle: contextUtils.PreparedStatementsStyleNative,
			},
			&SingleStatement{
				query:           "SELECT 2",
				paramsPositions: nil,
				parametersStyle: contextUtils.PreparedStatementsStyleNative,
			},
		})
	runPrepareQuerySuccess(t, "SELECT ?, ?", contextUtils.PreparedStatementsStyleNative,
		[]PreparedQuery{&SingleStatement{
			query:           "SELECT ?, ?",
			paramsPositions: []int{8, 11},
			parametersStyle: contextUtils.PreparedStatementsStyleNative,
		}})
	runPrepareQuerySuccess(t, "SET timezone=America/New_York", contextUtils.PreparedStatementsStyleNative,
		[]PreparedQuery{&SetStatement{
			key:   "timezone",
			value: "America/New_York",
		}})
	runPrepareQuerySuccess(t, "SET timezone=America/New_York;SELECT 1", contextUtils.PreparedStatementsStyleNative,
		[]PreparedQuery{
			&SetStatement{
				key:   "timezone",
				value: "America/New_York",
			},
			&SingleStatement{
				query:           "SELECT 1",
				paramsPositions: nil,
				parametersStyle: contextUtils.PreparedStatementsStyleNative,
			},
		})
	runPrepareQuerySuccess(t, "SELECT $1, $2", contextUtils.PreparedStatementsStyleFbNumeric,
		[]PreparedQuery{&SingleStatement{
			query:           "SELECT $1, $2",
			paramsPositions: nil,
			parametersStyle: contextUtils.PreparedStatementsStyleFbNumeric,
		}})
	runPrepareQueryFail(t, "SET engine=some_engine", contextUtils.PreparedStatementsStyleNative)

	runPrepareQuerySuccess(t, "SELECT 1;\n\nSET timezone=America/New_York;\n\nSELECT 2", contextUtils.PreparedStatementsStyleNative,
		[]PreparedQuery{
			&SingleStatement{
				query:           "SELECT 1",
				paramsPositions: nil,
				parametersStyle: contextUtils.PreparedStatementsStyleNative,
			},
			&SetStatement{
				key:   "timezone",
				value: "America/New_York",
			},
			&SingleStatement{
				query:           "SELECT 2",
				paramsPositions: nil,
				parametersStyle: contextUtils.PreparedStatementsStyleNative,
			},
		})
}

func TestFormatValueServerSideSQLNull(t *testing.T) {
	v, err := formatValueServerSide(nil)
	if err != nil {
		t.Fatalf("formatValueServerSide(nil): %v", err)
	}
	if v != nil {
		t.Fatalf("SQL NULL must use nil *string (JSON null), got %#v", v)
	}
}

func TestMakeQueryParametersSQLNullIsJSONNull(t *testing.T) {
	params, err := makeQueryParameters([]driver.NamedValue{
		{Ordinal: 1, Value: nil},
		{Ordinal: 2, Value: "public"},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, ok := params["query_parameters"]
	if !ok {
		t.Fatal("missing query_parameters")
	}
	var decoded []struct {
		Name  string  `json:"name"`
		Value *string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, raw)
	}
	if len(decoded) != 2 {
		t.Fatalf("len=%d, want 2: %s", len(decoded), raw)
	}
	if decoded[0].Name != "$1" || decoded[1].Name != "$2" {
		t.Fatalf("names: %+v", decoded)
	}
	if decoded[0].Value != nil {
		t.Fatalf("$1: expected JSON null for SQL NULL, got %#v", decoded[0].Value)
	}
	if decoded[1].Value == nil || *decoded[1].Value != "public" {
		t.Fatalf("$2: want public, got %+v", decoded[1].Value)
	}
}

func TestMakeQueryParametersNullByteString(t *testing.T) {
	nullByteString := "\x00"
	if len(nullByteString) != 1 || nullByteString[0] != 0 {
		t.Fatal("fixture must be a single NUL byte")
	}
	params, err := makeQueryParameters([]driver.NamedValue{{Ordinal: 1, Value: nullByteString}})
	if err != nil {
		t.Fatal(err)
	}
	raw := params["query_parameters"]
	if raw != `[{"name":"$1","value":"\u0000"}]` {
		t.Fatalf("unexpected JSON (want compact NUL escape), got: %s", raw)
	}
	var decoded []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[0].Value != nullByteString {
		t.Fatalf("round-trip: got %q want single NUL", decoded[0].Value)
	}
}

// Eight NUL code points in a Go string become \u0000 escapes in JSON — this is not SQL NULL
// (SQL NULL is encoded as JSON null via a nil *string). See formatValueServerSide / makeQueryParameters.
func TestMakeQueryParametersStringWithNULBytesJSONEscapes(t *testing.T) {
	eightNuls := string([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	params, err := makeQueryParameters([]driver.NamedValue{{Ordinal: 1, Value: eightNuls}})
	if err != nil {
		t.Fatal(err)
	}
	raw := params["query_parameters"]
	if !strings.Contains(raw, `\u0000`) {
		t.Fatalf("expected JSON to escape NUL as \\u0000, got: %s", raw)
	}
	var decoded []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded[0].Value != eightNuls {
		t.Fatal("round-trip string mismatch")
	}
}

// Documents the exact wire JSON when the bound Go string is eight NUL code points (debug aid).
func TestMakeQueryParametersEightNULJSONEscapeLiteral(t *testing.T) {
	eightNuls := string([]byte{0, 0, 0, 0, 0, 0, 0, 0})
	params, err := makeQueryParameters([]driver.NamedValue{{Ordinal: 1, Value: eightNuls}})
	if err != nil {
		t.Fatal(err)
	}
	// Backticks: each \u0000 is JSON escape (six ASCII chars), not a Go rune escape.
	want := `[{"name":"$1","value":"\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000"}]`
	if got := params["query_parameters"]; got != want {
		t.Fatalf("query_parameters mismatch.\nwant: %s\ngot:  %s", want, got)
	}
}

func TestMakeQueryParametersByteSliceZeroIsHexNotJSONNull(t *testing.T) {
	params, err := makeQueryParameters([]driver.NamedValue{{Ordinal: 1, Value: []byte{0, 0, 0, 0, 0, 0, 0, 0}}})
	if err != nil {
		t.Fatal(err)
	}
	raw := params["query_parameters"]
	if strings.Contains(raw, `\u0000`) {
		t.Fatalf("[]byte zeros must be hex-encoded, not raw NUL JSON escapes: %s", raw)
	}
	var decoded []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	want := `\x0000000000000000`
	if decoded[0].Value != want {
		t.Fatalf("got %q want %q", decoded[0].Value, want)
	}
}

func TestMakeQueryParametersRejectsNamedArguments(t *testing.T) {
	_, err := makeQueryParameters([]driver.NamedValue{{Name: "foo", Ordinal: 1, Value: 1}})
	if err == nil || !strings.Contains(err.Error(), "named parameters are not supported") {
		t.Fatalf("expected named-parameter error, got %v", err)
	}
}

func TestSingleStatementFormatFbNumericUsesQueryParameters(t *testing.T) {
	stmt := &SingleStatement{
		query:           "SELECT $1",
		paramsPositions: nil,
		parametersStyle: contextUtils.PreparedStatementsStyleFbNumeric,
	}
	sqlOut, extra, err := stmt.Format([]driver.NamedValue{{Ordinal: 1, Value: nil}})
	if err != nil {
		t.Fatal(err)
	}
	if sqlOut != "SELECT $1" {
		t.Fatalf("query: %q", sqlOut)
	}
	raw := extra["query_parameters"]
	var decoded []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("%+v", decoded)
	}
	// JSON null for value
	if string(decoded[0]["value"]) != "null" {
		t.Fatalf("value field: %s", decoded[0]["value"])
	}
}
