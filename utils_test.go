package fireboltgosdk

import (
	"database/sql/driver"
	"testing"
	"time"
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
	runParseSetStatementSuccess(t, "SET advanced_mode=1", "advanced_mode", "1")
	runParseSetStatementSuccess(t, "     SET advanced_mode=1  ", "advanced_mode", "1")
	runParseSetStatementSuccess(t, "     SET      advanced_mode      =       1  ", "advanced_mode", "1")
}

func TestParseSetStatementFail(t *testing.T) {
	runParseSetStatementFail(t, "SELECT 1")
	runParseSetStatementFail(t, "SET")
	runParseSetStatementFail(t, "SET ==")
	runParseSetStatementFail(t, "SET =1")
	runParseSetStatementFail(t, "SET advanced_mode=")
}

func runPrepareStatementSuccess(t *testing.T, query string, params []driver.Value, expected string) {

	var namedParams []driver.NamedValue
	for _, param := range params {
		namedParams = append(namedParams, driver.NamedValue{Value: param})
	}

	res, err := prepareStatement(query, namedParams)
	if res != expected {
		t.Errorf("expected string is not equal to result '%s' != '%s'", expected, res)
	}
	if err != nil {
		t.Errorf("function returned an error, but it shouldn't: %v", err)
	}
}

func runPrepareStatementFail(t *testing.T, query string, params []driver.Value) {
	var namedParams []driver.NamedValue
	for _, param := range params {
		namedParams = append(namedParams, driver.NamedValue{Value: param})
	}

	_, err := prepareStatement(query, namedParams)
	if err == nil {
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
	loc, _ := time.LoadLocation("UTC")

	runTestFormatValue(t, "", "''")
	runTestFormatValue(t, "abcd", "'abcd'")
	runTestFormatValue(t, "test\\", "'test\\\\'")
	runTestFormatValue(t, "test' OR '1' == '1", "'test\\' OR \\'1\\' == \\'1'")

	runTestFormatValue(t, 1, "1")
	runTestFormatValue(t, 1.123456, "1.123456")
	runTestFormatValue(t, true, "1")
	runTestFormatValue(t, false, "0")
	runTestFormatValue(t, -10, "-10")
	runTestFormatValue(t, time.Date(2022, 01, 10, 1, 3, 2, 0, loc), "'2022-01-10 01:03:02'")

	// not passing, but should: runTestFormatValue(t, 1.1234567, "1.1234567")
	// not passing, but should: runTestFormatValue(t, 1.123, "1.123")

}
