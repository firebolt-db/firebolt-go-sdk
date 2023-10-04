package fireboltgosdk

import (
	"database/sql/driver"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/matishsiao/goInfo"
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
	runParseSetStatementSuccess(t, "set advanced_mode=1", "advanced_mode", "1")
	runParseSetStatementSuccess(t, "sEt advanced_mode=1", "advanced_mode", "1")
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
	runTestFormatValue(t, true, "1")
	runTestFormatValue(t, false, "0")
	runTestFormatValue(t, -10, "-10")
	runTestFormatValue(t, nil, "NULL")
	runTestFormatValue(t, []byte("abcd"), "'\\x61\\x62\\x63\\x64'")
	// Timestamp is converted to UTC according to the provided loc value
	runTestFormatValue(t, time.Date(2022, 01, 10, 2, 3, 2, 123000, loc), "'2022-01-10 01:03:02.000123'")

}

func TestConstructUserAgentString(t *testing.T) {
	os.Setenv("FIREBOLT_GO_DRIVERS", "GORM/0.0.1")
	os.Setenv("FIREBOLT_GO_CLIENTS", "Client1/0.2.3 Client2/0.3.4")

	userAgentString := ConstructUserAgentString()

	if !strings.Contains(userAgentString, sdkVersion) {
		t.Errorf("sdk Version is not in userAgent string")
	}
	if !strings.Contains(userAgentString, "GoSDK") {
		t.Errorf("sdk name is not in userAgent string")
	}
	if !strings.Contains(userAgentString, "GORM/0.0.1") {
		t.Errorf("drivers is not in userAgent string")
	}
	if !strings.Contains(userAgentString, "Client1/0.2.3 Client2/0.3.4") {
		t.Errorf("clients are not in userAgent string")
	}

	os.Unsetenv("FIREBOLT_GO_DRIVERS")
	os.Unsetenv("FIREBOLT_GO_CLIENTS")
}

// FIR-25705
func TestConstructUserAgentStringFails(t *testing.T) {
	// Save current function and restore at the end
	old := goInfoFunc
	defer func() { goInfoFunc = old }()

	goInfoFunc = func() (goInfo.GoInfoObject, error) {
		// Simulate goinfo failing
		panic("Aaaaaaaaaa")
	}
	userAgentString := ConstructUserAgentString()

	if userAgentString != "GoSDK" {
		t.Errorf("UserAgent string was not generated correctly")
	}
}

func runSplitStatement(t *testing.T, value string, expected []string) {
	stmts, err := SplitStatements(value)
	if err != nil {
		t.Errorf("SplitStatements return an error for: %v", value)
	}

	if !reflect.DeepEqual(stmts, expected) {
		t.Errorf("SplitStatements returned and expected are not equal: %v != %v", stmts, expected)
	}
}

func TestSplitStatements(t *testing.T) {
	runSplitStatement(t, "SELECT 1; SELECT 2;", []string{"SELECT 1", " SELECT 2"})
	runSplitStatement(t, "SELECT 1;", []string{"SELECT 1"})
	runSplitStatement(t, "SELECT 1", []string{"SELECT 1"})
	runSplitStatement(t, "SELECT 1; ; ; ; ", []string{"SELECT 1", " ", " ", " ", " "})

	runSplitStatement(t, "SET advanced_mode=1; SELECT 2 /*some ; comment*/", []string{"SET advanced_mode=1", " SELECT 2 /*some ; comment*/"})
	runSplitStatement(t, "SET advanced_mode=';'; SELECT 2 /*some ; comment*/", []string{"SET advanced_mode=';'", " SELECT 2 /*some ; comment*/"})
	runSplitStatement(t, "SELECT 1; SELECT 2; SELECT 3; SELECT 4; SELECT 5; SELECT 6", []string{"SELECT 1", " SELECT 2", " SELECT 3", " SELECT 4", " SELECT 5", " SELECT 6"})
}

func TestValueToNamedValue(t *testing.T) {
	assert(len(valueToNamedValue([]driver.Value{})), 0, t, "valueToNamedValue of empty array is wrong")

	namedValues := valueToNamedValue([]driver.Value{2, "string"})
	assert(len(namedValues), 2, t, "len of namedValues is wrong")
	assert(namedValues[0].Value, 2, t, "namedValues value is wrong")
	assert(namedValues[1].Value, "string", t, "namedValues value is wrong")
}
