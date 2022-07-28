package fireboltgosdk

import "testing"

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
