package statement

import "testing"

// TestResult check, that the dummy FireboltResult doesn't return errors
func TestResult(t *testing.T) {
	res := FireboltResult{}
	if _, err := res.LastInsertId(); err != nil {
		t.Errorf("Result LastInsertId failed with %v", err)
	}
	if _, err := res.RowsAffected(); err != nil {
		t.Errorf("Result RowsAffected failed with %v", err)
	}
}
