package fireboltgosdk

import "testing"

func TestResult(t *testing.T) {
	res := FireboltResult{""}
	id, _ := res.LastInsertId()
	if id != 0 {
		t.Errorf("got %d want %d", id, 0)
	}
}
