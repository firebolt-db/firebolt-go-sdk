package fireboltgosdk

import "testing"

func TestExecStmt(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	client, err := Authenticate(username, password)
	if err != nil {
		t.Errorf("auth failed with %v", err)
	}

	stmt := fireboltStmt{client: client, query: "SELECT 1", engineUrl: engineUrl, databaseName: database}
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}
}

func TestExecWrongStmt(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	client, err := Authenticate(username, password)
	if err != nil {
		t.Errorf("auth failed with %v", err)
	}

	stmt := fireboltStmt{client: client, query: "INSERT INTO", engineUrl: engineUrl, databaseName: database}
	_, err = stmt.Exec(nil)
	if err == nil {
		t.Errorf("firebolt statement didn't fail, but should")
	}
}
