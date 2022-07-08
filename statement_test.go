package fireboltgosdk

import (
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
)

// TestExecStmt checks simple SELECT 1 exec
func TestExecStmt(t *testing.T) {
	markIntegrationTest(t)

	stmt := fireboltStmt{client: clientMock, query: "SELECT 1", engineUrl: engineUrlMock, databaseName: databaseMock}
	if _, err := stmt.Exec(nil); err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}
}

// TestExecWrongStmt checks, that an error is returned on wrong query
func TestExecWrongStmt(t *testing.T) {
	markIntegrationTest(t)

	stmt := fireboltStmt{client: clientMock, query: "INSERT INTO", engineUrl: engineUrlMock, databaseName: databaseMock}
	if _, err := stmt.Exec(nil); err == nil {
		t.Errorf("firebolt statement didn't fail, but should")
	}
}

// TestQueryStmt checks simple SELECT query
func TestQueryStmt(t *testing.T) {
	markIntegrationTest(t)

	stmt := fireboltStmt{client: clientMock,
		query:        "SELECT 3213212 as \"const\", 2.3 as \"float\", 'some_text' as \"text\"",
		engineUrl:    engineUrlMock,
		databaseName: databaseMock,
	}
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	columnNames := []string{"const", "float", "text"}
	if !reflect.DeepEqual(rows.Columns(), columnNames) {
		t.Errorf("column lists are not equal")
	}

	dest := make([]driver.Value, 3)
	err = rows.Next(dest)
	if err != nil {
		t.Errorf("Next returned an error, but shouldn't")
	}
	assert(dest[0] == uint32(3213212), t, "dest[0] is not equal")
	assert(dest[1] == float64(2.3), t, "dest[1] is not equal")
	assert(dest[2] == "some_text", t, "dest[2] is not equal")

	assert(rows.Next(dest) == io.EOF, t, "end of data didn't return io.EOF")
}
