package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
)

// TestConnectionPrepareStatement, tests that prepare statement doesn't result into an error
func TestConnectionPrepareStatement(t *testing.T) {
	emptyClient := Client{}
	fireboltConnection := fireboltConnection{&emptyClient, "database_name", "engine_url", map[string]string{}}

	queryMock := "SELECT 1"
	_, err := fireboltConnection.Prepare(queryMock)
	if err != nil {
		t.Errorf("Prepare failed, but it shouldn't: %v", err)
	}
}

// TestConnectionClose, tests that connection close doesn't result an error
// and prepare statement on closed connection is not possible
func TestConnectionClose(t *testing.T) {
	emptyClient := Client{}
	fireboltConnection := fireboltConnection{&emptyClient, databaseMock, engineUrlMock, map[string]string{}}
	if err := fireboltConnection.Close(); err != nil {
		t.Errorf("Close failed with an err: %v", err)
	}

	_, err := fireboltConnection.Prepare("SELECT 1")
	if err == nil {
		t.Errorf("Prepare on closed connection didn't fail, but it should")
	}
}

// TestConnectionPrepareStatement, tests that prepare statement doesn't result into an error
func TestConnectionSetStatement(t *testing.T) {
	markIntegrationTest(t)
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

	_, err := conn.ExecContext(context.TODO(), "SET use_standard_sql=1", nil)
	assert(err == nil, t, "set use_standard_sql returned an error, but shouldn't")

	_, err = conn.QueryContext(context.TODO(), "SELECT * FROM information_schema.tables", nil)
	assert(err == nil, t, "query returned an error, but shouldn't")

	_, err = conn.ExecContext(context.TODO(), "SET use_standard_sql=0", nil)
	assert(err == nil, t, "set use_standard_sql returned an error, but shouldn't")

	_, err = conn.QueryContext(context.TODO(), "SELECT * FROM information_schema.tables", nil)
	assert(err != nil, t, "query didn't return an error, but should")
}

// TestConnectionQuery checks simple SELECT 1 exec
func TestConnectionQueryWrong(t *testing.T) {
	markIntegrationTest(t)
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

	if _, err := conn.ExecContext(context.TODO(), "SELECT wrong query", nil); err == nil {
		t.Errorf("wrong statement didn't return an error")
	}
}

// TestConnectionQuery checks simple SELECT query
func TestConnectionQuery(t *testing.T) {
	markIntegrationTest(t)
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

	sql := "SELECT 3213212 as \"const\", 2.3 as \"float\", 'some_text' as \"text\""
	rows, err := conn.QueryContext(context.TODO(), sql, nil)
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
