package fireboltgosdk

import (
	"context"
	"database/sql/driver"
	"io"
	"reflect"
	"testing"
	"time"
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

func TestConnectionQueryDate32Type(t *testing.T) {
	markIntegrationTest(t)
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "select '2004-07-09 10:17:35'::DATE_EXT", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	if dest[0] != time.Date(2004, 7, 9, 0, 0, 0, 0, loc) {
		t.Errorf("values are not equal: %v\n", dest[0])
	}
}

func TestConnectionQueryDecimalType(t *testing.T) {
	markIntegrationTest(t)

	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{"firebolt_use_decimal": "1"}}

	rows, err := conn.QueryContext(context.TODO(), "SELECT cast (123.23 as NUMERIC (12,6))", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	if dest[0] != 123.23 {
		t.Errorf("values are not equal: %v\n", dest[0])
	}
}

func TestConnectionQueryDateTime64Type(t *testing.T) {
	markIntegrationTest(t)
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '1980/01/01 02:03:04.321321'::TIMESTAMP_EXT;", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	if expected := time.Date(1980, 1, 1, 2, 3, 4, 321321000, loc); expected != dest[0] {
		t.Errorf("values are not equal: %v and %v\n", dest[0], expected)
	}
}
