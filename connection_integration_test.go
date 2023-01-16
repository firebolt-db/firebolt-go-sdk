//go:build integration
// +build integration

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
func TestConnectionSetStatement(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

	_, err := conn.ExecContext(context.TODO(), "SET use_standard_sql=1", nil)
	assert(err, nil, t, "set use_standard_sql returned an error, but shouldn't")

	_, err = conn.QueryContext(context.TODO(), "SELECT * FROM information_schema.tables", nil)
	assert(err, nil, t, "query returned an error, but shouldn't")

}

// TestConnectionQuery checks simple SELECT 1 exec
func TestConnectionQueryWrong(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

	if _, err := conn.ExecContext(context.TODO(), "SELECT wrong query", nil); err == nil {
		t.Errorf("wrong statement didn't return an error")
	}
}

// TestConnectionInsertQuery checks simple Insert works
func TestConnectionInsertQuery(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	createTableSQL := "CREATE FACT TABLE integration_tests (id INT, name STRING) PRIMARY INDEX id"
	deleteTableSQL := "DROP TABLE IF EXISTS integration_tests"
	insertSQL := "INSERT INTO integration_tests (id, name) VALUES (0, 'some_text')"

	if _, err := conn.ExecContext(context.TODO(), createTableSQL, nil); err != nil {
		t.Errorf("statement returned an error: %v", err)
	}
	if _, err := conn.ExecContext(context.TODO(), "SET firebolt_dont_wait_for_upload_to_s3=1", nil); err != nil {
		t.Errorf("statement returned an error: %v", err)
	}
	if _, err := conn.ExecContext(context.TODO(), insertSQL, nil); err != nil {
		t.Errorf("statement returned an error: %v", err)
	}
	if _, err := conn.ExecContext(context.TODO(), deleteTableSQL, nil); err != nil {
		t.Errorf("statement returned an error: %v", err)
	}
}

// TestConnectionQuery checks simple SELECT query
func TestConnectionQuery(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

	sql := "SELECT -3213212 as \"const\", 2.3 as \"float\", 'some_text' as \"text\""
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
	assert(dest[0], int32(-3213212), t, "dest[0] is not equal")
	assert(dest[1], float64(2.3), t, "dest[1] is not equal")
	assert(dest[2], "some_text", t, "dest[2] is not equal")

	assert(rows.Next(dest), io.EOF, t, "end of data didn't return io.EOF")
}

func TestConnectionQueryDate32Type(t *testing.T) {
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
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}

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

func TestConnectionQueryPGDateType(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	loc, _ := time.LoadLocation("UTC")

	// Value 0001-01-01 is outside of range of regular DATE
	rows, err := conn.QueryContext(context.TODO(), "SELECT '0001-01-01' :: PGDATE;", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	if expected := time.Date(0001, 1, 1, 0, 0, 0, 0, loc); expected != dest[0] {
		t.Errorf("values are not equal: %v and %v\n", dest[0], expected)
	}
}

func TestConnectionQueryTimestampNTZType(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '0001-01-05 17:04:42.123456' :: TIMESTAMPNTZ;", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	if expected := time.Date(0001, 1, 5, 17, 4, 42, 123456000, loc); expected != dest[0] {
		t.Errorf("values are not equal: %v and %v\n", dest[0], expected)
	}
}

func TestConnectionQueryTimestampTZType(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '2023-01-05 17:04:42.1234 Europe/Berlin'::TIMESTAMPTZ;", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	to_test, _ := dest[0].(time.Time)
	// Expected offset by 1 hour when converted to UTC
	expected := time.Date(2023, 1, 5, 16, 4, 42, 123400000, loc)
	if !to_test.Equal(expected) {
		t.Errorf("values are not equal Expected: %v Got: %v\n", expected, to_test)
	}
}

func TestConnectionQueryTimestampTZTypeAsia(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{"advanced_mode": "1", "time_zone": "Asia/Calcutta"}}
	loc, _ := time.LoadLocation("Asia/Calcutta")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '2023-01-05 17:04:42.123456 Europe/Berlin'::TIMESTAMPTZ;", nil)
	if err != nil {
		t.Errorf("firebolt statement failed with %v", err)
	}

	dest := make([]driver.Value, 1)

	if err = rows.Next(dest); err != nil {
		t.Errorf("firebolt rows Next failed with %v", err)
	}
	// Expected offset by 5:30 when converted to Asia/Calcutta
	expected := time.Date(2023, 1, 5, 21, 34, 42, 123456000, loc)
	to_test, _ := dest[0].(time.Time)
	if !to_test.Equal(expected) {
		t.Errorf("%s date with half-timezone check failed Expected: %s Got: %s", err, expected, to_test)
	}
}

func TestConnectionMultipleStatement(t *testing.T) {
	conn := fireboltConnection{clientMock, databaseMock, engineUrlMock, map[string]string{}}
	if rows, err := conn.QueryContext(context.TODO(), "SELECT -1; SELECT -2", nil); err != nil {
		t.Errorf("Query multistement returned err: %v", err)
	} else {
		dest := make([]driver.Value, 1)

		err = rows.Next(dest)
		assert(err, nil, t, "rows.Next returned an error")
		assert(dest[0], int32(-1), t, "results are not equal")

		if nextResultSet, ok := rows.(driver.RowsNextResultSet); !ok {
			t.Errorf("multistatement didn't return RowsNextResultSet")
		} else {
			if !nextResultSet.HasNextResultSet() {
				t.Errorf("HasNextResultSet returned false")
			}
			assert(nextResultSet.NextResultSet(), nil, t, "NextResultSet returned an error")

			err = rows.Next(dest)
			assert(err, nil, t, "rows.Next returned an error")
			assert(dest[0], int32(-2), t, "results are not equal")

			if nextResultSet.HasNextResultSet() {
				t.Errorf("HasNextResultSet returned true")
			}
		}
	}
}
