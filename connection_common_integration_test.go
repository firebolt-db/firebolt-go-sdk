//go:build integration || integration_v0
// +build integration integration_v0

package fireboltgosdk

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

const OPEN_CONNECTION_ERROR_MSG = "opening a connection failed unexpectedly"
const STATEMENT_ERROR_MSG = "firebolt statement failed with %v"
const NEXT_STATEMENT_ERROR_MSG = "Next() call returned false"
const SCAN_STATEMENT_ERROR_MSG = "firebolt rows Scan() call failed with %v"
const VALUES_ARE_NOT_EQUAL_ERROR_MSG = "values are not equal: %v and %v\n"
const RESULTS_ARE_NOT_EQUAL_ERROR_MSG = "results are not equal "

var longTestValue int = 350000000000 // default value

func init() {
	var err error
	longTestValueStr, exists := os.LookupEnv("LONG_TEST_VALUE")
	if exists {
		longTestValue, err = strconv.Atoi(longTestValueStr)
		if err != nil {
			infolog.Println(fmt.Errorf("failed to convert LONG_TEST_VALUE to int: %v", err))
		}
	}
}

// TestConnectionPrepareStatement, tests that prepare statement doesn't result into an error
func TestConnectionSetStatement(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.TODO(), "SET time_zone=America/New_York")
	assert(err, nil, t, "set time_zone returned an error, but shouldn't")

	_, err = conn.QueryContext(context.TODO(), "SELECT * FROM information_schema.tables")
	assert(err, nil, t, "query returned an error, but shouldn't")

}

// TestConnectionQuery checks simple SELECT 1 exec
func TestConnectionQueryWrong(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	if _, err = conn.ExecContext(context.TODO(), "SELECT wrong query"); err == nil {
		t.Errorf("wrong statement didn't return an error")
	}
}

// TestConnectionInsertQuery checks simple Insert works
func TestConnectionInsertQuery(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	createTableSQL := "CREATE FACT TABLE integration_tests (id INT, name STRING) PRIMARY INDEX id"
	deleteTableSQL := "DROP TABLE IF EXISTS integration_tests"
	insertSQL := "INSERT INTO integration_tests (id, name) VALUES (0, 'some_text')"

	if _, err = conn.ExecContext(context.TODO(), createTableSQL); err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}
	if _, err = conn.ExecContext(context.TODO(), insertSQL); err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}
	if _, err = conn.ExecContext(context.TODO(), deleteTableSQL); err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}
}

// TestConnectionQuery checks simple SELECT query
func TestConnectionQuery(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	sql := "SELECT -3213212 as \"const\", 2.3 as \"float\", 'some_text' as \"text\""
	rows, err := conn.QueryContext(context.TODO(), sql)
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	columnNames := []string{"const", "float", "text"}
	columns, err := rows.Columns()
	if err != nil {
		t.Errorf("columns returned an error, but shouldn't")
	}
	if !reflect.DeepEqual(columns, columnNames) {
		t.Errorf("column lists are not equal")
	}

	var i int32
	var f float64
	var s string
	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	err = rows.Scan(&i, &f, &s)
	if err != nil {
		t.Errorf("Next returned an error, but shouldn't")
	}
	assert(i, int32(-3213212), t, "dest[0] is not equal")
	assert(f, float64(2.3), t, "dest[1] is not equal")
	assert(s, "some_text", t, "dest[2] is not equal")

	assert(rows.Next(), false, t, "end of data didn't return io.EOF")
}

func TestConnectionQueryDate32Type(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "select '2004-07-09'::DATE")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest time.Time

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	if dest != time.Date(2004, 7, 9, 0, 0, 0, 0, loc) {
		t.Errorf("values are not equal: %v\n", dest)
	}
}

func TestConnectionQueryDecimalType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	rows, err := conn.QueryContext(context.TODO(), "SELECT cast (123.23 as NUMERIC (12,6))")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest float64

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	if dest != 123.23 {
		t.Errorf("values are not equal: %v\n", dest)
	}
}

func TestConnectionQueryDateTime64Type(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '1980-01-01 02:03:04.321321'::TIMESTAMPNTZ;")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest time.Time

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	if expected := time.Date(1980, 1, 1, 2, 3, 4, 321321000, loc); expected != dest {
		t.Errorf(VALUES_ARE_NOT_EQUAL_ERROR_MSG, dest, expected)
	}
}

func TestConnectionQueryPGDateType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	loc, _ := time.LoadLocation("UTC")

	// Value 0001-01-01 is outside of range of regular DATE
	rows, err := conn.QueryContext(context.TODO(), "SELECT '0001-01-01' :: PGDATE;")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest time.Time

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	if expected := time.Date(0001, 1, 1, 0, 0, 0, 0, loc); expected != dest {
		t.Errorf(VALUES_ARE_NOT_EQUAL_ERROR_MSG, dest, expected)
	}
}

func TestConnectionQueryTimestampNTZType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '0001-01-05 17:04:42.123456' :: TIMESTAMPNTZ;")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest time.Time

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	if expected := time.Date(0001, 1, 5, 17, 4, 42, 123456000, loc); expected != dest {
		t.Errorf(VALUES_ARE_NOT_EQUAL_ERROR_MSG, dest, expected)
	}
}

func TestConnectionQueryTimestampTZType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	loc, _ := time.LoadLocation("UTC")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '2023-01-05 17:04:42.1234 Europe/Berlin'::TIMESTAMPTZ;")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest time.Time

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	// Expected offset by 1 hour when converted to UTC
	expected := time.Date(2023, 1, 5, 16, 4, 42, 123400000, loc)
	if !dest.Equal(expected) {
		t.Errorf("values are not equal Expected: %v Got: %v\n", expected, dest)
	}
}

func TestConnectionQueryTimestampTZTypeAsia(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	if _, err = conn.ExecContext(context.Background(), "SET time_zone=Asia/Calcutta"); err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
		t.FailNow()
	}
	loc, _ := time.LoadLocation("Asia/Calcutta")

	rows, err := conn.QueryContext(context.TODO(), "SELECT '2023-01-05 17:04:42.123456 Europe/Berlin'::TIMESTAMPTZ;")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest time.Time

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	// Expected offset by 5:30 when converted to Asia/Calcutta
	expected := time.Date(2023, 1, 5, 21, 34, 42, 123456000, loc)
	if !dest.Equal(expected) {
		t.Errorf("%s date with half-timezone check failed Expected: %s Got: %s", err, expected, dest)
	}
}

func TestConnectionMultipleStatement(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	if rows, err := conn.QueryContext(context.TODO(), "SELECT -1; SELECT -2"); err != nil {
		t.Errorf("Query multistement returned err: %v", err)
	} else {

		var dest int32

		assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
		err = rows.Scan(&dest)
		assert(err, nil, t, "rows.Scan returned an error")
		assert(dest, int32(-1), t, RESULTS_ARE_NOT_EQUAL_ERROR_MSG)

		assert(rows.NextResultSet(), true, t, "NextResultSet returned false")
		assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
		err = rows.Scan(&dest)
		assert(err, nil, t, "rows.Scan returned an error")
		assert(dest, int32(-2), t, RESULTS_ARE_NOT_EQUAL_ERROR_MSG)

		assert(rows.NextResultSet(), false, t, "NextResultSet returned true")
		assert(rows.Next(), false, t, "Next returned true")
	}
}

func TestConnectionQueryBooleanType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	rows, err := conn.QueryContext(context.TODO(), "SELECT true, false, null::boolean;")
	if err != nil {
		t.Errorf("statement failed with %v", err)
	}

	var b1, b2 bool
	// Nil value can only be assigned to an interface{}
	var b3 interface{}

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&b1, &b2, &b3); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	assert(b1, true, t, RESULTS_ARE_NOT_EQUAL_ERROR_MSG)
	assert(b2, false, t, RESULTS_ARE_NOT_EQUAL_ERROR_MSG)
	assert(b3, nil, t, RESULTS_ARE_NOT_EQUAL_ERROR_MSG)
}

func TestConnectionQueryByteaType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	rows, err := conn.QueryContext(context.TODO(), "SELECT 'abc123'::bytea")
	if err != nil {
		t.Errorf("statement failed with %v", err)
	}

	var dest []byte

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	expected := []byte("abc123")
	if !bytes.Equal(dest, expected) {
		t.Errorf("Bytea type check failed Expected: %s Got: %s", expected, dest)
	}
}

func TestConnectionQueryGeographyType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	rows, err := conn.QueryContext(context.TODO(), "SELECT 'POINT(1 1)'::geography")
	if err != nil {
		t.Errorf("statement failed with %v", err)
	}

	var dest string

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	expected := "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F"
	if dest != expected {
		t.Errorf("Geography type check failed Expected: %s Got: %s", expected, dest)
	}
}

func TestLongQuery(t *testing.T) {
	var maxValue = longTestValue

	finished_in := make(chan time.Duration, 1)
	go func() {
		started := time.Now()
		db, err := sql.Open("firebolt", dsnMock)
		if err != nil {
			t.Errorf("failed unexpectedly with %v", err)
		}
		_, err = db.Query("SELECT checksum(*) FROM generate_series(1, ?)", maxValue)
		if err != nil {
			t.Errorf("failed to run long query %v", err)
		}
		finished_in <- time.Since(started)
	}()
	select {
	case elapsed := <-finished_in:
		if elapsed < 350*time.Second {
			t.Errorf("Expected execution time to be more than 350 sec but was %.2f sec", elapsed.Seconds())
		}
	case <-time.After(10 * time.Minute):
		t.Errorf("Long query didn't finish in 10 minutes")
	}
}
