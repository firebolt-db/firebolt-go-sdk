//go:build integration_v0
// +build integration_v0

package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"runtime/debug"
	"testing"
	"time"
)

func TestConnectionPreparedStatementV0(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	_, err = conn.QueryContext(
		context.Background(),
		"DROP TABLE IF EXISTS test_prepared_statements",
	)
	if err != nil {
		t.Errorf("drop table statement failed with %v", err)
		t.FailNow()
	}

	_, err = conn.QueryContext(
		context.Background(),
		"CREATE TABLE test_prepared_statements (i INT, l LONG, f FLOAT, d DOUBLE, t TEXT, dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, b BOOLEAN, ba BYTEA) PRIMARY INDEX i",
	)
	if err != nil {
		t.Errorf("create table statement failed with %v", err)
		t.FailNow()
	}

	loc, _ := time.LoadLocation("Europe/Berlin")

	d := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	ts := time.Date(2021, 1, 1, 2, 10, 20, 3000, time.UTC)
	tstz := time.Date(2021, 1, 1, 2, 10, 20, 3000, loc)
	ba := []byte("hello_world_123ツ\n\u0048")

	_, err = conn.QueryContext(
		context.Background(),
		"INSERT INTO test_prepared_statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		1, int64(2), 0.333333, 0.333333333333, "text", d, ts, tstz, true, ba,
	)

	if err != nil {
		t.Errorf("insert statement failed with %v", err)
		t.FailNow()
	}

	_, err = conn.QueryContext(context.Background(), "SET time_zone=Europe/Berlin")
	if err != nil {
		t.Errorf("set time_zone statement failed with %v", err)
		t.FailNow()
	}

	rows, err := conn.QueryContext(
		context.Background(),
		"SELECT * FROM test_prepared_statements",
	)
	if err != nil {
		t.Errorf("select statement failed with %v", err)
		t.FailNow()
	}

	dest := make([]driver.Value, 10)
	pointers := make([]interface{}, 10)
	for i := range pointers {
		pointers[i] = &dest[i]
	}
	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(pointers...); err != nil {
		t.Errorf("firebolt rows Scan failed with %v", err)
		t.FailNow()
	}

	assert(dest[0], int32(1), t, "int32 results are not equal")
	assert(dest[1], int64(2), t, "int64 results are not equal")
	// float is now alias for double so both 32 an 64 bit float values are converted to float64
	assert(dest[2], float32(0.333333), t, "float32 results are not equal")
	assert(dest[3], 0.333333333333, t, "float64 results are not equal")
	assert(dest[4], "text", t, "string results are not equal")
	assert(dest[5], d, t, "date results are not equal")
	assert(dest[6], ts.UTC(), t, "timestamp results are not equal")
	// Use .Equal to correctly compare timezones
	if !dest[7].(time.Time).Equal(tstz) {
		t.Errorf("timestamptz results are not equal Expected: %s Got: %s", tstz, dest[7])
	}
	assert(dest[8], true, t, "boolean results are not equal")
	baValue := dest[9].([]byte)
	if len(baValue) != len(ba) {
		t.Log(string(debug.Stack()))
		t.Errorf("bytea results are not equal Expected length: %d Got: %d", len(ba), len(baValue))
	}
	for i := range ba {
		if ba[i] != baValue[i] {
			t.Log(string(debug.Stack()))
			t.Errorf("bytea results are not equal Expected: %s Got: %s", ba, baValue)
			break
		}
	}
}
