//go:build integration_v0
// +build integration_v0

package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime/debug"
	"testing"
	"time"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
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
	utils.AssertEqual(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(pointers...); err != nil {
		t.Errorf("firebolt rows Scan failed with %v", err)
		t.FailNow()
	}

	utils.AssertEqual(dest[0], int32(1), t, "int32 results are not equal")
	utils.AssertEqual(dest[1], int64(2), t, "int64 results are not equal")
	// float is now alias for double so both 32 an 64 bit float values are converted to float64
	utils.AssertEqual(dest[2], float32(0.333333), t, "float32 results are not equal")
	utils.AssertEqual(dest[3], 0.333333333333, t, "float64 results are not equal")
	utils.AssertEqual(dest[4], "text", t, "string results are not equal")
	utils.AssertEqual(dest[5], d, t, "date results are not equal")
	utils.AssertEqual(dest[6], ts.UTC(), t, "timestamp results are not equal")
	// Use .Equal to correctly compare timezones
	if !dest[7].(time.Time).Equal(tstz) {
		t.Errorf("timestamptz results are not equal Expected: %s Got: %s", tstz, dest[7])
	}
	utils.AssertEqual(dest[8], true, t, "boolean results are not equal")
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

// TestResponseMetadata is the same as for V2 but without new types (like geography)
func TestResponseMetadata(t *testing.T) {
	const selectAllTypesSQL = `
       select 1                                                  as col_int,
       30000000000                                               as col_long,
       1.23::FLOAT4                                              as col_float,
       1.23456789012                                             as col_double,
       'text'                                                    as col_text,
       '2021-03-28'::date                                        as col_date,
       '2019-07-31 01:01:01'::timestamp                          as col_timestamp,
       '1111-01-05 17:04:42.123456'::timestamptz                 as col_timestamptz,
       true                                                      as col_boolean,
       [1,2,3,4]                                                 as col_array,
       '1231232.123459999990457054844258706536'::decimal(38, 30) as col_decimal,
       'abc123'::bytea                                           as col_bytea,
       null                                                      as col_nullable;`

	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		expectedColumnTypes := getExpectedColumnTypes(contextUtils.IsStreaming(ctx))

		conn, err := sql.Open("firebolt", dsnMock)
		if err != nil {
			t.Errorf(OPEN_CONNECTION_ERROR_MSG)
			t.FailNow()
		}

		rows, err := conn.QueryContext(ctx, selectAllTypesSQL)
		if err != nil {
			t.Errorf(STATEMENT_ERROR_MSG, err)
			t.FailNow()
		}

		if !rows.Next() {
			t.Errorf("Next() call returned false with error: %v", rows.Err())
			t.FailNow()
		}

		types, err := rows.ColumnTypes()
		if err != nil {
			t.Errorf("ColumnTypes returned an error, but shouldn't")
			t.FailNow()
		}

		for i, ct := range types {
			utils.AssertEqual(ct.Name(), expectedColumnTypes[i].Name, t, fmt.Sprintf("column name is not equal for column %s", ct.Name()))
			utils.AssertEqual(ct.DatabaseTypeName(), expectedColumnTypes[i].DatabaseTypeName, t, fmt.Sprintf("database type name is not equal for column %s", ct.Name()))
			utils.AssertEqual(ct.ScanType(), expectedColumnTypes[i].ScanType, t, fmt.Sprintf("scan type is not equal for column %s", ct.Name()))
			nullable, ok := ct.Nullable()
			utils.AssertEqual(ok, expectedColumnTypes[i].HasNullable, t, fmt.Sprintf("nullable ok is not equal for column %s", ct.Name()))
			utils.AssertEqual(nullable, expectedColumnTypes[i].Nullable, t, fmt.Sprintf("nullable is not equal for column %s", ct.Name()))
			length, ok := ct.Length()
			utils.AssertEqual(ok, expectedColumnTypes[i].HasLength, t, fmt.Sprintf("length ok is not equal for column %s", ct.Name()))
			utils.AssertEqual(length, expectedColumnTypes[i].Length, t, fmt.Sprintf("length is not equal for column %s", ct.Name()))
			precision, scale, ok := ct.DecimalSize()
			utils.AssertEqual(ok, expectedColumnTypes[i].HasPrecisionScale, t, fmt.Sprintf("precision scale ok is not equal for column %s", ct.Name()))
			utils.AssertEqual(precision, expectedColumnTypes[i].Precision, t, fmt.Sprintf("precision is not equal for column %s", ct.Name()))
			utils.AssertEqual(scale, expectedColumnTypes[i].Scale, t, fmt.Sprintf("scale is not equal for column %s", ct.Name()))
		}

	})
}
