// Description: This file contains integration tests for selecting common data types.
// Every test selects 3 values of a specific data type from a table:
// 1. not null value for non-nullable column
// 2. not null value for nullable column
// 3. null value for nullable column
// And then validates that all values and their metadata are correct.

//go:build integration || integration_v0
// +build integration integration_v0

package fireboltgosdk

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/rows"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
	"github.com/shopspring/decimal"
)

// runSetupAndSelect runs the setup query and the query and returns the values and column types
func runSetupAndSelect(t *testing.T, ctx context.Context, setupQueries []string, query, cleanupQuery string) (interface{}, interface{}, interface{}, []*sql.ColumnType, func()) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	for _, q := range setupQueries {
		_, err = conn.ExecContext(ctx, q)
		if err != nil {
			t.Errorf("setup query '%v' failed: %v", q, err)
			t.FailNow()
		}
	}

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		t.Errorf("query failed: %v", err)
		t.FailNow()
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Errorf("column types failed: %v", err)
		t.FailNow()
	}

	if !rows.Next() {
		t.Errorf("no rows returned")
		t.FailNow()
	}

	defer rows.Close()

	val := reflect.New(colTypes[0].ScanType()).Interface()
	valNullNotNull := reflect.New(colTypes[1].ScanType()).Interface()
	valNullNull := reflect.New(colTypes[2].ScanType()).Interface()

	if err := rows.Scan(val, valNullNotNull, valNullNull); err != nil {
		t.Errorf(scanErrorMessage)
		t.FailNow()
	}

	cleanup := func() {
		_, err = conn.QueryContext(ctx, cleanupQuery)
		if err != nil {
			t.Errorf("cleanup query '%v' failed: %v", cleanupQuery, err)
			t.FailNow()
		}

		conn.Close()
	}

	return val, valNullNotNull, valNullNull, colTypes, cleanup
}

func TestSelectInt(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		i, iNullNotNull, iNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_int (i INT NOT NULL, i_n INT NULL, i_nn INT NULL) PRIMARY INDEX i",
				"INSERT INTO test_select_int VALUES (1, 2, null)",
			},
			"SELECT i, i_n, i_nn FROM test_select_int",
			"DROP TABLE test_select_int",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(int32(0)), t, "invalid scan type returned for int")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullInt32{}), t, "invalid scan type returned for nullable int")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullInt32{}), t, "invalid scan type returned for nullable int")

		utils.AssertEqual(*(i.(*int32)), int32(1), t, "invalid value returned for int")
		const errMsg = "invalid value returned for nullable int"
		utils.AssertEqual(iNullNotNull.(*sql.NullInt32).Valid, true, t, errMsg)
		utils.AssertEqual(iNullNotNull.(*sql.NullInt32).Int32, int32(2), t, errMsg)
		utils.AssertEqual(iNullNull.(*sql.NullInt32).Valid, false, t, errMsg)
	})
}

func TestSelectLong(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		l, lNullNotNull, lNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_long (l LONG NOT NULL, l_n LONG NULL, l_nn LONG NULL) PRIMARY INDEX l",
				"INSERT INTO test_select_long VALUES (1, 2, null)",
			},
			"SELECT l, l_n, l_nn FROM test_select_long",
			"DROP TABLE test_select_long",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(int64(0)), t, "invalid scan type returned for long")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullInt64{}), t, "invalid scan type returned for nullable long")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullInt64{}), t, "invalid scan type returned for nullable long")

		utils.AssertEqual(*(l.(*int64)), int64(1), t, "invalid value returned for long")
		const errMsg = "invalid value returned for nullable long"
		utils.AssertEqual(lNullNotNull.(*sql.NullInt64).Valid, true, t, errMsg)
		utils.AssertEqual(lNullNotNull.(*sql.NullInt64).Int64, int64(2), t, errMsg)
		utils.AssertEqual(lNullNull.(*sql.NullInt64).Valid, false, t, errMsg)
	})
}

func TestSelectFloat4(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		f, fNullNotNull, fNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_float4 (f FLOAT4 NOT NULL, f_n FLOAT4 NULL, f_nn FLOAT4 NULL) PRIMARY INDEX f",
				"INSERT INTO test_select_float4 VALUES (1, 2, null)",
			},
			"SELECT f, f_n, f_nn FROM test_select_float4",
			"DROP TABLE test_select_float4",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(float32(0)), t, "invalid scan type returned for float4")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullFloat64{}), t, "invalid scan type returned for nullable float4")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullFloat64{}), t, "invalid scan type returned for nullable float4")

		utils.AssertEqual(*(f.(*float32)), float32(1), t, "invalid value returned for float4")
		const errMsg = "invalid value returned for nullable float4"
		utils.AssertEqual(fNullNotNull.(*sql.NullFloat64).Valid, true, t, errMsg)
		utils.AssertEqual(fNullNotNull.(*sql.NullFloat64).Float64, float64(2), t, errMsg)
		utils.AssertEqual(fNullNull.(*sql.NullFloat64).Valid, false, t, errMsg)
	})
}

func TestSelectDouble(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		d, dNullNotNull, dNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_double (d DOUBLE NOT NULL, d_n DOUBLE NULL, d_nn DOUBLE NULL) PRIMARY INDEX d",
				"INSERT INTO test_select_double VALUES (1, 2, null)",
			},
			"SELECT d, d_n, d_nn FROM test_select_double",
			"DROP TABLE test_select_double",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(float64(0)), t, "invalid scan type returned for double")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullFloat64{}), t, "invalid scan type returned for nullable double")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullFloat64{}), t, "invalid scan type returned for nullable double")

		utils.AssertEqual(*(d.(*float64)), float64(1), t, "invalid value returned for double")
		const errMsg = "invalid value returned for nullable double"
		utils.AssertEqual(dNullNotNull.(*sql.NullFloat64).Valid, true, t, errMsg)
		utils.AssertEqual(dNullNotNull.(*sql.NullFloat64).Float64, float64(2), t, errMsg)
		utils.AssertEqual(dNullNull.(*sql.NullFloat64).Valid, false, t, errMsg)
	})
}

func TestSelectText(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		tt, ttNullNotNull, ttNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_text (t TEXT NOT NULL, t_n TEXT NULL, t_nn TEXT NULL) PRIMARY INDEX t",
				"INSERT INTO test_select_text VALUES ('a', 'b', null)",
			},
			"SELECT t, t_n, t_nn FROM test_select_text",
			"DROP TABLE test_select_text",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(""), t, "invalid scan type returned for text")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullString{}), t, "invalid scan type returned for nullable text")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullString{}), t, "invalid scan type returned for nullable text")

		utils.AssertEqual(*(tt.(*string)), "a", t, "invalid value returned for text")
		const errMsg = "invalid value returned for nullable text"
		utils.AssertEqual(ttNullNotNull.(*sql.NullString).Valid, true, t, errMsg)
		utils.AssertEqual(ttNullNotNull.(*sql.NullString).String, "b", t, errMsg)
		utils.AssertEqual(ttNullNull.(*sql.NullString).Valid, false, t, errMsg)
	})
}

func TestSelectDate(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		d, dNullNotNull, dNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_date (d DATE NOT NULL, d_n DATE NULL, d_nn DATE NULL) PRIMARY INDEX d",
				"INSERT INTO test_select_date VALUES ('2021-01-01', '2021-01-02', null)",
			},
			"SELECT d, d_n, d_nn FROM test_select_date",
			"DROP TABLE test_select_date",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(time.Time{}), t, "invalid scan type returned for date")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullTime{}), t, "invalid scan type returned for nullable date")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullTime{}), t, "invalid scan type returned for nullable date")

		utils.AssertEqual(*(d.(*time.Time)), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), t, "invalid value returned for date")
		const errMsg = "invalid value returned for nullable date"
		utils.AssertEqual(dNullNotNull.(*sql.NullTime).Valid, true, t, errMsg)
		utils.AssertEqual(dNullNotNull.(*sql.NullTime).Time, time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC), t, errMsg)
		utils.AssertEqual(dNullNull.(*sql.NullTime).Valid, false, t, errMsg)
	})
}

func TestSelectTimestamp(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		ts, tsNullNotNull, tsNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_timestamp (ts TIMESTAMP NOT NULL, ts_n TIMESTAMP NULL, ts_nn TIMESTAMP NULL) PRIMARY INDEX ts",
				"INSERT INTO test_select_timestamp VALUES ('2021-01-01 10:01:00', '2021-01-02 10:01:00', null)",
			},
			"SELECT ts, ts_n, ts_nn FROM test_select_timestamp",
			"DROP TABLE test_select_timestamp",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(time.Time{}), t, "invalid scan type returned for timestamp")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullTime{}), t, "invalid scan type returned for nullable timestamp")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullTime{}), t, "invalid scan type returned for nullable timestamp")

		utils.AssertEqual(*(ts.(*time.Time)), time.Date(2021, 1, 1, 10, 1, 0, 0, time.UTC), t, "invalid value returned for timestamp")
		const errMsg = "invalid value returned for nullable timestamp"
		utils.AssertEqual(tsNullNotNull.(*sql.NullTime).Valid, true, t, errMsg)
		utils.AssertEqual(tsNullNotNull.(*sql.NullTime).Time, time.Date(2021, 1, 2, 10, 1, 0, 0, time.UTC), t, errMsg)
		utils.AssertEqual(tsNullNull.(*sql.NullTime).Valid, false, t, errMsg)
	})
}

func TestSelectTimestamptz(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		tstz, tstzNullNotNull, tstzNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"SET time_zone=Europe/Berlin",
				"CREATE TABLE test_select_timestamptz (tstz TIMESTAMPTZ NOT NULL, tstz_n TIMESTAMPTZ NULL, tstz_nn TIMESTAMPTZ NULL) PRIMARY INDEX tstz",
				"INSERT INTO test_select_timestamptz VALUES ('2021-01-01 10:01:00', '2021-01-02 10:01:00', null)",
			},
			"SELECT tstz, tstz_n, tstz_nn FROM test_select_timestamptz",
			"DROP TABLE test_select_timestamptz",
		)

		defer cleanup()

		berlinTz, err := time.LoadLocation("Europe/Berlin")
		if err != nil {
			t.Errorf("failed to load Europe/Berlin timezone: %v", err)
			t.FailNow()
		}

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(time.Time{}), t, "invalid scan type returned for timestamptz")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullTime{}), t, "invalid scan type returned for nullable timestamptz")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullTime{}), t, "invalid scan type returned for nullable timestamptz")

		utils.AssertEqual(*(tstz.(*time.Time)), time.Date(2021, 1, 1, 10, 1, 0, 0, berlinTz), t, "invalid value returned for timestamptz")
		const errMsg = "invalid value returned for nullable timestamptz"
		utils.AssertEqual(tstzNullNotNull.(*sql.NullTime).Valid, true, t, errMsg)
		utils.AssertEqual(tstzNullNotNull.(*sql.NullTime).Time, time.Date(2021, 1, 2, 10, 1, 0, 0, berlinTz), t, errMsg)
		utils.AssertEqual(tstzNullNull.(*sql.NullTime).Valid, false, t, errMsg)
	})
}

func TestSelectBoolean(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		b, bNullNotNull, bNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_boolean (b BOOLEAN NOT NULL, b_n BOOLEAN NULL, b_nn BOOLEAN NULL) PRIMARY INDEX b",
				"INSERT INTO test_select_boolean VALUES (true, false, null)",
			},
			"SELECT b, b_n, b_nn FROM test_select_boolean",
			"DROP TABLE test_select_boolean",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(true), t, "invalid scan type returned for boolean")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(sql.NullBool{}), t, "invalid scan type returned for nullable boolean")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(sql.NullBool{}), t, "invalid scan type returned for nullable boolean")

		utils.AssertEqual(*(b.(*bool)), true, t, "invalid value returned for boolean")
		const errMsg = "invalid value returned for nullable boolean"
		utils.AssertEqual(bNullNotNull.(*sql.NullBool).Valid, true, t, errMsg)
		utils.AssertEqual(bNullNotNull.(*sql.NullBool).Bool, false, t, errMsg)
		utils.AssertEqual(bNullNull.(*sql.NullBool).Valid, false, t, errMsg)
	})
}

func TestSelectDecimal(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		d, dNullNotNull, dNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_decimal (d DECIMAL(38, 30) NOT NULL, d_n DECIMAL(38, 30) NULL, d_nn DECIMAL(38, 30) NULL) PRIMARY INDEX d",
				"INSERT INTO test_select_decimal VALUES ('1.1', '2.2', null)",
			},
			"SELECT d, d_n, d_nn FROM test_select_decimal",
			"DROP TABLE test_select_decimal",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(rows.FireboltDecimal{}), t, "invalid scan type returned for decimal")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(rows.FireboltNullDecimal{}), t, "invalid scan type returned for nullable decimal")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(rows.FireboltNullDecimal{}), t, "invalid scan type returned for nullable decimal")

		utils.AssertEqual(d.(*rows.FireboltDecimal).Decimal, decimal.NewFromFloat(1.1), t, "invalid value returned for decimal")
		const errMsg = "invalid value returned for nullable decimal"
		utils.AssertEqual(dNullNotNull.(*rows.FireboltNullDecimal).Valid, true, t, errMsg)
		utils.AssertEqual(dNullNotNull.(*rows.FireboltNullDecimal).Decimal, decimal.NewFromFloat(2.2), t, errMsg)
		utils.AssertEqual(dNullNull.(*rows.FireboltNullDecimal).Valid, false, t, errMsg)
	})
}

func TestSelectBytea(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		ba, baNullNotNull, baNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_bytea (ba BYTEA NOT NULL, ba_n BYTEA NULL, ba_nn BYTEA NULL) PRIMARY INDEX ba",
				"INSERT INTO test_select_bytea VALUES ('a', 'b', null)",
			},
			"SELECT ba, ba_n, ba_nn FROM test_select_bytea",
			"DROP TABLE test_select_bytea",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf([]byte{}), t, "invalid scan type returned for bytea")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(rows.NullBytes{}), t, "invalid scan type returned for nullable bytea")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(rows.NullBytes{}), t, "invalid scan type returned for nullable bytea")

		utils.AssertEqual(*(ba.(*[]byte)), []byte("a"), t, "invalid value returned for bytea")
		const errMsg = "invalid value returned for nullable bytea"
		utils.AssertEqual(baNullNotNull.(*rows.NullBytes).Valid, true, t, errMsg)
		utils.AssertEqual(baNullNotNull.(*rows.NullBytes).Bytes, []byte("b"), t, errMsg)
		utils.AssertEqual(baNullNull.(*rows.NullBytes).Valid, false, t, errMsg)
	})
}

func TestSelectArrayInt(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		ai, aiNullNotNull, aiNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_array_int (ai ARRAY(INT) NOT NULL, ai_n ARRAY(INT) NULL, ai_nn ARRAY(INT) NULL) PRIMARY INDEX ai",
				"INSERT INTO test_select_array_int VALUES ([1, 2], [3, 4], null)",
			},
			"SELECT ai, ai_n, ai_nn FROM test_select_array_int",
			"DROP TABLE test_select_array_int",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(rows.FireboltArray{}), t, "invalid scan type returned for array(int)")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(rows.FireboltNullArray{}), t, "invalid scan type returned for nullable array(int)")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(rows.FireboltNullArray{}), t, "invalid scan type returned for nullable array(int)")

		utils.AssertEqual(*(ai.(*rows.FireboltArray)), []int32{1, 2}, t, "invalid value returned for array(int)")
		const errMsg = "invalid value returned for nullable array(int)"
		utils.AssertEqual(aiNullNotNull.(*rows.FireboltNullArray).Valid, true, t, errMsg)
		utils.AssertEqual(aiNullNotNull.(*rows.FireboltNullArray).Array, []int32{3, 4}, t, errMsg)
		utils.AssertEqual(aiNullNull.(*rows.FireboltNullArray).Valid, false, t, errMsg)
	})
}

func TestSelectArrayArrayInt(t *testing.T) {
	utils.RunInMemoryAndStream(t, func(t *testing.T, ctx context.Context) {
		aai, aaiNullNotNull, aaiNullNull, colTypes, cleanup := runSetupAndSelect(
			t, ctx,
			[]string{
				"CREATE TABLE test_select_array_array_int (aai ARRAY(ARRAY(INT)) NOT NULL, aai_n ARRAY(ARRAY(INT)) NULL, aai_nn ARRAY(ARRAY(INT)) NULL) PRIMARY INDEX aai",
				"INSERT INTO test_select_array_array_int VALUES ([[1, 2], [3, 4]], [[5, 6], [7, 8]], null)",
			},
			"SELECT aai, aai_n, aai_nn FROM test_select_array_array_int",
			"DROP TABLE test_select_array_array_int",
		)

		defer cleanup()

		utils.AssertEqual(colTypes[0].ScanType(), reflect.TypeOf(rows.FireboltArray{}), t, "invalid scan type returned for array(array(int))")
		utils.AssertEqual(colTypes[1].ScanType(), reflect.TypeOf(rows.FireboltNullArray{}), t, "invalid scan type returned for nullable array(array(int))")
		utils.AssertEqual(colTypes[2].ScanType(), reflect.TypeOf(rows.FireboltNullArray{}), t, "invalid scan type returned for nullable array(array(int))")

		utils.AssertEqual(*(aai.(*rows.FireboltArray)), [][]int32{{1, 2}, {3, 4}}, t, "invalid value returned for array(array(int))")
		const errMsg = "invalid value returned for nullable array(array(int))"
		utils.AssertEqual(aaiNullNotNull.(*rows.FireboltNullArray).Valid, true, t, errMsg)
		utils.AssertEqual(aaiNullNotNull.(*rows.FireboltNullArray).Array, [][]int32{{5, 6}, {7, 8}}, t, errMsg)
		utils.AssertEqual(aaiNullNull.(*rows.FireboltNullArray).Valid, false, t, errMsg)
	})
}
