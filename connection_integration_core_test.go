//go:build integration_core
// +build integration_core

package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"reflect"
	"runtime/debug"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/rows"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/utils"

	"github.com/shopspring/decimal"
)

func TestConnectionUseDatabase(t *testing.T) {
	tableName := "test_use_database"
	newDatabaseName := databaseMock + "_new"

	createTableSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS \"%s\" (id INT)", tableName)
	selectTableSQL := "SELECT table_name FROM information_schema.tables WHERE table_name = ?"
	useDatabaseSQL := fmt.Sprintf("USE DATABASE \"%s\"", databaseMock)
	useNewDatabaseSQL := fmt.Sprintf("USE DATABASE \"%s\"", newDatabaseName)

	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly: %v", err)
		t.FailNow()
	}
	// We need separate connections for the original database and the system engine
	// which are not affected by the USE command to clean up the resources properly
	original_conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("opening a second connection failed unexpectedly: %v", err)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), useDatabaseSQL)

	if err != nil {
		t.Errorf("use database statement failed with %v", err)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Errorf("create table statement failed with %v", err)
		t.FailNow()
	}
	defer original_conn.Exec("DROP TABLE " + tableName)

	rows, err := conn.QueryContext(context.Background(), selectTableSQL, tableName)
	if err != nil {
		t.Errorf("select statement failed with %v", err)
		t.FailNow()
	}
	if !rows.Next() {
		t.Errorf("table %s wasn't created", tableName)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), "CREATE DATABASE IF NOT EXISTS "+newDatabaseName)
	if err != nil {
		t.Errorf("create database statement failed with %v", err)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), useNewDatabaseSQL)
	if err != nil {
		t.Errorf("use database statement failed with %v", err)
		t.FailNow()
	}

	rows, err = conn.QueryContext(context.Background(), selectTableSQL, tableName)
	if err != nil {
		t.Errorf("select statement failed with %v", err)
		t.FailNow()
	}
	if rows.Next() {
		t.Errorf("use database statement didn't update the database")
		t.FailNow()
	}
}

func TestConnectionUppercaseNames(t *testing.T) {
	databaseName := databaseMock + "_UPPERCASE"
	createDatabaseSQL := fmt.Sprintf("CREATE DATABASE \"%s\"", databaseName)
	dropDatabaseSQL := fmt.Sprintf("DROP DATABASE \"%s\"", databaseName)

	systemConnection, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("opening a system connection failed unexpectedly %v", err)
		t.FailNow()
	}

	_, err = systemConnection.Exec(createDatabaseSQL)
	if err != nil {
		t.Errorf("creating a database failed unexpectedly %v", err)
		t.FailNow()
	}
	defer systemConnection.Exec(dropDatabaseSQL)

	dsnUppercase := fmt.Sprintf(
		"firebolt:///%s?url=%s",
		databaseName, urlMock,
	)

	conn, err := sql.Open("firebolt", dsnUppercase)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly")
		t.FailNow()
	}

	_, err = conn.Exec("SELECT 1")
	if err != nil {
		t.Errorf("query failed with %v", err)
		t.FailNow()
	}
}

func TestConnectionUseDatabaseEngine(t *testing.T) {
	createTableSQL := "CREATE TABLE IF NOT EXISTS test_use (id INT)"
	insertSQL := "INSERT INTO test_use VALUES (1)"
	useDatabaseSQL := fmt.Sprintf("USE DATABASE \"%s\"", databaseMock)

	conn, err := sql.Open("firebolt", dsnNoDatabaseMock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly")
		t.FailNow()
	}

	_, err = conn.Exec(createTableSQL)
	if err != nil {
		t.Errorf("create table failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec(insertSQL)
	if err != nil {
		t.Errorf("insert failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec(useDatabaseSQL)
	if err != nil {
		t.Errorf("use database failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec(createTableSQL)
	if err != nil {
		t.Errorf("create table failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec(insertSQL)
	if err != nil {
		t.Errorf("insert failed with %v", err)
		t.FailNow()
	}
}

func TestConnectionPreparedStatement(t *testing.T) {
	utils.RunClientAndServerPreparedStatements(t, func(t *testing.T, ctx context.Context) {
		conn, err := sql.Open("firebolt", dsnMock)
		if err != nil {
			t.Errorf(OPEN_CONNECTION_ERROR_MSG)
			t.FailNow()
		}

		_, err = conn.QueryContext(
			ctx,
			"DROP TABLE IF EXISTS test_prepared_statements",
		)
		if err != nil {
			t.Errorf("drop table statement failed with %v", err)
			t.FailNow()
		}

		_, err = conn.QueryContext(
			ctx,
			"CREATE TABLE test_prepared_statements (i INT, l LONG, f FLOAT, d DOUBLE, t TEXT, dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ, b BOOLEAN, ba BYTEA, ge GEOGRAPHY) PRIMARY INDEX i",
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
		ge := "POINT(1 1)"
		geEncoded := "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F"

		var sql string
		if contextUtils.GetPreparedStatementsStyle(ctx) == contextUtils.PreparedStatementsStyleNative {
			sql = "INSERT INTO test_prepared_statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		} else {
			sql = "INSERT INTO test_prepared_statements VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
		}

		_, err = conn.QueryContext(
			ctx,
			sql,
			1, int64(2), 0.333333, 0.333333333333, "text", d, ts, tstz, true, ba, ge,
		)
		if err != nil {
			t.Errorf("insert statement failed with %v", err)
			t.FailNow()
		}

		_, err = conn.QueryContext(ctx, "SET timezone=Europe/Berlin")
		if err != nil {
			t.Errorf("set time_zone statement failed with %v", err)
			t.FailNow()
		}

		rows, err := conn.QueryContext(
			ctx,
			"SELECT * FROM test_prepared_statements",
		)
		if err != nil {
			t.Errorf("select statement failed with %v", err)
			t.FailNow()
		}

		dest := make([]driver.Value, 11)
		pointers := make([]interface{}, 11)
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
		utils.AssertEqual(dest[2], 0.333333, t, "float32 results are not equal")
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
		utils.AssertEqual(dest[10], geEncoded, t, "geography results are not equal")
	})
}

func TestConnectionQueryGeographyType(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	rows, err := conn.QueryContext(context.TODO(), "SELECT 'POINT(1 1)'::geography")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest string

	utils.AssertEqual(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	expected := "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F"
	if dest != expected {
		t.Errorf("Geography type check failed Expected: %s Got: %s", expected, dest)
	}
}

func queryStruct(t *testing.T) *sql.Rows {
	setupSQL := []string{
		"SET advanced_mode=1",
		"SET enable_struct_syntax=true",
		"DROP TABLE IF EXISTS test_struct",
		"DROP TABLE IF EXISTS test_struct_helper",
		"CREATE TABLE IF NOT EXISTS test_struct(id int not null, s struct(a array(int) null, b datetime null) not null)",
		"CREATE TABLE IF NOT EXISTS test_struct_helper(a array(int) null, b datetime null)",
		"INSERT INTO test_struct_helper(a, b) VALUES ([1, 2], '2019-07-31 01:01:01')",
		"INSERT INTO test_struct(id, s) SELECT 1, test_struct_helper FROM test_struct_helper",
	}
	tearDownSQL := []string{
		"DROP TABLE IF EXISTS test_struct",
		"DROP TABLE IF EXISTS test_struct_helper",
	}

	connection, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}
	for _, sql := range setupSQL {
		_, err = connection.ExecContext(context.Background(), sql)
		if err != nil {
			t.Errorf("setup failed with %v", err)
			t.FailNow()
		}
	}
	for _, sql := range tearDownSQL {
		defer connection.ExecContext(context.Background(), sql)
	}

	rows, err := connection.QueryContext(context.Background(), "SELECT test_struct FROM test_struct")
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
		t.FailNow()
	}
	return rows
}

func TestConnectionQueryStructType(t *testing.T) {
	rows := queryStruct(t)

	var dest map[string]driver.Value

	utils.AssertEqual(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err := rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}

	utils.AssertEqual(dest, map[string]driver.Value{
		"id": int32(1),
		"s": map[string]driver.Value{
			"a": []driver.Value{int32(1), int32(2)},
			"b": time.Date(2019, 7, 31, 1, 1, 1, 0, time.UTC),
		},
	}, t, "struct type check failed")

}

func TestConnectionStructTypeScannable(t *testing.T) {
	rows := queryStruct(t)

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Errorf("ColumnTypes failed with %v", err)
		t.FailNow()
	}

	dest := reflect.New(colTypes[0].ScanType()).Interface()

	utils.AssertEqual(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err := rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}

	utils.AssertEqual(dest, map[string]driver.Value{
		"id": int32(1),
		"s": map[string]driver.Value{
			"a": []driver.Value{int32(1), int32(2)},
			"b": time.Date(2019, 7, 31, 1, 1, 1, 0, time.UTC),
		},
	}, t, "struct type was scanned incorrectly")

}

func TestConnectionQuotedDecimal(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	sql := "SELECT 12345678901234567890123456789.123456789::decimal(38, 9)"

	res, err := conn.QueryContext(context.TODO(), sql)
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest driver.Value

	utils.AssertEqual(res.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = res.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	expected, _ := decimal.NewFromString("12345678901234567890123456789.123456789")
	if !expected.Equal(dest.(*rows.FireboltDecimal).Decimal) {
		t.Errorf("Quoted decimal check failed Expected: %s Got: %s", expected, dest)
	}
}

func getExpectedColumnTypes(isStreaming bool) []columnType {
	res := []columnType{
		{"col_int", "int", reflect.TypeOf(int32(0)), true, false, false, 0, false, 0, 0},
		{"col_int_null", "int", reflect.TypeOf(sql.NullInt32{}), true, true, false, 0, false, 0, 0},
		{"col_long", "long", reflect.TypeOf(int64(0)), true, false, false, 0, false, 0, 0},
		{"col_long_null", "long", reflect.TypeOf(sql.NullInt64{}), true, true, false, 0, false, 0, 0},
		{"col_float", "float", reflect.TypeOf(float32(0)), true, false, false, 0, false, 0, 0},
		{"col_float_null", "float", reflect.TypeOf(sql.NullFloat64{}), true, true, false, 0, false, 0, 0},
		{"col_double", "double", reflect.TypeOf(float64(0)), true, false, false, 0, false, 0, 0},
		{"col_double_null", "double", reflect.TypeOf(sql.NullFloat64{}), true, true, false, 0, false, 0, 0},
		{"col_text", "text", reflect.TypeOf(""), true, false, true, math.MaxInt64, false, 0, 0},
		{"col_text_null", "text", reflect.TypeOf(sql.NullString{}), true, true, true, math.MaxInt64, false, 0, 0},
		{"col_date", "date", reflect.TypeOf(time.Time{}), true, false, false, 0, false, 0, 0},
		{"col_date_null", "date", reflect.TypeOf(sql.NullTime{}), true, true, false, 0, false, 0, 0},
		{"col_timestamp", "timestamp", reflect.TypeOf(time.Time{}), true, false, false, 0, false, 0, 0},
		{"col_timestamp_null", "timestamp", reflect.TypeOf(sql.NullTime{}), true, true, false, 0, false, 0, 0},
		{"col_timestamptz", "timestamptz", reflect.TypeOf(time.Time{}), true, false, false, 0, false, 0, 0},
		{"col_timestamptz_null", "timestamptz", reflect.TypeOf(sql.NullTime{}), true, true, false, 0, false, 0, 0},
		{"col_boolean", "boolean", reflect.TypeOf(true), true, false, false, 0, false, 0, 0},
		{"col_boolean_null", "boolean", reflect.TypeOf(sql.NullBool{}), true, true, false, 0, false, 0, 0},
		{"col_array", "array(int)", reflect.TypeOf(rows.FireboltArray{}), true, false, true, math.MaxInt64, false, 0, 0},
		{"col_array_null", "array(int)", reflect.TypeOf(rows.FireboltNullArray{}), true, true, true, math.MaxInt64, false, 0, 0},
		{"col_decimal", "Decimal(38, 30)", reflect.TypeOf(rows.FireboltDecimal{}), true, false, false, 0, true, 38, 30},
		{"col_decimal_null", "Decimal(38, 30)", reflect.TypeOf(rows.FireboltNullDecimal{}), true, true, false, 0, true, 38, 30},
		{"col_bytea", "bytea", reflect.TypeOf([]byte{}), true, false, true, math.MaxInt64, false, 0, 0},
		{"col_bytea_null", "bytea", reflect.TypeOf(rows.NullBytes{}), true, true, true, math.MaxInt64, false, 0, 0},
		{"col_geography", "geography", reflect.TypeOf(""), true, false, false, 0, false, 0, 0},
		{"col_geography_null", "geography", reflect.TypeOf(sql.NullString{}), true, true, false, 0, false, 0, 0},
	}
	// Some types are returned by different alias from database when streaming
	if isStreaming {
		res[0].DatabaseTypeName = "integer"
		res[1].DatabaseTypeName = "integer"
		res[2].DatabaseTypeName = "bigint"
		res[3].DatabaseTypeName = "bigint"
		res[4].DatabaseTypeName = "real"
		res[5].DatabaseTypeName = "real"
		res[6].DatabaseTypeName = "double precision"
		res[7].DatabaseTypeName = "double precision"
		res[18].DatabaseTypeName = "array(integer)"
		res[19].DatabaseTypeName = "array(integer)"
		res[20].DatabaseTypeName = "numeric(38, 30)"
		res[21].DatabaseTypeName = "numeric(38, 30)"
	}
	return res
}

const allTypesSQLPath = "fixtures/all_types_query.sql"

func TestResponseMetadata(t *testing.T) {
	testResponseMetadata(t, allTypesSQLPath, func(ctx context.Context) []columnType { return getExpectedColumnTypes(contextUtils.IsStreaming(ctx)) })
}

func TestTypesScannable(t *testing.T) {
	testTypesScannable(t, allTypesSQLPath)
}

func TestConnectionSetStatement(t *testing.T) {
	testConnectionSetStatement(t, "timezone")
}
