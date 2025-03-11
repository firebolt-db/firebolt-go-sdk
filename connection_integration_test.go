//go:build integration
// +build integration

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

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/utils"

	"github.com/shopspring/decimal"
)

func TestConnectionUseDatabase(t *testing.T) {
	tableName := "test_use_database"
	createTableSQL := "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT)"
	selectTableSQL := "SELECT table_name FROM information_schema.tables WHERE table_name = ?"
	useDatabaseSQL := "USE DATABASE "
	newDatabaseName := databaseMock + "_new"

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
	system_conn, err := sql.Open("firebolt", dsnSystemEngineWithDatabaseMock)
	if err != nil {
		t.Errorf("opening a system connection failed unexpectedly: %v", err)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), useDatabaseSQL+databaseMock)

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
	defer system_conn.Exec("DROP DATABASE " + newDatabaseName)

	_, err = conn.ExecContext(context.Background(), useDatabaseSQL+newDatabaseName)
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
	systemConnection, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("opening a system connection failed unexpectedly %v", err)
		t.FailNow()
	}

	engineName := engineNameMock + "_UPPERCASE"
	databaseName := databaseMock + "_UPPERCASE"

	_, err = systemConnection.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", databaseName))
	if err != nil {
		t.Errorf("creating a database failed unexpectedly %v", err)
		t.FailNow()
	}
	defer systemConnection.Exec(fmt.Sprintf("DROP DATABASE \"%s\"", databaseName))
	_, err = systemConnection.Exec(fmt.Sprintf("CREATE ENGINE \"%s\"", engineName))
	if err != nil {
		t.Errorf("creating an engine failed unexpectedly %v", err)
		t.FailNow()
	}
	defer systemConnection.Exec(fmt.Sprintf("DROP ENGINE \"%s\"", engineName))
	// defers run in reverse order so we stop the engine before dropping it
	defer systemConnection.Exec(fmt.Sprintf("STOP ENGINE \"%s\"", engineName))

	dsnUppercase := fmt.Sprintf(
		"firebolt:///%s?account_name=%s&engine=%s&client_id=%s&client_secret=%s",
		databaseName, accountName, engineName, clientIdMock, clientSecretMock,
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

	const createTableSQL = "CREATE TABLE IF NOT EXISTS test_use (id INT)"
	const insertSQL = "INSERT INTO test_use VALUES (1)"
	const insertSQL2 = "INSERT INTO test_use VALUES (2)"

	conn, err := sql.Open("firebolt", dsnSystemEngineMock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly")
		t.FailNow()
	}

	_, err = conn.Exec(createTableSQL)
	if err == nil {
		t.Errorf("create table worked on a system engine without a database, while it shouldn't")
		t.FailNow()
	}

	_, err = conn.Exec(fmt.Sprintf("USE DATABASE \"%s\"", databaseMock))
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
	if err == nil {
		t.Errorf("insert worked on a system engine, while it shouldn't")
		t.FailNow()
	}

	_, err = conn.Exec(fmt.Sprintf("USE ENGINE \"%s\"", engineNameMock))
	if err != nil {
		t.Errorf("use engine failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec(insertSQL)
	if err != nil {
		t.Errorf("insert failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec("USE ENGINE system")
	if err != nil {
		t.Errorf("use engine failed with %v", err)
		t.FailNow()
	}

	_, err = conn.Exec(insertSQL2)
	if err == nil {
		t.Errorf("insert worked on a system engine, while it shouldn't")
		t.FailNow()
	}
}

func TestConnectionPreparedStatement(t *testing.T) {
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

	_, err = conn.QueryContext(
		context.Background(),
		"INSERT INTO test_prepared_statements VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		1, int64(2), 0.333333, 0.333333333333, "text", d, ts, tstz, true, ba, ge,
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

func TestConnectionQueryStructType(t *testing.T) {
	setupSQL := []string{
		"SET advanced_mode=1",
		"SET enable_create_table_v2=true",
		"SET enable_struct_syntax=true",
		"SET prevent_create_on_information_schema=true",
		"SET enable_create_table_with_struct_type=true",
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

	var dest map[string]driver.Value

	utils.AssertEqual(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
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

func TestConnectionQuotedDecimal(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf(OPEN_CONNECTION_ERROR_MSG)
		t.FailNow()
	}

	sql := "SELECT 12345678901234567890123456789.123456789::decimal(38, 9)"

	rows, err := conn.QueryContext(context.TODO(), sql)
	if err != nil {
		t.Errorf(STATEMENT_ERROR_MSG, err)
	}

	var dest driver.Value

	utils.AssertEqual(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	expected, _ := decimal.NewFromString("12345678901234567890123456789.123456789")
	if !expected.Equal(dest.(decimal.Decimal)) {
		t.Errorf("Quoted decimal check failed Expected: %s Got: %s", expected, dest)
	}
}

type columnType struct {
	Name              string
	DatabaseTypeName  string
	ScanType          reflect.Type
	HasNullable       bool
	Nullable          bool
	HasLength         bool
	Length            int64
	HasPrecisionScale bool
	Precision         int64
	Scale             int64
}

func getExpectedColumnTypes(isStreaming bool) []columnType {
	res := []columnType{
		{"col_int", "int", reflect.TypeOf(int32(0)), true, false, false, 0, false, 0, 0},
		{"col_long", "long", reflect.TypeOf(int64(0)), true, false, false, 0, false, 0, 0},
		{"col_float", "float", reflect.TypeOf(float32(0)), true, false, false, 0, false, 0, 0},
		{"col_double", "double", reflect.TypeOf(float64(0)), true, false, false, 0, false, 0, 0},
		{"col_text", "text", reflect.TypeOf(""), true, false, true, math.MaxInt64, false, 0, 0},
		{"col_date", "date", reflect.TypeOf(time.Time{}), true, false, false, 0, false, 0, 0},
		{"col_timestamp", "timestamp", reflect.TypeOf(time.Time{}), true, false, false, 0, false, 0, 0},
		{"col_timestamptz", "timestamptz", reflect.TypeOf(time.Time{}), true, false, false, 0, false, 0, 0},
		{"col_boolean", "boolean", reflect.TypeOf(true), true, false, false, 0, false, 0, 0},
		{"col_array", "array(int)", reflect.TypeOf([]int32{}), true, false, true, math.MaxInt64, false, 0, 0},
		{"col_decimal", "Decimal(38, 30)", reflect.TypeOf(decimal.Decimal{}), true, false, false, 0, true, 38, 30},
		{"col_bytea", "bytea", reflect.TypeOf([]byte{}), true, false, true, math.MaxInt64, false, 0, 0},
		{"col_geography", "geography", reflect.TypeOf(""), true, false, false, 0, false, 0, 0},
		{"col_nullable", "text", reflect.TypeOf(""), true, true, true, math.MaxInt64, false, 0, 0},
	}
	// Some types are returned by different alias from database when streaming
	if isStreaming {
		res[0].DatabaseTypeName = "integer"
		res[1].DatabaseTypeName = "bigint"
		res[2].DatabaseTypeName = "real"
		res[3].DatabaseTypeName = "double precision"
		res[9].DatabaseTypeName = "array(integer)"
		res[10].DatabaseTypeName = "numeric(38, 30)"
	}
	return res
}

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
       'point(1 2)'::geography                                   as col_geography,
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
