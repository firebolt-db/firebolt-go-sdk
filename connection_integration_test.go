//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime/debug"
	"testing"
	"time"
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
	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(pointers...); err != nil {
		t.Errorf("firebolt rows Scan failed with %v", err)
		t.FailNow()
	}

	assert(dest[0], int32(1), t, "int32 results are not equal")
	assert(dest[1], int64(2), t, "int64 results are not equal")
	// float is now alias for double so both 32 an 64 bit float values are converted to float64
	assert(dest[2], 0.333333, t, "float32 results are not equal")
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
	assert(dest[10], geEncoded, t, "geography results are not equal")
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

	assert(rows.Next(), true, t, NEXT_STATEMENT_ERROR_MSG)
	if err = rows.Scan(&dest); err != nil {
		t.Errorf(SCAN_STATEMENT_ERROR_MSG, err)
	}
	expected := "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F"
	if dest != expected {
		t.Errorf("Geography type check failed Expected: %s Got: %s", expected, dest)
	}
}
