//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"testing"
)

func setupEngineAndDatabase(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnSystemEngineV2Mock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly: %v", err)
		t.FailNow()
	}
	if _, err = conn.Exec("CREATE DATABASE IF NOT EXISTS " + databaseMock); err != nil {
		t.Errorf("creating a database failed unexpectedly: %v", err)
		t.FailNow()
	}
	if _, err = conn.Exec("CREATE ENGINE IF NOT EXISTS " + engineNameMock); err != nil {
		t.Errorf("creating an engine failed unexpectedly: %v", err)
		t.FailNow()
	}
}

func cleanupEngineAndDatabase(t *testing.T) {
	conn, err := sql.Open("firebolt", dsnSystemEngineV2Mock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly: %v", err)
		t.FailNow()
	}
	if _, err = conn.Exec("STOP ENGINE " + engineNameMock); err != nil {
		t.Errorf("stopping an engine failed unexpectedly: %v", err)
		t.FailNow()
	}
	if _, err = conn.Exec("DROP ENGINE IF EXISTS " + engineNameMock); err != nil {
		t.Errorf("dropping an engine failed unexpectedly: %v", err)
		t.FailNow()
	}
	if _, err = conn.Exec("DROP DATABASE IF EXISTS " + databaseMock); err != nil {
		t.Errorf("dropping a database failed unexpectedly: %v", err)
		t.FailNow()
	}
}

func TestConnectionUseDatabase(t *testing.T) {
	// tableName := "test_use_database"
	// createTableSQL := "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT)"
	// selectTableSQL := "SELECT table_name FROM information_schema.tables WHERE table_name = ?"
	useDatabaseSQL := "USE DATABASE "
	// newDatabaseName := databaseMock + "_new"

	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly: %v", err)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), useDatabaseSQL+databaseMock)
	if err == nil {
		t.Errorf("use database works on a user engine. The test can be enabled")
		t.FailNow()
	}

	/*if err != nil {
		t.Errorf("use database statement failed with %v", err)
		t.FailNow()
	}

	_, err = conn.ExecContext(context.Background(), createTableSQL)
	if err != nil {
		t.Errorf("create table statement failed with %v", err)
		t.FailNow()
	}
	defer conn.Exec(useDatabaseSQL + databaseMock + "; DROP TABLE " + tableName)

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
	defer conn.Exec(useDatabaseSQL + databaseMock + "; DROP DATABASE " + newDatabaseName)

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
	}*/
}

func TestConnectionV2(t *testing.T) {
	setupEngineAndDatabase(t)
	defer cleanupEngineAndDatabase(t)

	conn, err := sql.Open("firebolt", dsnV2Mock)
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

func TestConnectionV2UseDatabaseEngine(t *testing.T) {
	setupEngineAndDatabase(t)
	defer cleanupEngineAndDatabase(t)

	const createTableSQL = "CREATE TABLE IF NOT EXISTS test_use (id INT)"
	const insertSQL = "INSERT INTO test_use VALUES (1)"
	const insertSQL2 = "INSERT INTO test_use VALUES (2)"

	conn, err := sql.Open("firebolt", dsnSystemEngineV2Mock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly")
		t.FailNow()
	}

	_, err = conn.Exec(createTableSQL)
	if err == nil {
		t.Errorf("create table worked on a system engine without a database, while it shouldn't")
		t.FailNow()
	}

	_, err = conn.Exec("USE DATABASE " + databaseMock)
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

	_, err = conn.Exec("USE ENGINE " + engineNameMock)
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
