//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
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
