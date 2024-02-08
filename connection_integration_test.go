//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"testing"
)

func TestConnectionUseDatabase(t *testing.T) {
	// tableName := "test_use_database"
	// createTableSQL := "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT)"
	// selectTableSQL := "SELECT table_name FROM information_schema.tables WHERE table_name = ?"
	useDatabaseSQL := "USE DATABASE "
	// newDatabaseName := databaseMock + "_new"

	conn, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Errorf("opening a connection failed unexpectedly")
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
