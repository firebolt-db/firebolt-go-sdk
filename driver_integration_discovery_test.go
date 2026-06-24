//go:build integration_discovery
// +build integration_discovery

package fireboltgosdk

import (
	"database/sql"
	"fmt"
	"testing"
)

var (
	dsnMock           string
	dsnNoDatabaseMock string
	databaseMock      string
)

func init() {
	databaseMock = "integration_test_db"
	dsnMock = fmt.Sprintf("firebolt://localhost:3473?database=%s&ssl_mode=none", databaseMock)
	dsnNoDatabaseMock = "firebolt://localhost:3473?ssl_mode=none"

	conn, err := sql.Open("firebolt", dsnNoDatabaseMock)
	if err != nil {
		panic(fmt.Sprintf("failed to open discovery connection: %v", err))
	}
	defer conn.Close()
	if _, err = conn.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", databaseMock)); err != nil {
		panic(fmt.Sprintf("failed to create database: %v", err))
	}
}

func TestDriverDiscoveryExecStatement(t *testing.T) {
	runTestDriverExecStatement(t, dsnMock)
}

func TestDriverDiscoveryOpenNoDatabase(t *testing.T) {
	runTestDriverExecStatement(t, dsnNoDatabaseMock)
}
