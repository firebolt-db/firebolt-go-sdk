//go:build integration
// +build integration

package fireboltgosdk

import (
	"context"
	"database/sql"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/rows"
)

type execer func(db *sql.DB, query string, args ...any) (rows.AsyncResult, error)

func execContext(db *sql.DB, query string, args ...any) (rows.AsyncResult, error) {
	asyncContext := context.Background() // Replace with actual context if needed
	return ExecAsyncContext(asyncContext, db, query, args...)
}

func execWithoutContext(db *sql.DB, query string, args ...any) (rows.AsyncResult, error) {
	return ExecAsync(db, query, args...)
}

func RunWithAndWithoutContext(t *testing.T, testCase func(*testing.T, execer)) {
	t.Run("WithContext", func(t *testing.T) { testCase(t, execContext) })
	t.Run("WithoutContext", func(t *testing.T) { testCase(t, execWithoutContext) })
}

func TestExecAsync(t *testing.T) {
	RunWithAndWithoutContext(t, func(t *testing.T, asyncExec execer) {
		db, err := sql.Open("firebolt", dsnMock)
		if err != nil {
			t.Fatalf("Failed to open database connection: %v", err)
		}

		dropTableSQL := "DROP TABLE IF EXISTS test_async_exec"
		createTableSQL := "CREATE TABLE IF NOT EXISTS test_async_exec (id INT, value TEXT)"
		insertSQL := "INSERT INTO test_async_exec (id, value) VALUES (1, 'test_value'), (2, 'another_value')"
		selectSQL := "SELECT * FROM test_async_exec"

		_, err = db.Exec(dropTableSQL)
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		_, err = db.Exec(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		token, err := asyncExec(db, insertSQL)
		if err != nil {
			t.Fatalf("Failed to execute async insert: %v", err)
		}
		for {
			running, err := IsAsyncQueryRunning(db, token)
			if err != nil {
				t.Fatalf("Failed to check async query status: %v", err)
			}
			if !running {
				break
			}
		}
		success, err := IsAsyncQuerySuccessful(db, token)
		if err != nil {
			t.Fatalf("Failed to check async query success: %v", err)
		}
		if !success {
			t.Fatalf("Async query did not complete successfully")
		}

		rows, err := db.Query(selectSQL)
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}
		defer func() { utils.Must(rows.Close()) }()

		// Validate that row count is 2
		count := 0
		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++
		}
		if count != 2 {
			t.Fatalf("Expected 2 rows, got %d", count)
		}
	})
}

func TestExecAsyncClientPreparedStatement(t *testing.T) {
	RunWithAndWithoutContext(t, func(t *testing.T, asyncExec execer) {
		db, err := sql.Open("firebolt", dsnMock)
		if err != nil {
			t.Fatalf("Failed to open database connection: %v", err)
		}
		dropTableSQL := "DROP TABLE IF EXISTS test_async_exec_client_prepared"
		createTableSQL := "CREATE TABLE IF NOT EXISTS test_async_exec_client_prepared (id INT, value TEXT)"
		insertSQL := "INSERT INTO test_async_exec_client_prepared (id, value) VALUES (?, ?)"
		selectSQL := "SELECT * FROM test_async_exec_client_prepared"

		_, err = db.Exec(dropTableSQL)
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		_, err = db.Exec(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		token, err := asyncExec(db, insertSQL, 1, "test_value")
		if err != nil {
			t.Fatalf("Failed to execute async insert: %v", err)
		}
		for {
			running, err := IsAsyncQueryRunning(db, token)
			if err != nil {
				t.Fatalf("Failed to check async query status: %v", err)
			}
			if !running {
				break
			}
		}
		success, err := IsAsyncQuerySuccessful(db, token)
		if err != nil {
			t.Fatalf("Failed to check async query success: %v", err)
		}
		if !success {
			t.Fatalf("Async query did not complete successfully")
		}

		rows, err := db.Query(selectSQL)
		if err != nil {
			t.Fatalf("Failed to query data: %v", err)
		}
		defer func() { utils.Must(rows.Close()) }()

		// Validate that row count is 2
		count := 0
		for rows.Next() {
			var id int
			var value string
			if err := rows.Scan(&id, &value); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			count++
		}
		if count != 1 {
			t.Fatalf("Expected 1 row, got %d", count)
		}
	})
}

func TestExecAsyncServerPreparedStatement(t *testing.T) {
	db, err := sql.Open("firebolt", dsnMock)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}
	dropTableSQL := "DROP TABLE IF EXISTS test_async_exec_client_prepared"
	createTableSQL := "CREATE TABLE IF NOT EXISTS test_async_exec_client_prepared (id INT, value TEXT)"
	insertSQL := "INSERT INTO test_async_exec_client_prepared (id, value) VALUES ($2, $1)"
	selectSQL := "SELECT * FROM test_async_exec_client_prepared"

	_, err = db.Exec(dropTableSQL)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	_, err = db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	preparedContext := contextUtils.WithPreparedStatementsStyle(
		context.Background(), contextUtils.PreparedStatementsStyleFbNumeric)

	token, err := ExecAsyncContext(preparedContext, db, insertSQL, "test_value", 1)
	if err != nil {
		t.Fatalf("Failed to execute async insert: %v", err)
	}
	for {
		running, err := IsAsyncQueryRunning(db, token)
		if err != nil {
			t.Fatalf("Failed to check async query status: %v", err)
		}
		if !running {
			break
		}
	}
	success, err := IsAsyncQuerySuccessful(db, token)
	if err != nil {
		t.Fatalf("Failed to check async query success: %v", err)
	}
	if !success {
		t.Fatalf("Async query did not complete successfully")
	}

	rows, err := db.Query(selectSQL)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer func() { utils.Must(rows.Close()) }()

	// Validate that row count is 2
	count := 0
	for rows.Next() {
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++
	}
	if count != 1 {
		t.Fatalf("Expected 1 row, got %d", count)
	}
}

func TestCancelAsyncQuery(t *testing.T) {
	RunWithAndWithoutContext(t, func(t *testing.T, asyncExec execer) {
		db, err := sql.Open("firebolt", dsnMock)
		if err != nil {
			t.Fatalf("Failed to open database connection: %v", err)
		}
		dropTableSQL := "DROP TABLE IF EXISTS test_async_cancel"
		createTableSQL := "CREATE TABLE IF NOT EXISTS test_async_cancel (id INT)"
		insertSQL := "INSERT INTO test_async_cancel (id) SELECT checksum(*) FROM GENERATE_SERIES(1, 2500000000)"

		_, err = db.Exec(dropTableSQL)
		if err != nil {
			t.Fatalf("Failed to drop table: %v", err)
		}

		_, err = db.Exec(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		token, err := asyncExec(db, insertSQL)
		if err != nil {
			t.Fatalf("Failed to execute async insert: %v", err)
		}

		running, err := IsAsyncQueryRunning(db, token)
		if err != nil {
			t.Fatalf("Failed to check async query status: %v", err)
		}
		if !running {
			t.Fatal("Expected async query to be running but it doesn't")
		}

		err = CancelAsyncQuery(db, token)
		if err != nil {
			t.Fatalf("Failed to cancel async query: %v", err)
		}

		running, err = IsAsyncQueryRunning(db, token)
		if err != nil {
			t.Fatalf("Failed to check async query status: %v", err)
		}
		if running {
			t.Fatal("Expected async query to be cancelled but it is still running")
		}

		success, err := IsAsyncQuerySuccessful(db, token)
		if err != nil {
			t.Fatalf("Failed to check async query success: %v", err)
		}
		if success {
			t.Fatal("Expected async query to be unsuccessful after cancellation, but it succeeded")
		}
	})
}
