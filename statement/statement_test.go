package statement

import (
	"context"
	"database/sql/driver"
	"testing"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

type driverExecerMock struct {
	callCount int
	lastQuery []PreparedQuery
}

func (c *driverExecerMock) ExecutePreparedQueries(ctx context.Context, queries []PreparedQuery, args []driver.NamedValue, isQuery bool) (driver.Rows, error) {
	c.callCount += 1
	c.lastQuery = queries
	return nil, nil
}

// TestExecStmt tests that Exec and ExecContext actually calls execer
func TestQueryStmt(t *testing.T) {
	var executor driverExecerMock
	sql := "SELECT 1 UNION SELECT 2"

	stmt, err := MakeStmt(&executor, sql, contextUtils.PreparedStatementsStyleNative)
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}

	_, err = stmt.Query(nil)

	utils.AssertEqual(err, nil, t, "executor returned an error, but shouldn't")
	utils.AssertEqual(executor.callCount, 1, t, "executor wasn't called")
	lastQuery, _, err := executor.lastQuery[0].Format([]driver.NamedValue{})
	if err != nil {
		t.Fatalf("Failed to format lastQuery: %v", err)
	}
	utils.AssertEqual(lastQuery, sql, t, "executor was called with wrong sql")

	_, err = stmt.QueryContext(context.TODO(), nil)
	utils.AssertEqual(err, nil, t, "executor returned an error, but shouldn't")

	utils.AssertEqual(executor.callCount, 2, t, "executor wasn't called")
	lastQuery, _, err = executor.lastQuery[0].Format([]driver.NamedValue{})
	if err != nil {
		t.Fatalf("Failed to format lastQuery: %v", err)
	}
	utils.AssertEqual(lastQuery, sql, t, "executor was called with wrong sql")
}

// TestExecStmt tests that Query and QueryContext actually calls queryer
func TestExecStmt(t *testing.T) {
	var executor driverExecerMock
	sql := "SELECT 1 UNION SELECT 2"

	stmt, err := MakeStmt(&executor, sql, contextUtils.PreparedStatementsStyleNative)
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}

	_, err = stmt.Exec(nil)

	utils.AssertEqual(err, nil, t, "executor returned an error, but shouldn't")
	utils.AssertEqual(executor.callCount, 1, t, "executor wasn't called")
	lastQuery, _, err := executor.lastQuery[0].Format([]driver.NamedValue{})
	if err != nil {
		t.Fatalf("Failed to format lastQuery: %v", err)
	}
	utils.AssertEqual(lastQuery, sql, t, "executor was called with wrong sql")

	_, err = stmt.ExecContext(context.TODO(), nil)
	utils.AssertEqual(err, nil, t, "executor returned an error, but shouldn't")

	utils.AssertEqual(executor.callCount, 2, t, "executor wasn't called")
	lastQuery, _, err = executor.lastQuery[0].Format([]driver.NamedValue{})
	if err != nil {
		t.Fatalf("Failed to format lastQuery: %v", err)
	}
	utils.AssertEqual(lastQuery, sql, t, "executor was called with wrong sql")
}

// TestCloseStmt checks, that closing connection resets all variable, and makes statement not usable
func TestCloseStmt(t *testing.T) {
	var executor driverExecerMock
	sql := "SELECT 1 UNION SELECT 2"

	stmt, err := MakeStmt(&executor, sql, contextUtils.PreparedStatementsStyleNative)
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}
	stmt.Close()

	utils.AssertEqual(stmt.executor, nil, t, "execer wasn't reset by stmt")
	utils.AssertEqual(stmt.Queries, []PreparedQuery{}, t, "queries weren't reset by stmt")
}

/*// TestNumInput checks, that NumInput returns the number of input parameters
func TestNumInput(t *testing.T) {
	executor := driverExecerMock{}

	sql := "SELECT 1"
	stmt, err := MakeStmt(&executor, sql, contextUtils.PreparedStatementsStyleNative)
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}

	utils.AssertEqual(stmt.NumInput(), 2, t, "NumInput should return 0 for a query without parameters")

	sql = "INSERT INTO t VALUES (?, ?)"
	stmt, err = MakeStmt(&executor, sql, contextUtils.PreparedStatementsStyleNative)
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}

	utils.AssertEqual(stmt.NumInput(), 2, t, "NumInput should return 2 for 2 parameters")

	sql = "INSERT INTO t VALUES (?, ?)"
	stmt, err = MakeStmt(&driverExecerMock{}, sql, contextUtils.PreparedStatementsStyleFbNumeric)
	if err != nil {
		t.Fatalf("Failed to create statement: %v", err)
	}

	utils.AssertEqual(stmt.NumInput(), -1, t, "NumInput should return -1 for server-side parameters")
}*/
