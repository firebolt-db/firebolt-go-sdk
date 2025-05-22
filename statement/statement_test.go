package statement

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

type driverExecerMock struct {
	callCount int
	lastQuery string
}

func (c *driverExecerMock) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.callCount += 1
	c.lastQuery = query
	return nil, nil
}

type driverQueryerMock struct {
	callCount int
	lastQuery string
}

func (c *driverQueryerMock) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.callCount += 1
	c.lastQuery = query
	return nil, nil
}

// TestExecStmt tests that Exec and ExecContext actually calls execer
func TestExecStmt(t *testing.T) {
	var queryer driverQueryerMock
	var execer driverExecerMock
	sql := "SELECT 1 UNION SELECT 2"

	stmt := fireboltStmt{queryer: &queryer, execer: &execer, query: sql}

	_, err := stmt.Query(nil)

	utils.AssertEqual(err, nil, t, "queryer returned an error, but shouldn't")
	utils.AssertEqual(queryer.callCount, 1, t, "queryer wasn't called")
	utils.AssertEqual(queryer.lastQuery, sql, t, "queryer was called with wrong sql")

	_, err = stmt.QueryContext(context.TODO(), nil)
	utils.AssertEqual(err, nil, t, "queryer returned an error, but shouldn't")

	utils.AssertEqual(queryer.callCount, 2, t, "queryer wasn't called")
	utils.AssertEqual(queryer.lastQuery, sql, t, "queryer was called with wrong sql")

	utils.AssertEqual(execer.callCount, 0, t, "execer was called, but shouldn't")
}

// TestQueryStmt tests that Query and QueryContext actually calls queryer
func TestQueryStmt(t *testing.T) {
	var queryer driverQueryerMock
	var execer driverExecerMock
	sql := "SELECT 1 UNION SELECT 2"

	stmt := fireboltStmt{queryer: &queryer, execer: &execer, query: sql}

	_, err := stmt.Exec(nil)

	utils.AssertEqual(err, nil, t, "execer returned an error, but shouldn't")
	utils.AssertEqual(execer.callCount, 1, t, "execer wasn't called")
	utils.AssertEqual(execer.lastQuery, sql, t, "execer was called with wrong sql")

	_, err = stmt.ExecContext(context.TODO(), nil)
	utils.AssertEqual(err, nil, t, "execer returned an error, but shouldn't")

	utils.AssertEqual(execer.callCount, 2, t, "execer wasn't called")
	utils.AssertEqual(execer.lastQuery, sql, t, "execer was called with wrong sql")

	utils.AssertEqual(queryer.callCount, 0, t, "queryer was called, but shouldn't")
}

// TestCloseStmt checks, that closing connection resets all variable, and makes statement not usable
func TestCloseStmt(t *testing.T) {
	var queryer driverQueryerMock
	var execer driverExecerMock
	sql := "SELECT 1 UNION SELECT 2"

	stmt := fireboltStmt{queryer: &queryer, execer: &execer, query: sql}
	stmt.Close()

	utils.AssertEqual(stmt.execer, nil, t, "execer wasn't reset by stmt")
	utils.AssertEqual(stmt.queryer, nil, t, "queryer wasn't reset by stmt")
	utils.AssertEqual(stmt.query, "", t, "query wasn't reset by stmt")
}
