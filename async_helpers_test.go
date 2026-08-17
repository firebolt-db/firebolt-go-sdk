package fireboltgosdk

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/rows"
)

var asyncTestDriverID atomic.Uint64

type asyncTestDriver struct {
	open func() (driver.Conn, error)
}

func (d asyncTestDriver) Open(string) (driver.Conn, error) { return d.open() }

type asyncTestConn struct {
	rowsErr  error
	closeErr error
}

func (*asyncTestConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not implemented") }
func (*asyncTestConn) Close() error                        { return nil }
func (*asyncTestConn) Begin() (driver.Tx, error)           { return nil, errors.New("not implemented") }
func (c *asyncTestConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &asyncErrorRows{err: c.rowsErr, closeErr: c.closeErr}, nil
}

type asyncErrorRows struct {
	err      error
	closeErr error
}

func (r *asyncErrorRows) Columns() []string {
	if r.closeErr != nil {
		return make([]string, 12)
	}
	return []string{"status"}
}
func (r *asyncErrorRows) Close() error              { return r.closeErr }
func (r *asyncErrorRows) Next([]driver.Value) error { return r.err }

func openAsyncTestDB(t *testing.T, testDriver driver.Driver) *sql.DB {
	t.Helper()
	name := fmt.Sprintf("firebolt-async-test-%d", asyncTestDriverID.Add(1))
	sql.Register(name, testDriver)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCancelAsyncQueryReturnsMonitorError(t *testing.T) {
	monitorErr := errors.New("monitor unavailable")
	db := openAsyncTestDB(t, asyncTestDriver{open: func() (driver.Conn, error) { return nil, monitorErr }})

	err := CancelAsyncQuery(db, *rows.NewAsyncResult("token", "SELECT status"))
	if !errors.Is(err, monitorErr) {
		t.Fatalf("CancelAsyncQuery() error = %v, want monitor error", err)
	}
}

func TestGetAsyncQueryStatusReturnsRowsError(t *testing.T) {
	rowsErr := errors.New("row stream failed")
	db := openAsyncTestDB(t, asyncTestDriver{open: func() (driver.Conn, error) {
		return &asyncTestConn{rowsErr: rowsErr}, nil
	}})

	_, err := getAsyncQueryStatus(db, *rows.NewAsyncResult("token", "SELECT status"))
	if !errors.Is(err, rowsErr) {
		t.Fatalf("getAsyncQueryStatus() error = %v, want rows error", err)
	}
}

func TestGetAsyncQueryStatusClearsResultOnCloseError(t *testing.T) {
	closeErr := errors.New("close failed")
	db := openAsyncTestDB(t, asyncTestDriver{open: func() (driver.Conn, error) {
		return &asyncTestConn{closeErr: closeErr}, nil
	}})

	status, err := getAsyncQueryStatus(db, *rows.NewAsyncResult("token", "SELECT status"))
	if status != nil {
		t.Fatalf("getAsyncQueryStatus() status = %v, want nil", status)
	}
	if !errors.Is(err, closeErr) {
		t.Fatalf("getAsyncQueryStatus() error = %v, want close error", err)
	}
}

func TestExecAsyncContextReleasesConnection(t *testing.T) {
	db := openAsyncTestDB(t, asyncTestDriver{open: func() (driver.Conn, error) {
		return &asyncTestConn{}, nil
	}})
	db.SetMaxOpenConns(1)

	_, _ = ExecAsyncContext(context.Background(), db, "SELECT 1")
	if inUse := db.Stats().InUse; inUse != 0 {
		t.Fatalf("database connections still in use = %d, want 0", inUse)
	}
}
