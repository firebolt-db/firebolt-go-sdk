//go:build integration_batch
// +build integration_batch

package fireboltgosdk

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"
)

const batchTestDSN = "firebolt:///integration_test_db?url=http://localhost:3473"

func openBatchTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("firebolt", batchTestDSN)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS integration_test_db"); err != nil {
		t.Fatalf("CREATE DATABASE: %v", err)
	}
	return db
}

func execOrFatal(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// doBatch acquires a conn, calls fn inside conn.Raw, and returns any error.
func doBatch(t *testing.T, db *sql.DB, fn func(BatchConnection) error) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	err = conn.Raw(func(driverConn interface{}) error {
		bc, ok := driverConn.(BatchConnection)
		if !ok {
			return fmt.Errorf("driver does not implement BatchConnection")
		}
		return fn(bc)
	})
	if err != nil {
		t.Fatalf("batch operation: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Row-wise batch insert
// ---------------------------------------------------------------------------

func TestBatchInsertRowWise(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_row"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id     INT       NOT NULL,
		name   TEXT      NOT NULL,
		score  DOUBLE    NOT NULL,
		active BOOLEAN   NOT NULL,
		ts     TIMESTAMP NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ts1 := time.Date(2025, 3, 10, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 15, 8, 30, 0, 0, time.UTC)
	ts3 := time.Date(2025, 9, 20, 18, 45, 0, 0, time.UTC)

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, name, score, active, ts)", table))
		if err != nil {
			return err
		}
		if err := batch.Append(int32(1), "Alice", float64(95.5), true, ts1); err != nil {
			return err
		}
		if err := batch.Append(int32(2), "Bob", float64(82.0), false, ts2); err != nil {
			return err
		}
		if err := batch.Append(int32(3), "Charlie", float64(77.3), true, ts3); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	type row struct {
		id     int
		name   string
		score  float64
		active bool
		ts     time.Time
	}
	want := []row{
		{1, "Alice", 95.5, true, ts1},
		{2, "Bob", 82.0, false, ts2},
		{3, "Charlie", 77.3, true, ts3},
	}

	rows, err := db.Query(fmt.Sprintf("SELECT id, name, score, active, ts FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name, &r.score, &r.active, &r.ts); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].id != want[i].id || got[i].name != want[i].name ||
			got[i].score != want[i].score || got[i].active != want[i].active ||
			!got[i].ts.Equal(want[i].ts) {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Columnar batch insert
// ---------------------------------------------------------------------------

func TestBatchInsertColumnar(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_col"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id    INT    NOT NULL,
		name  TEXT   NOT NULL,
		value LONG   NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, name, value)", table))
		if err != nil {
			return err
		}
		if err := batch.Column(0).Append([]int32{10, 20, 30, 40}); err != nil {
			return err
		}
		if err := batch.Column(1).Append([]string{"w", "x", "y", "z"}); err != nil {
			return err
		}
		if err := batch.Column(2).Append([]int64{100, 200, 300, 400}); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT id, name, value FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	wantIDs := []int{10, 20, 30, 40}
	wantNames := []string{"w", "x", "y", "z"}
	wantValues := []int64{100, 200, 300, 400}

	i := 0
	for rows.Next() {
		var id int
		var name string
		var value int64
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Scan row %d: %v", i, err)
		}
		if id != wantIDs[i] || name != wantNames[i] || value != wantValues[i] {
			t.Errorf("row %d = (%d, %q, %d), want (%d, %q, %d)",
				i, id, name, value, wantIDs[i], wantNames[i], wantValues[i])
		}
		i++
	}
	if i != 4 {
		t.Fatalf("row count = %d, want 4", i)
	}
}

// ---------------------------------------------------------------------------
// Mixed row + columnar
// ---------------------------------------------------------------------------

func TestBatchInsertMixed(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_mixed"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id   INT  NOT NULL,
		name TEXT NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, name)", table))
		if err != nil {
			return err
		}
		if err := batch.Append(int32(1), "row_mode"); err != nil {
			return err
		}
		if err := batch.Column(0).Append([]int32{2, 3}); err != nil {
			return err
		}
		if err := batch.Column(1).Append([]string{"col_a", "col_b"}); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	wantIDs := []int{1, 2, 3}
	wantNames := []string{"row_mode", "col_a", "col_b"}
	i := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if id != wantIDs[i] || name != wantNames[i] {
			t.Errorf("row %d = (%d, %q), want (%d, %q)", i, id, name, wantIDs[i], wantNames[i])
		}
		i++
	}
	if i != 3 {
		t.Fatalf("row count = %d, want 3", i)
	}
}

// ---------------------------------------------------------------------------
// Nullable columns: row-wise with NULL and non-NULL values
// ---------------------------------------------------------------------------

func TestBatchInsertNullable(t *testing.T) { // NOSONAR - the integration assertions belong in one lifecycle.
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_nullable"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id   INT          NOT NULL,
		name TEXT         NULL,
		val  INT          NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, name, val)", table))
		if err != nil {
			return err
		}
		if err := batch.Append(int32(1), "Alice", int32(100)); err != nil {
			return err
		}
		if err := batch.Append(int32(2), nil, nil); err != nil {
			return err
		}
		if err := batch.Append(int32(3), "Charlie", nil); err != nil {
			return err
		}
		if err := batch.Append(int32(4), nil, int32(400)); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT id, name, val FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct {
		id   int
		name sql.NullString
		val  sql.NullInt32
	}
	want := []row{
		{1, sql.NullString{String: "Alice", Valid: true}, sql.NullInt32{Int32: 100, Valid: true}},
		{2, sql.NullString{}, sql.NullInt32{}},
		{3, sql.NullString{String: "Charlie", Valid: true}, sql.NullInt32{}},
		{4, sql.NullString{}, sql.NullInt32{Int32: 400, Valid: true}},
	}

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name, &r.val); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].id != want[i].id {
			t.Errorf("row %d id = %d, want %d", i, got[i].id, want[i].id)
		}
		if got[i].name != want[i].name {
			t.Errorf("row %d name = %v, want %v", i, got[i].name, want[i].name)
		}
		if got[i].val != want[i].val {
			t.Errorf("row %d val = %v, want %v", i, got[i].val, want[i].val)
		}
	}
}

// ---------------------------------------------------------------------------
// Date and timestamp round-trip
// ---------------------------------------------------------------------------

func TestBatchInsertDateTimestamp(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_dt"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		d  DATE      NOT NULL,
		ts TIMESTAMP NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	d1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)
	ts1 := time.Date(2020, 1, 1, 12, 30, 45, 0, time.UTC)
	ts2 := time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC)

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (d, ts)", table))
		if err != nil {
			return err
		}
		if err := batch.Append(d1, ts1); err != nil {
			return err
		}
		if err := batch.Append(d2, ts2); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT d, ts FROM %s ORDER BY d", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	wantDates := []time.Time{d2, d1}
	wantTSs := []time.Time{ts2, ts1}

	i := 0
	for rows.Next() {
		var d, ts time.Time
		if err := rows.Scan(&d, &ts); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if !d.Equal(wantDates[i]) {
			t.Errorf("row %d date = %v, want %v", i, d, wantDates[i])
		}
		if !ts.Equal(wantTSs[i]) {
			t.Errorf("row %d ts = %v, want %v", i, ts, wantTSs[i])
		}
		i++
	}
	if i != 2 {
		t.Fatalf("row count = %d, want 2", i)
	}
}

// ---------------------------------------------------------------------------
// Float types round-trip (FLOAT + DOUBLE)
// ---------------------------------------------------------------------------

func TestBatchInsertFloatTypes(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_floats"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		f FLOAT  NOT NULL,
		d DOUBLE NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (f, d)", table))
		if err != nil {
			return err
		}
		if err := batch.Append(float32(3.14), float64(2.718281828)); err != nil {
			return err
		}
		if err := batch.Append(float32(0.0), float64(-1.0)); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT f, d FROM %s ORDER BY d", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct {
		f float32
		d float64
	}
	want := []row{
		{0.0, -1.0},
		{3.14, 2.718281828},
	}

	i := 0
	for rows.Next() {
		var f float64
		var d float64
		if err := rows.Scan(&f, &d); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		if math.Abs(f-float64(want[i].f)) > 0.01 {
			t.Errorf("row %d f = %f, want ~%f", i, f, want[i].f)
		}
		if math.Abs(d-want[i].d) > 1e-6 {
			t.Errorf("row %d d = %f, want %f", i, d, want[i].d)
		}
		i++
	}
	if i != 2 {
		t.Fatalf("row count = %d, want 2", i)
	}
}

// ---------------------------------------------------------------------------
// Large batch (1000 rows) — crosses Parquet write batch boundary
// ---------------------------------------------------------------------------

func TestBatchInsertLarge(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_large"
	const n = 1000
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id  INT  NOT NULL,
		val TEXT NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, val)", table))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if err := batch.Append(int32(i), fmt.Sprintf("item_%04d", i)); err != nil {
				return err
			}
		}
		return batch.Send(ctx)
	})

	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatalf("COUNT: %v", err)
	}
	if count != n {
		t.Errorf("count = %d, want %d", count, n)
	}

	// Spot-check first and last
	var id int
	var val string
	if err := db.QueryRow(fmt.Sprintf("SELECT id, val FROM %s ORDER BY id LIMIT 1", table)).Scan(&id, &val); err != nil {
		t.Fatal(err)
	}
	if id != 0 || val != "item_0000" {
		t.Errorf("first row = (%d, %q), want (0, item_0000)", id, val)
	}
	if err := db.QueryRow(fmt.Sprintf("SELECT id, val FROM %s ORDER BY id DESC LIMIT 1", table)).Scan(&id, &val); err != nil {
		t.Fatal(err)
	}
	if id != n-1 || val != fmt.Sprintf("item_%04d", n-1) {
		t.Errorf("last row = (%d, %q), want (%d, item_%04d)", id, val, n-1, n-1)
	}
}

// ---------------------------------------------------------------------------
// Large batch columnar (1000 rows)
// ---------------------------------------------------------------------------

func TestBatchInsertLargeColumnar(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_large_col"
	const n = 1000
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id  INT  NOT NULL,
		val LONG NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ids := make([]int32, n)
	vals := make([]int64, n)
	for i := 0; i < n; i++ {
		ids[i] = int32(i)
		vals[i] = int64(i) * 10
	}

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, val)", table))
		if err != nil {
			return err
		}
		if err := batch.Column(0).Append(ids); err != nil {
			return err
		}
		if err := batch.Column(1).Append(vals); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Errorf("count = %d, want %d", count, n)
	}
}

// ---------------------------------------------------------------------------
// Batch reuse: Send, then Append more, then Send again
// ---------------------------------------------------------------------------

func TestBatchReuseAfterSend(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_reuse"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id INT NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id)", table))
		if err != nil {
			return err
		}
		// First send
		if err := batch.Append(int32(1)); err != nil {
			return err
		}
		if err := batch.Append(int32(2)); err != nil {
			return err
		}
		if err := batch.Send(ctx); err != nil {
			return fmt.Errorf("first Send: %w", err)
		}
		// Reuse: send more rows
		if err := batch.Append(int32(3)); err != nil {
			return err
		}
		if err := batch.Send(ctx); err != nil {
			return fmt.Errorf("second Send: %w", err)
		}
		return nil
	})

	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

// ---------------------------------------------------------------------------
// Empty batch Send (no rows) is a no-op
// ---------------------------------------------------------------------------

func TestBatchEmptySend(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_empty"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (id INT NOT NULL)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id)", table))
		if err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("count = %d after empty Send, want 0", count)
	}
}

// ---------------------------------------------------------------------------
// PrepareBatch on non-existent table
// ---------------------------------------------------------------------------

func TestBatchPrepareBatchNonExistentTable(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	execOrFatal(t, db, "DROP TABLE IF EXISTS this_table_does_not_exist")

	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("db.Conn: %v", err)
	}
	defer conn.Close()

	var prepareErr error
	conn.Raw(func(driverConn interface{}) error {
		bc := driverConn.(BatchConnection)
		_, prepareErr = bc.PrepareBatch(ctx, "INSERT INTO this_table_does_not_exist (id)")
		return nil
	})

	if prepareErr == nil {
		t.Error("expected error for PrepareBatch on non-existent table")
	}
}

// ---------------------------------------------------------------------------
// Abort discards data, table stays empty
// ---------------------------------------------------------------------------

func TestBatchAbortIntegration(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_abort"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (id INT NOT NULL)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id)", table))
		if err != nil {
			return err
		}
		batch.Append(int32(1))
		batch.Append(int32(2))
		return batch.Abort()
	})

	var count int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Errorf("count = %d after Abort, want 0", count)
	}
}

// ---------------------------------------------------------------------------
// Type coercion: plain Go int to INT column
// ---------------------------------------------------------------------------

func TestBatchInsertTypeCoercion(t *testing.T) { // NOSONAR - the coercion matrix is clearer as one integration test.
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_coerce"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		i32 INT  NOT NULL,
		i64 LONG NOT NULL,
		f64 DOUBLE NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (i32, i64, f64)", table))
		if err != nil {
			return err
		}
		// Use plain int (not int32) — should be coerced
		if err := batch.Append(int(1), int(100), float32(3.14)); err != nil {
			return err
		}
		// Use int64 for int32 column, int32 for int64 column
		if err := batch.Append(int64(2), int32(200), int(42)); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT i32, i64, f64 FROM %s ORDER BY i32", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var i32 int
		var i64 int64
		var f64 float64
		if err := rows.Scan(&i32, &i64, &f64); err != nil {
			t.Fatal(err)
		}
		switch i {
		case 0:
			if i32 != 1 || i64 != 100 {
				t.Errorf("row 0: (%d, %d), want (1, 100)", i32, i64)
			}
			if math.Abs(f64-3.14) > 0.01 {
				t.Errorf("row 0 f64 = %f, want ~3.14", f64)
			}
		case 1:
			if i32 != 2 || i64 != 200 || f64 != 42.0 {
				t.Errorf("row 1: (%d, %d, %f), want (2, 200, 42.0)", i32, i64, f64)
			}
		}
		i++
	}
	if i != 2 {
		t.Fatalf("row count = %d, want 2", i)
	}
}

// ---------------------------------------------------------------------------
// Boolean column round-trip
// ---------------------------------------------------------------------------

func TestBatchInsertBoolean(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_bool"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id  INT     NOT NULL,
		val BOOLEAN NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, val)", table))
		if err != nil {
			return err
		}
		if err := batch.Append(int32(1), true); err != nil {
			return err
		}
		if err := batch.Append(int32(2), false); err != nil {
			return err
		}
		if err := batch.Append(int32(3), true); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT id, val FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	wantBools := []bool{true, false, true}
	i := 0
	for rows.Next() {
		var id int
		var val bool
		if err := rows.Scan(&id, &val); err != nil {
			t.Fatal(err)
		}
		if val != wantBools[i] {
			t.Errorf("row %d val = %v, want %v", i, val, wantBools[i])
		}
		i++
	}
	if i != 3 {
		t.Fatalf("row count = %d, want 3", i)
	}
}

// ---------------------------------------------------------------------------
// Nullable columnar insert
// ---------------------------------------------------------------------------

func TestBatchInsertNullableColumnar(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_nullable_col"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id  INT  NOT NULL,
		val INT  NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, val)", table))
		if err != nil {
			return err
		}
		if err := batch.Column(0).Append([]int32{1, 2, 3}); err != nil {
			return err
		}
		// Nullable column uses []interface{} with nil for NULLs
		if err := batch.Column(1).Append([]interface{}{int32(10), nil, int32(30)}); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	rows, err := db.Query(fmt.Sprintf("SELECT id, val FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct {
		id  int
		val sql.NullInt32
	}
	want := []row{
		{1, sql.NullInt32{Int32: 10, Valid: true}},
		{2, sql.NullInt32{}},
		{3, sql.NullInt32{Int32: 30, Valid: true}},
	}

	i := 0
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.val); err != nil {
			t.Fatal(err)
		}
		if r.id != want[i].id || r.val != want[i].val {
			t.Errorf("row %d = %+v, want %+v", i, r, want[i])
		}
		i++
	}
	if i != 3 {
		t.Fatalf("row count = %d, want 3", i)
	}
}

// ---------------------------------------------------------------------------
// JSON round-trip
// ---------------------------------------------------------------------------

// TestBatchInsertJSON covers the json column type against a live engine.
//
// The unit tests assert what the Parquet writer emits; only the engine says
// whether it accepts it. Every serious defect found in this type so far was of
// that kind -- an empty document rejected at ingest, and null array elements
// encoded at a definition level the reader resolves differently -- and none of
// them is visible from a local round-trip.
func TestBatchInsertJSON(t *testing.T) { // NOSONAR - the JSON matrix shares one Engine-backed lifecycle.
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_json"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id   INT  NOT NULL,
		doc  JSON NULL,
		docs ARRAY(JSON)
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, doc, docs)", table))
		if err != nil {
			return err
		}
		// A scalar, a nested document, an empty array, and a null column.
		if err := batch.Append(int32(1), `{"a":1,"s":"hi"}`, []interface{}{`{"x":1}`, `{"y":2}`}); err != nil {
			return err
		}
		if err := batch.Append(int32(2), `{"nested":{"b":[1,2,3]}}`, []interface{}{}); err != nil {
			return err
		}
		if err := batch.Append(int32(3), nil, []interface{}{`{"z":3}`}); err != nil {
			return err
		}
		// []byte and json.RawMessage are accepted for the scalar column.
		if err := batch.Append(int32(4), []byte(`{"c":4}`), []interface{}{}); err != nil {
			return err
		}
		if err := batch.Append(int32(5), json.RawMessage(`{"raw":5}`), []interface{}{}); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	// Read the documents back through the engine's own JSON accessor, which
	// only works if what was written parses as JSON.
	// Both accessors: _EXTRACT returns the raw JSON value, _EXTRACT_TEXT only
	// resolves JSON strings and yields NULL for a number.
	rows, err := db.Query(fmt.Sprintf(`SELECT id,
		JSON_POINTER_EXTRACT(doc::TEXT, '/a'),
		JSON_POINTER_EXTRACT_TEXT(doc::TEXT, '/s'),
		JSON_POINTER_EXTRACT(doc::TEXT, '/nested/b'),
		JSON_POINTER_EXTRACT(doc::TEXT, '/c'),
		JSON_POINTER_EXTRACT(doc::TEXT, '/raw'),
		LENGTH(docs)
		FROM %s ORDER BY id`, table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct {
		id      int
		a       sql.NullString
		s       sql.NullString
		nested  sql.NullString
		c       sql.NullString
		raw     sql.NullString
		numDocs int
	}
	want := []row{
		{id: 1, a: sql.NullString{String: "1", Valid: true}, s: sql.NullString{String: "hi", Valid: true}, numDocs: 2},
		{id: 2, nested: sql.NullString{String: "[1,2,3]", Valid: true}},
		{id: 3, numDocs: 1},
		{id: 4, c: sql.NullString{String: "4", Valid: true}},
		{id: 5, raw: sql.NullString{String: "5", Valid: true}},
	}

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.a, &r.s, &r.nested, &r.c, &r.raw, &r.numDocs); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Verify the JSON documents inside the arrays, not only their lengths.
	var x, y string
	if err := db.QueryRow(fmt.Sprintf(`SELECT
		JSON_POINTER_EXTRACT(docs[1]::TEXT, '/x'),
		JSON_POINTER_EXTRACT(docs[2]::TEXT, '/y')
		FROM %s WHERE id = 1`, table)).Scan(&x, &y); err != nil {
		t.Fatalf("array documents for id 1: %v", err)
	}
	if x != "1" || y != "2" {
		t.Errorf("array documents for id 1 = (%q, %q), want (1, 2)", x, y)
	}
	var z string
	if err := db.QueryRow(fmt.Sprintf(`SELECT
		JSON_POINTER_EXTRACT(docs[1]::TEXT, '/z')
		FROM %s WHERE id = 3`, table)).Scan(&z); err != nil {
		t.Fatalf("array document for id 3: %v", err)
	}
	if z != "3" {
		t.Errorf("array document for id 3 = %q, want 3", z)
	}

	// The null json column must read back as NULL, not as an empty document.
	var nulls int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT count(*) FROM %s WHERE doc IS NULL", table)).Scan(&nulls); err != nil {
		t.Fatalf("null count: %v", err)
	}
	if nulls != 1 {
		t.Errorf("rows with a null doc = %d, want 1", nulls)
	}
}

// TestBatchInsertJSONColumnar covers the columnar API, which has a []string
// fast path that bypasses the per-row append entirely.
func TestBatchInsertJSONColumnar(t *testing.T) {
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_json_columnar"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id  INT  NOT NULL,
		doc JSON NOT NULL
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, doc)", table))
		if err != nil {
			return err
		}
		if err := batch.Column(0).Append([]int32{1, 2, 3}); err != nil {
			return err
		}
		if err := batch.Column(1).Append([]string{`{"n":1}`, `{"n":2}`, `{"n":3}`}); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	var total int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT SUM(JSON_POINTER_EXTRACT(doc::TEXT, '/n')::INT) FROM %s", table)).Scan(&total); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if total != 6 {
		t.Errorf("sum of /n = %d, want 6: documents did not round-trip", total)
	}
}

// ---------------------------------------------------------------------------
// ARRAY(STRUCT(...)) round-trip
// ---------------------------------------------------------------------------

// TestBatchInsertStructArray covers array(struct(...)) against a live engine.
//
// A struct array is the one column type that spans several Parquet leaves, and
// the repetition and definition levels the writer emits for them are exactly
// what a local round-trip is least able to judge: values can read back at
// plausible levels and still be grouped into the wrong rows. Only the engine
// resolves them the way a query will.
func TestBatchInsertStructArray(t *testing.T) { // NOSONAR - one Engine lifecycle validates levels and field alignment.
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_struct_array"
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id     INT NOT NULL,
		events ARRAY(STRUCT(name TEXT, count INT, tag TEXT NULL, attrs JSON NULL))
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, events)", table))
		if err != nil {
			return err
		}
		// Varying element counts across rows is the case that exercises
		// repetition levels; a uniform count would pass even if they were wrong.
		if err := batch.Append(int32(1), []interface{}{
			map[string]interface{}{"name": "start", "count": int32(1), "tag": "a", "attrs": `{"phase":"start"}`},
			map[string]interface{}{"name": "end", "count": int32(2), "tag": nil, "attrs": nil},
		}); err != nil {
			return err
		}
		if err := batch.Append(int32(2), []interface{}{}); err != nil {
			return err
		}
		if err := batch.Append(int32(3), []interface{}{
			map[string]interface{}{"name": "only", "count": int32(9), "tag": "z", "attrs": "null"},
		}); err != nil {
			return err
		}
		return batch.Send(ctx)
	})

	// Element counts per row: wrong repetition levels regroup elements across
	// rows, which shows up here before any field value is compared.
	rows, err := db.Query(fmt.Sprintf(
		"SELECT id, LENGTH(events) FROM %s ORDER BY id", table))
	if err != nil {
		t.Fatalf("SELECT lengths: %v", err)
	}
	counts := map[int]int{}
	for rows.Next() {
		var id, n int
		if err := rows.Scan(&id, &n); err != nil {
			rows.Close()
			t.Fatalf("Scan: %v", err)
		}
		counts[id] = n
	}
	rows.Close()
	for id, want := range map[int]int{1: 2, 2: 0, 3: 1} {
		if counts[id] != want {
			t.Errorf("row %d has %d elements, want %d", id, counts[id], want)
		}
	}

	// Field values, unnested, to confirm fields stay aligned with each other.
	rows, err = db.Query(fmt.Sprintf(`SELECT t.id, e.name, e.count, e.tag,
		JSON_POINTER_EXTRACT_TEXT(e.attrs::TEXT, '/phase'),
		e.attrs IS NULL, COALESCE(e.attrs::TEXT = 'null', FALSE)
		FROM %s t, UNNEST(t.events) AS e ORDER BY t.id, e.name`, table))
	if err != nil {
		t.Fatalf("SELECT unnested: %v", err)
	}
	defer rows.Close()

	type elem struct {
		id       int
		name     string
		count    int
		tag      sql.NullString
		phase    sql.NullString
		null     bool
		jsonNull bool
	}
	want := []elem{
		{id: 1, name: "end", count: 2, null: true},
		{id: 1, name: "start", count: 1, tag: sql.NullString{String: "a", Valid: true},
			phase: sql.NullString{String: "start", Valid: true}},
		{id: 3, name: "only", count: 9, tag: sql.NullString{String: "z", Valid: true},
			jsonNull: true},
	}

	var got []elem
	for rows.Next() {
		var e elem
		if err := rows.Scan(&e.id, &e.name, &e.count, &e.tag, &e.phase, &e.null, &e.jsonNull); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("element count = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("element %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestBatchInsertStructArrayLarge writes many struct-array rows with varying
// element counts.
//
// liftIntoGroup walks a cursor over the element values in step with the row
// offsets. A drift of one, or an off-by-one on the marker a row with no
// elements contributes, stays invisible at three rows but compounds over
// thousands -- and it misaligns fields against each other rather than dropping
// anything, so a row count still looks right. Element counts vary per row
// (including empty rows) because a uniform count would hide exactly that.
func TestBatchInsertStructArrayLarge(t *testing.T) { // NOSONAR - scale assertions belong to the same Engine-backed dataset.
	db := openBatchTestDB(t)
	defer db.Close()

	const table = "test_batch_struct_large"
	const n = 2000
	execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	execOrFatal(t, db, fmt.Sprintf(`CREATE TABLE %s (
		id     INT NOT NULL,
		events ARRAY(STRUCT(seq INT, label TEXT NULL))
	)`, table))
	defer execOrFatal(t, db, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))

	// Row i holds i%4 elements, so empty rows recur throughout rather than
	// only at the start.
	wantElems := 0
	wantNulls := 0
	for i := 0; i < n; i++ {
		wantElems += i % 4
		for j := 0; j < i%4; j++ {
			if j == 1 {
				wantNulls++ // every element at index 1 has a null label
			}
		}
	}

	ctx := context.Background()
	doBatch(t, db, func(bc BatchConnection) error {
		batch, err := bc.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (id, events)", table))
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			elems := make([]interface{}, 0, i%4)
			for j := 0; j < i%4; j++ {
				var label interface{} = fmt.Sprintf("r%d_e%d", i, j)
				if j == 1 {
					label = nil
				}
				elems = append(elems, map[string]interface{}{
					"seq":   int32(i*10 + j),
					"label": label,
				})
			}
			if err := batch.Append(int32(i), elems); err != nil {
				return err
			}
		}
		return batch.Send(ctx)
	})

	var rowCount, elemCount, nullLabels int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT count(*), SUM(LENGTH(events)) FROM %s", table)).Scan(&rowCount, &elemCount); err != nil {
		t.Fatalf("counts: %v", err)
	}
	if rowCount != n {
		t.Errorf("rows = %d, want %d", rowCount, n)
	}
	if elemCount != wantElems {
		t.Errorf("total elements = %d, want %d", elemCount, wantElems)
	}

	if err := db.QueryRow(fmt.Sprintf(
		`SELECT count(*) FROM %s t, UNNEST(t.events) AS e WHERE e.label IS NULL`,
		table)).Scan(&nullLabels); err != nil {
		t.Fatalf("null labels: %v", err)
	}
	if nullLabels != wantNulls {
		t.Errorf("null labels = %d, want %d", nullLabels, wantNulls)
	}

	// The strongest check: seq encodes its own row and position, so any
	// misalignment between the two fields, or between elements and rows,
	// breaks this without changing any count above.
	var mismatched int
	if err := db.QueryRow(fmt.Sprintf(`SELECT count(*) FROM %s t, UNNEST(t.events) AS e
		WHERE e.seq / 10 <> t.id
		   OR (e.label IS NOT NULL AND e.label <> 'r' || t.id || '_e' || (e.seq %% 10))`,
		table)).Scan(&mismatched); err != nil {
		t.Fatalf("alignment: %v", err)
	}
	if mismatched != 0 {
		t.Errorf("%d elements are misaligned with their row or between fields", mismatched)
	}
}
