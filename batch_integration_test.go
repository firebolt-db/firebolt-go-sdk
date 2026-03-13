//go:build integration_batch
// +build integration_batch

package fireboltgosdk

import (
	"context"
	"database/sql"
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
		return batch.Send()
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
		return batch.Send()
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
		return batch.Send()
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

func TestBatchInsertNullable(t *testing.T) {
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
		return batch.Send()
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
		return batch.Send()
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
		return batch.Send()
	})

	rows, err := db.Query(fmt.Sprintf("SELECT f, d FROM %s ORDER BY d", table))
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rows.Close()

	type row struct{ f float32; d float64 }
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
		return batch.Send()
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
		return batch.Send()
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
		if err := batch.Send(); err != nil {
			return fmt.Errorf("first Send: %w", err)
		}
		// Reuse: send more rows
		if err := batch.Append(int32(3)); err != nil {
			return err
		}
		if err := batch.Send(); err != nil {
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
		return batch.Send()
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

func TestBatchInsertTypeCoercion(t *testing.T) {
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
		return batch.Send()
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
		return batch.Send()
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
		return batch.Send()
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
