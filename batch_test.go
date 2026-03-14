package fireboltgosdk

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestParseInsertQuery(t *testing.T) {
	tests := []struct {
		query       string
		wantTable   string
		wantColumns []string
		wantErr     bool
	}{
		{
			query:       `INSERT INTO my_table (col1, col2, col3)`,
			wantTable:   "my_table",
			wantColumns: []string{"col1", "col2", "col3"},
		},
		{
			query:       `INSERT INTO my_table (col1, col2) VALUES`,
			wantTable:   "my_table",
			wantColumns: []string{"col1", "col2"},
		},
		{
			query:       `INSERT INTO "my_schema"."my_table" ("col1", "col2")`,
			wantTable:   `"my_schema"."my_table"`,
			wantColumns: []string{"col1", "col2"},
		},
		{
			query:       `insert into t (a)`,
			wantTable:   "t",
			wantColumns: []string{"a"},
		},
		{
			query:   `SELECT * FROM t`,
			wantErr: true,
		},
		{
			query:   `INSERT INTO t`,
			wantErr: true,
		},
		{
			query:   `INSERT INTO  ()`,
			wantErr: true,
		},
		{
			query:   `INSERT INTO t; DROP TABLE x-- (a)`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tableName, columns, err := parseInsertQuery(tc.query)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseInsertQuery(%q): expected error", tc.query)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseInsertQuery(%q): %v", tc.query, err)
			continue
		}
		if tableName != tc.wantTable {
			t.Errorf("parseInsertQuery(%q): table = %q, want %q", tc.query, tableName, tc.wantTable)
		}
		if len(columns) != len(tc.wantColumns) {
			t.Errorf("parseInsertQuery(%q): %d columns, want %d", tc.query, len(columns), len(tc.wantColumns))
			continue
		}
		for i, col := range columns {
			if col != tc.wantColumns[i] {
				t.Errorf("parseInsertQuery(%q): column[%d] = %q, want %q", tc.query, i, col, tc.wantColumns[i])
			}
		}
	}
}

func TestBuildParquetInsertQuery(t *testing.T) {
	got := buildParquetInsertQuery("my_table", []string{"col2", "col1"}, "batch_data")
	want := `INSERT INTO "my_table" ("col1", "col2") SELECT * FROM read_parquet('upload://batch_data')`
	if got != want {
		t.Errorf("buildParquetInsertQuery = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// Columnar batch API tests (unit-level, no network)
// ---------------------------------------------------------------------------

func TestBatchColumnarViaBlock(t *testing.T) {
	blk, err := newBlock([]string{"id", "name", "active"},
		[]string{"int", "text", "boolean"})
	if err != nil {
		t.Fatal(err)
	}

	batch := &fireboltBatch{blk: blk}

	if err := batch.Column(0).Append([]int32{1, 2, 3}); err != nil {
		t.Fatalf("Column(0).Append: %v", err)
	}
	if err := batch.Column(1).Append([]string{"a", "b", "c"}); err != nil {
		t.Fatalf("Column(1).Append: %v", err)
	}
	if err := batch.Column(2).Append([]bool{true, false, true}); err != nil {
		t.Fatalf("Column(2).Append: %v", err)
	}

	if blk.blockRows() != 3 {
		t.Fatalf("blockRows() = %d, want 3", blk.blockRows())
	}
	if err := blk.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestBatchColumnarIndexOutOfRange(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}
	if err := batch.Column(5).Append([]int32{1}); err == nil {
		t.Error("expected error for out-of-range column index")
	}
}

func TestBatchMixedRowAndColumnar(t *testing.T) {
	blk, _ := newBlock([]string{"id", "name"}, []string{"int", "text"})
	batch := &fireboltBatch{blk: blk}

	if err := batch.Append(int32(1), "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Column(0).Append([]int32{2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := batch.Column(1).Append([]string{"Bob", "Charlie"}); err != nil {
		t.Fatal(err)
	}

	if blk.blockRows() != 3 {
		t.Fatalf("blockRows() = %d, want 3", blk.blockRows())
	}
	if err := blk.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestBatchAbortClearsColumnarData(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}
	if err := batch.Column(0).Append([]int32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	if err := batch.Abort(); err != nil {
		t.Fatal(err)
	}
	if blk.blockRows() != 0 {
		t.Errorf("blockRows() = %d after Abort, want 0", blk.blockRows())
	}
}

// ---------------------------------------------------------------------------
// Parquet round-trip helpers
// ---------------------------------------------------------------------------

// readParquetRows opens the Parquet bytes and reads all rows back.
// Returns rows as []parquet.Row keyed by column NAME (sorted alphabetically
// by parquet.Group).
func readParquetRows(t *testing.T, data []byte) (*parquet.File, []parquet.Row) {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	var rows []parquet.Row
	for _, rg := range f.RowGroups() {
		rr := parquet.NewRowGroupReader(rg)
		buf := make([]parquet.Row, rg.NumRows())
		n, err := rr.ReadRows(buf)
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("ReadRows: %v", err)
		}
		rows = append(rows, buf[:n]...)
	}
	return f, rows
}

// colIndex returns the leaf column index for a given field name in the schema.
func colIndex(f *parquet.File, name string) int {
	for i, field := range f.Schema().Fields() {
		if field.Name() == name {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Row-wise round-trip: write rows → serialise → read back → verify values
// ---------------------------------------------------------------------------

func TestRowWiseRoundTrip(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "name", "active"},
		[]string{"int", "text", "boolean"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{int32(1), "Alice", true}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(2), "Bob", false}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	idCol := colIndex(f, "id")
	nameCol := colIndex(f, "name")
	activeCol := colIndex(f, "active")

	// Row 0
	if got := rows[0][idCol].Int32(); got != 1 {
		t.Errorf("row 0 id = %d, want 1", got)
	}
	if got := string(rows[0][nameCol].ByteArray()); got != "Alice" {
		t.Errorf("row 0 name = %q, want Alice", got)
	}
	if got := rows[0][activeCol].Boolean(); got != true {
		t.Errorf("row 0 active = %v, want true", got)
	}

	// Row 1
	if got := rows[1][idCol].Int32(); got != 2 {
		t.Errorf("row 1 id = %d, want 2", got)
	}
	if got := string(rows[1][nameCol].ByteArray()); got != "Bob" {
		t.Errorf("row 1 name = %q, want Bob", got)
	}
	if got := rows[1][activeCol].Boolean(); got != false {
		t.Errorf("row 1 active = %v, want false", got)
	}
}

// ---------------------------------------------------------------------------
// Columnar round-trip: append columns → serialise → read back → verify
// ---------------------------------------------------------------------------

func TestColumnarRoundTrip(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "name", "active"},
		[]string{"int", "text", "boolean"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.columnAt(0).appendColumn([]int32{10, 20, 30}); err != nil {
		t.Fatal(err)
	}
	if err := blk.columnAt(1).appendColumn([]string{"x", "y", "z"}); err != nil {
		t.Fatal(err)
	}
	if err := blk.columnAt(2).appendColumn([]bool{false, true, false}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	idCol := colIndex(f, "id")
	nameCol := colIndex(f, "name")
	activeCol := colIndex(f, "active")

	wantIDs := []int32{10, 20, 30}
	wantNames := []string{"x", "y", "z"}
	wantActive := []bool{false, true, false}

	for i := 0; i < 3; i++ {
		if got := rows[i][idCol].Int32(); got != wantIDs[i] {
			t.Errorf("row %d id = %d, want %d", i, got, wantIDs[i])
		}
		if got := string(rows[i][nameCol].ByteArray()); got != wantNames[i] {
			t.Errorf("row %d name = %q, want %q", i, got, wantNames[i])
		}
		if got := rows[i][activeCol].Boolean(); got != wantActive[i] {
			t.Errorf("row %d active = %v, want %v", i, got, wantActive[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Mixed (row + columnar) round-trip
// ---------------------------------------------------------------------------

func TestMixedModeRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"id", "name"}, []string{"int", "text"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{int32(1), "row"}); err != nil {
		t.Fatal(err)
	}
	if err := blk.columnAt(0).appendColumn([]int32{2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := blk.columnAt(1).appendColumn([]string{"col1", "col2"}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	idCol := colIndex(f, "id")
	nameCol := colIndex(f, "name")

	wantIDs := []int32{1, 2, 3}
	wantNames := []string{"row", "col1", "col2"}

	for i := 0; i < 3; i++ {
		if got := rows[i][idCol].Int32(); got != wantIDs[i] {
			t.Errorf("row %d id = %d, want %d", i, got, wantIDs[i])
		}
		if got := string(rows[i][nameCol].ByteArray()); got != wantNames[i] {
			t.Errorf("row %d name = %q, want %q", i, got, wantNames[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Nullable round-trip: NULL and non-NULL survive serialisation
// ---------------------------------------------------------------------------

func TestNullableRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"val"}, []string{"int null"})
	if err != nil {
		t.Fatal(err)
	}

	if err = blk.appendRow([]interface{}{int32(42)}); err != nil {
		t.Fatal(err)
	}
	if err = blk.appendRow([]interface{}{nil}); err != nil {
		t.Fatal(err)
	}
	if err = blk.appendRow([]interface{}{int32(7)}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	_, rows := readParquetRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Row 0: 42 (not null)
	if rows[0][0].IsNull() {
		t.Error("row 0: expected non-null")
	} else if got := rows[0][0].Int32(); got != 42 {
		t.Errorf("row 0 val = %d, want 42", got)
	}

	// Row 1: NULL
	if !rows[1][0].IsNull() {
		t.Errorf("row 1: expected null, got %v", rows[1][0])
	}

	// Row 2: 7 (not null)
	if rows[2][0].IsNull() {
		t.Error("row 2: expected non-null")
	} else if got := rows[2][0].Int32(); got != 7 {
		t.Errorf("row 2 val = %d, want 7", got)
	}
}

// ---------------------------------------------------------------------------
// All primitive types round-trip
// ---------------------------------------------------------------------------

func TestAllTypesRowWiseRoundTrip(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	dt := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)

	blk, err := newBlock(
		[]string{"a_int", "b_long", "c_float", "d_double", "e_text", "f_bool", "g_date", "h_ts", "i_bytea"},
		[]string{"int", "long", "float", "double", "text", "boolean", "date", "timestamp", "bytea"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{
		int32(42), int64(9999999), float32(3.14), float64(2.718),
		"hello", true, dt, ts, []byte{0xDE, 0xAD},
	}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]

	if got := row[colIndex(f, "a_int")].Int32(); got != 42 {
		t.Errorf("a_int = %d, want 42", got)
	}
	if got := row[colIndex(f, "b_long")].Int64(); got != 9999999 {
		t.Errorf("b_long = %d, want 9999999", got)
	}
	if got := row[colIndex(f, "c_float")].Float(); got != 3.14 {
		t.Errorf("c_float = %v, want 3.14", got)
	}
	if got := row[colIndex(f, "d_double")].Double(); got != 2.718 {
		t.Errorf("d_double = %v, want 2.718", got)
	}
	if got := string(row[colIndex(f, "e_text")].ByteArray()); got != "hello" {
		t.Errorf("e_text = %q, want hello", got)
	}
	if got := row[colIndex(f, "f_bool")].Boolean(); got != true {
		t.Errorf("f_bool = %v, want true", got)
	}
	// DATE = days since epoch
	wantDays := int32(dt.Sub(epoch) / (24 * time.Hour))
	if got := row[colIndex(f, "g_date")].Int32(); got != wantDays {
		t.Errorf("g_date = %d, want %d", got, wantDays)
	}
	// TIMESTAMP = microseconds since epoch
	if got := row[colIndex(f, "h_ts")].Int64(); got != ts.UnixMicro() {
		t.Errorf("h_ts = %d, want %d", got, ts.UnixMicro())
	}
	if got := row[colIndex(f, "i_bytea")].ByteArray(); got[0] != 0xDE || got[1] != 0xAD {
		t.Errorf("i_bytea = %x, want DEAD", got)
	}
}

// ---------------------------------------------------------------------------
// Empty block
// ---------------------------------------------------------------------------

func TestBlockToParquetEmpty(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	data, err := blk.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Errorf("expected nil for empty block, got %d bytes", len(data))
	}
}

// ---------------------------------------------------------------------------
// Array column round-trip
// ---------------------------------------------------------------------------

func TestArrayColumnRowWiseRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"tags"}, []string{"array(text)"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{[]string{"a", "b", "c"}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]string{"d"}}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	tagsCol := colIndex(f, "tags")

	// Row 0: ["a", "b", "c"] — 3 values all at leaf column tagsCol
	var r0vals []string
	for _, v := range rows[0] {
		if v.Column() == tagsCol {
			r0vals = append(r0vals, string(v.ByteArray()))
		}
	}
	if len(r0vals) != 3 || r0vals[0] != "a" || r0vals[1] != "b" || r0vals[2] != "c" {
		t.Errorf("row 0 tags = %v, want [a b c]", r0vals)
	}

	// Row 1: ["d"] — 1 value
	var r1vals []string
	for _, v := range rows[1] {
		if v.Column() == tagsCol {
			r1vals = append(r1vals, string(v.ByteArray()))
		}
	}
	if len(r1vals) != 1 || r1vals[0] != "d" {
		t.Errorf("row 1 tags = %v, want [d]", r1vals)
	}
}

func TestArrayColumnEmptyArrayRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"nums"}, []string{"array(int)"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{[]int32{1, 2}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]int32{}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]int32{3}}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	_, rows := readParquetRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Row 0: [1, 2]
	if len(rows[0]) != 2 {
		t.Errorf("row 0: expected 2 values, got %d", len(rows[0]))
	}

	// Row 1: [] — in REPEATED encoding, an empty array is represented by a
	// single sentinel null value (rep=0, def=0).
	if len(rows[1]) != 1 {
		t.Errorf("row 1: expected 1 sentinel value for empty array, got %d", len(rows[1]))
	} else if !rows[1][0].IsNull() {
		t.Errorf("row 1: expected null sentinel, got %v", rows[1][0])
	}

	// Row 2: [3]
	if len(rows[2]) != 1 {
		t.Errorf("row 2: expected 1 value, got %d", len(rows[2]))
	}
}

func TestArrayWithOtherColumnsRoundTrip(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "tags"},
		[]string{"int", "array(text)"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{int32(1), []string{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(2), []string{"c"}}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	idCol := colIndex(f, "id")
	tagsCol := colIndex(f, "tags")

	// Row 0: id=1, tags=["a","b"]
	if got := rows[0][0].Int32(); rows[0][0].Column() == idCol && got != 1 {
		t.Errorf("row 0 id = %d, want 1", got)
	}

	var r0tags []string
	for _, v := range rows[0] {
		if v.Column() == tagsCol {
			r0tags = append(r0tags, string(v.ByteArray()))
		}
	}
	if len(r0tags) != 2 || r0tags[0] != "a" || r0tags[1] != "b" {
		t.Errorf("row 0 tags = %v, want [a b]", r0tags)
	}

	// Row 1: id=2, tags=["c"]
	var r1id int32
	for _, v := range rows[1] {
		if v.Column() == idCol {
			r1id = v.Int32()
		}
	}
	if r1id != 2 {
		t.Errorf("row 1 id = %d, want 2", r1id)
	}
}

// ---------------------------------------------------------------------------
// Row-wise and columnar produce identical Parquet content
// ---------------------------------------------------------------------------

func TestRowAndColumnarProduceSameData(t *testing.T) {
	makeRowBlock := func() *block {
		blk, _ := newBlock([]string{"x", "y"}, []string{"long", "text"})
		if err := blk.appendRow([]interface{}{int64(1), "a"}); err != nil {
			t.Fatal(err)
		}
		if err := blk.appendRow([]interface{}{int64(2), "b"}); err != nil {
			t.Fatal(err)
		}
		return blk
	}
	makeColBlock := func() *block {
		blk, _ := newBlock([]string{"x", "y"}, []string{"long", "text"})
		if err := blk.columnAt(0).appendColumn([]int64{1, 2}); err != nil {
			t.Fatal(err)
		}
		if err := blk.columnAt(1).appendColumn([]string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
		return blk
	}

	dataRow, err := makeRowBlock().toParquet()
	if err != nil {
		t.Fatal(err)
	}
	dataCol, err := makeColBlock().toParquet()
	if err != nil {
		t.Fatal(err)
	}

	fRow, rowsRow := readParquetRows(t, dataRow)
	fCol, rowsCol := readParquetRows(t, dataCol)

	if len(rowsRow) != len(rowsCol) {
		t.Fatalf("row count: row=%d, col=%d", len(rowsRow), len(rowsCol))
	}
	for i := range rowsRow {
		xR := rowsRow[i][colIndex(fRow, "x")].Int64()
		xC := rowsCol[i][colIndex(fCol, "x")].Int64()
		if xR != xC {
			t.Errorf("row %d x: row=%d, col=%d", i, xR, xC)
		}
		yR := string(rowsRow[i][colIndex(fRow, "y")].ByteArray())
		yC := string(rowsCol[i][colIndex(fCol, "y")].ByteArray())
		if yR != yC {
			t.Errorf("row %d y: row=%q, col=%q", i, yR, yC)
		}
	}
}

// ===========================================================================
// Error handling and edge cases
// ===========================================================================

func TestAppendWrongType(t *testing.T) {
	blk, _ := newBlock([]string{"id"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}

	if err := batch.Append("not_an_int"); err == nil {
		t.Error("expected error appending string to int column")
	}
}

func TestAppendWrongColumnCount(t *testing.T) {
	blk, _ := newBlock([]string{"id", "name"}, []string{"int", "text"})
	batch := &fireboltBatch{blk: blk}

	if err := batch.Append(int32(1)); err == nil {
		t.Error("expected error for too few values")
	}
	if err := batch.Append(int32(1), "a", true); err == nil {
		t.Error("expected error for too many values")
	}
}

func TestColumnarAppendWrongType(t *testing.T) {
	blk, _ := newBlock([]string{"id"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}

	if err := batch.Column(0).Append([]string{"a", "b"}); err == nil {
		t.Error("expected error appending []string to int column")
	}
}

func TestColumnarAppendNonSlice(t *testing.T) {
	blk, _ := newBlock([]string{"id"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}

	if err := batch.Column(0).Append(int32(42)); err == nil {
		t.Error("expected error appending scalar to column (requires slice)")
	}
}

func TestDoubleAbortIsSafe(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}
	if err := batch.Append(int32(1)); err != nil {
		t.Fatal(err)
	}

	if err := batch.Abort(); err != nil {
		t.Fatalf("first Abort: %v", err)
	}
	if err := batch.Abort(); err != nil {
		t.Fatalf("second Abort: %v", err)
	}
	if blk.blockRows() != 0 {
		t.Errorf("blockRows() = %d after double Abort, want 0", blk.blockRows())
	}
}

func TestAbortThenReuse(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}

	if err := batch.Append(int32(1)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(int32(2)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Abort(); err != nil {
		t.Fatal(err)
	}

	if err := batch.Append(int32(10)); err != nil {
		t.Fatalf("Append after Abort: %v", err)
	}
	if blk.blockRows() != 1 {
		t.Errorf("blockRows() = %d after reuse, want 1", blk.blockRows())
	}
}

func TestValidateMismatchedColumnLengths(t *testing.T) {
	blk, _ := newBlock([]string{"a", "b"}, []string{"int", "text"})
	_ = blk.columnAt(0).appendColumn([]int32{1, 2, 3})
	_ = blk.columnAt(1).appendColumn([]string{"x"})

	if err := blk.validate(); err == nil {
		t.Error("expected validation error for mismatched column lengths")
	}
}

func TestUnsupportedColumnType(t *testing.T) {
	_, err := newBlock([]string{"x"}, []string{"decimal(10,2)"})
	if err == nil {
		t.Error("expected error for unsupported column type")
	}
}

func TestNewBlockColumnCountMismatch(t *testing.T) {
	_, err := newBlock([]string{"a", "b"}, []string{"int"})
	if err == nil {
		t.Error("expected error for column name/type count mismatch")
	}
}

func TestNewBlockDuplicateColumnName(t *testing.T) {
	_, err := newBlock([]string{"a", "b", "a"}, []string{"int", "text", "int"})
	if err == nil {
		t.Error("expected error for duplicate column name")
	}
}

// ===========================================================================
// Type coercion tests
// ===========================================================================

func TestInt32CoercionFromVariousTypes(t *testing.T) {
	blk, _ := newBlock([]string{"v"}, []string{"int"})
	cases := []interface{}{
		int32(1), int(2), int64(3), int16(4), int8(5),
		uint8(6), uint16(7), float64(8.0), float32(9.0),
	}
	for _, v := range cases {
		if err := blk.appendRow([]interface{}{v}); err != nil {
			t.Errorf("appendRow(%T(%v)): %v", v, v, err)
		}
	}
	if blk.blockRows() != len(cases) {
		t.Errorf("blockRows() = %d, want %d", blk.blockRows(), len(cases))
	}
}

func TestInt64CoercionFromVariousTypes(t *testing.T) {
	blk, _ := newBlock([]string{"v"}, []string{"long"})
	cases := []interface{}{
		int64(1), int(2), int32(3), int16(4), int8(5),
		uint8(6), uint16(7), uint32(8), float64(9.0), float32(10.0),
	}
	for _, v := range cases {
		if err := blk.appendRow([]interface{}{v}); err != nil {
			t.Errorf("appendRow(%T(%v)): %v", v, v, err)
		}
	}
	if blk.blockRows() != len(cases) {
		t.Errorf("blockRows() = %d, want %d", blk.blockRows(), len(cases))
	}
}

func TestFloat64CoercionFromVariousTypes(t *testing.T) {
	blk, _ := newBlock([]string{"v"}, []string{"double"})
	cases := []interface{}{
		float64(1.1), float32(2.2), int(3), int32(4), int64(5),
	}
	for _, v := range cases {
		if err := blk.appendRow([]interface{}{v}); err != nil {
			t.Errorf("appendRow(%T(%v)): %v", v, v, err)
		}
	}
}

func TestFloat32CoercionFromVariousTypes(t *testing.T) {
	blk, _ := newBlock([]string{"v"}, []string{"float"})
	cases := []interface{}{
		float32(1.1), float64(2.2), int(3), int32(4), int64(5),
	}
	for _, v := range cases {
		if err := blk.appendRow([]interface{}{v}); err != nil {
			t.Errorf("appendRow(%T(%v)): %v", v, v, err)
		}
	}
}

// ===========================================================================
// Nullable columnar mode
// ===========================================================================

func TestNullableColumnarAppend(t *testing.T) {
	blk, err := newBlock([]string{"val"}, []string{"int null"})
	if err != nil {
		t.Fatal(err)
	}

	// Columnar append of nullable values goes through appendColumnFallback
	// which calls appendRow per element.
	vals := []interface{}{int32(1), nil, int32(3), nil, int32(5)}
	if err := blk.columnAt(0).appendColumn(vals); err != nil {
		t.Fatalf("appendColumn: %v", err)
	}
	if blk.blockRows() != 5 {
		t.Fatalf("blockRows() = %d, want 5", blk.blockRows())
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	_, rows := readParquetRows(t, data)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	for i, row := range rows {
		if i%2 == 0 {
			if row[0].IsNull() {
				t.Errorf("row %d: expected non-null", i)
			}
		} else {
			if !row[0].IsNull() {
				t.Errorf("row %d: expected null", i)
			}
		}
	}
}

func TestNullableAllTypesRoundTrip(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	dt := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	types := []struct {
		name    string
		fbType  string
		val     interface{}
		checker func(t *testing.T, v parquet.Value)
	}{
		{"ni", "int null", int32(42), func(t *testing.T, v parquet.Value) {
			if v.Int32() != 42 {
				t.Errorf("ni = %d, want 42", v.Int32())
			}
		}},
		{"nl", "long null", int64(99), func(t *testing.T, v parquet.Value) {
			if v.Int64() != 99 {
				t.Errorf("nl = %d, want 99", v.Int64())
			}
		}},
		{"nf", "float null", float32(1.5), func(t *testing.T, v parquet.Value) {
			if v.Float() != 1.5 {
				t.Errorf("nf = %v, want 1.5", v.Float())
			}
		}},
		{"nd", "double null", float64(2.5), func(t *testing.T, v parquet.Value) {
			if v.Double() != 2.5 {
				t.Errorf("nd = %v, want 2.5", v.Double())
			}
		}},
		{"nt", "text null", "hello", func(t *testing.T, v parquet.Value) {
			if string(v.ByteArray()) != "hello" {
				t.Errorf("nt = %q, want hello", v.ByteArray())
			}
		}},
		{"nb", "boolean null", true, func(t *testing.T, v parquet.Value) {
			if !v.Boolean() {
				t.Errorf("nb = false, want true")
			}
		}},
		{"ndt", "date null", dt, func(t *testing.T, v parquet.Value) {
			wantDays := int32(dt.Sub(epoch) / (24 * time.Hour))
			if v.Int32() != wantDays {
				t.Errorf("ndt = %d, want %d", v.Int32(), wantDays)
			}
		}},
		{"nts", "timestamp null", ts, func(t *testing.T, v parquet.Value) {
			if v.Int64() != ts.UnixMicro() {
				t.Errorf("nts = %d, want %d", v.Int64(), ts.UnixMicro())
			}
		}},
	}

	names := make([]string, len(types))
	fbTypes := make([]string, len(types))
	for i, tt := range types {
		names[i] = tt.name
		fbTypes[i] = tt.fbType
	}

	blk, err := newBlock(names, fbTypes)
	if err != nil {
		t.Fatal(err)
	}

	// Row 0: all non-null
	vals0 := make([]interface{}, len(types))
	for i, tt := range types {
		vals0[i] = tt.val
	}
	if err := blk.appendRow(vals0); err != nil {
		t.Fatal(err)
	}

	// Row 1: all null
	vals1 := make([]interface{}, len(types))
	if err := blk.appendRow(vals1); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Row 0: all non-null
	for _, tt := range types {
		idx := colIndex(f, tt.name)
		v := rows[0][idx]
		if v.IsNull() {
			t.Errorf("row 0 %s: expected non-null", tt.name)
			continue
		}
		tt.checker(t, v)
	}

	// Row 1: all null
	for _, tt := range types {
		idx := colIndex(f, tt.name)
		v := rows[1][idx]
		if !v.IsNull() {
			t.Errorf("row 1 %s: expected null, got %v", tt.name, v)
		}
	}
}

// ===========================================================================
// Large batch (crosses the 4096 batchSize boundary in toParquet)
// ===========================================================================

func TestLargeBatchRowWise(t *testing.T) {
	const n = 5000
	blk, _ := newBlock([]string{"id", "val"}, []string{"int", "text"})

	for i := 0; i < n; i++ {
		if err := blk.appendRow([]interface{}{int32(i), fmt.Sprintf("row_%d", i)}); err != nil {
			t.Fatalf("appendRow %d: %v", i, err)
		}
	}
	if blk.blockRows() != n {
		t.Fatalf("blockRows() = %d, want %d", blk.blockRows(), n)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != n {
		t.Fatalf("expected %d rows, got %d", n, len(rows))
	}

	idCol := colIndex(f, "id")
	valCol := colIndex(f, "val")

	for i := 0; i < n; i++ {
		if got := rows[i][idCol].Int32(); got != int32(i) {
			t.Errorf("row %d id = %d, want %d", i, got, i)
		}
		want := fmt.Sprintf("row_%d", i)
		if got := string(rows[i][valCol].ByteArray()); got != want {
			t.Errorf("row %d val = %q, want %q", i, got, want)
		}
	}
}

func TestLargeBatchColumnar(t *testing.T) {
	const n = 5000
	blk, _ := newBlock([]string{"id", "val"}, []string{"long", "double"})

	ids := make([]int64, n)
	vals := make([]float64, n)
	for i := 0; i < n; i++ {
		ids[i] = int64(i)
		vals[i] = float64(i) * 0.1
	}

	if err := blk.columnAt(0).appendColumn(ids); err != nil {
		t.Fatal(err)
	}
	if err := blk.columnAt(1).appendColumn(vals); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != n {
		t.Fatalf("expected %d rows, got %d", n, len(rows))
	}

	idCol := colIndex(f, "id")
	for i := 0; i < n; i++ {
		if got := rows[i][idCol].Int64(); got != int64(i) {
			t.Errorf("row %d id = %d, want %d", i, got, i)
		}
	}
}

// ===========================================================================
// Batch reset / reuse after toParquet (simulates Send then reuse)
// ===========================================================================

func TestBlockResetAndReuse(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	if err := blk.appendRow([]interface{}{int32(1)}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(2)}); err != nil {
		t.Fatal(err)
	}

	data1, err := blk.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	_, rows1 := readParquetRows(t, data1)
	if len(rows1) != 2 {
		t.Fatalf("first batch: %d rows, want 2", len(rows1))
	}

	blk.reset()
	if blk.blockRows() != 0 {
		t.Fatalf("blockRows() = %d after reset, want 0", blk.blockRows())
	}

	if err := blk.appendRow([]interface{}{int32(10)}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(20)}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(30)}); err != nil {
		t.Fatal(err)
	}

	data2, err := blk.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	f2, rows2 := readParquetRows(t, data2)
	if len(rows2) != 3 {
		t.Fatalf("second batch: %d rows, want 3", len(rows2))
	}

	xCol := colIndex(f2, "x")
	want := []int32{10, 20, 30}
	for i, w := range want {
		if got := rows2[i][xCol].Int32(); got != w {
			t.Errorf("row %d = %d, want %d", i, got, w)
		}
	}
}

// ===========================================================================
// Empty batch Send is a no-op (no error)
// ===========================================================================

func TestEmptyBatchSendNoOp(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}

	// Send with zero rows should be a no-op (no network call needed)
	// Since conn is nil, a real upload would panic; if we reach here
	// without panic/error, it means the empty check works.
	if err := batch.Send(context.Background()); err != nil {
		t.Fatalf("empty Send: %v", err)
	}
}

// ===========================================================================
// Column type aliases
// ===========================================================================

func TestColumnTypeAliases(t *testing.T) {
	aliases := []struct {
		fbType   string
		testVal  interface{}
		wantRows int
	}{
		{"integer", int32(1), 1},
		{"bigint", int64(1), 1},
		{"real", float32(1.0), 1},
		{"double precision", float64(1.0), 1},
		{"pgdate", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), 1},
		{"timestampntz", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), 1},
		{"timestamptz", time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC), 1},
		{"geography", "POINT(0 0)", 1},
	}

	for _, a := range aliases {
		t.Run(a.fbType, func(t *testing.T) {
			blk, err := newBlock([]string{"v"}, []string{a.fbType})
			if err != nil {
				t.Fatalf("newBlock(%q): %v", a.fbType, err)
			}
			if err := blk.appendRow([]interface{}{a.testVal}); err != nil {
				t.Fatalf("appendRow: %v", err)
			}
			if blk.blockRows() != a.wantRows {
				t.Errorf("blockRows() = %d, want %d", blk.blockRows(), a.wantRows)
			}
		})
	}
}

// ===========================================================================
// Bytea column round-trip
// ===========================================================================

func TestByteaRoundTrip(t *testing.T) {
	blk, _ := newBlock([]string{"data"}, []string{"bytea"})
	if err := blk.appendRow([]interface{}{[]byte{0x00, 0xFF, 0xAB}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]byte{}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]byte{0x42}}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatal(err)
	}

	_, rows := readParquetRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	got0 := rows[0][0].ByteArray()
	if len(got0) != 3 || got0[0] != 0x00 || got0[1] != 0xFF || got0[2] != 0xAB {
		t.Errorf("row 0 = %x, want 00ffab", got0)
	}
	got1 := rows[1][0].ByteArray()
	if len(got1) != 0 {
		t.Errorf("row 1 = %x, want empty", got1)
	}
	got2 := rows[2][0].ByteArray()
	if len(got2) != 1 || got2[0] != 0x42 {
		t.Errorf("row 2 = %x, want 42", got2)
	}
}

// ===========================================================================
// Bytea from string
// ===========================================================================

func TestByteaFromString(t *testing.T) {
	blk, _ := newBlock([]string{"data"}, []string{"bytea"})
	if err := blk.appendRow([]interface{}{"hello"}); err != nil {
		t.Fatal(err)
	}
	data, err := blk.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	_, rows := readParquetRows(t, data)
	if string(rows[0][0].ByteArray()) != "hello" {
		t.Errorf("got %q, want hello", rows[0][0].ByteArray())
	}
}

// ===========================================================================
// Multiple Parquet batches from same block (simulate Send-reuse-Send)
// ===========================================================================

func TestMultipleSendSimulation(t *testing.T) {
	blk, _ := newBlock([]string{"id"}, []string{"int"})
	batch := &fireboltBatch{blk: blk}

	// First "batch"
	if err := batch.Append(int32(1)); err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(int32(2)); err != nil {
		t.Fatal(err)
	}
	d1, _ := blk.toParquet()
	blk.reset()

	// Second "batch"
	if err := batch.Append(int32(10)); err != nil {
		t.Fatal(err)
	}
	d2, _ := blk.toParquet()
	blk.reset()

	_, r1 := readParquetRows(t, d1)
	_, r2 := readParquetRows(t, d2)

	if len(r1) != 2 {
		t.Errorf("batch 1: %d rows, want 2", len(r1))
	}
	if len(r2) != 1 {
		t.Errorf("batch 2: %d rows, want 1", len(r2))
	}
	if r1[0][0].Int32() != 1 || r1[1][0].Int32() != 2 {
		t.Errorf("batch 1 values wrong")
	}
	if r2[0][0].Int32() != 10 {
		t.Errorf("batch 2 values wrong")
	}
}
