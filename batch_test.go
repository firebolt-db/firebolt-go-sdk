package fireboltgosdk

import (
	"bytes"
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
	want := `INSERT INTO my_table ("col1", "col2") SELECT * FROM read_parquet('upload://batch_data')`
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
	batch.Column(0).Append([]int32{1, 2, 3})

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

	blk.appendRow([]interface{}{int32(1), "Alice", true})
	blk.appendRow([]interface{}{int32(2), "Bob", false})

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

	blk.columnAt(0).appendColumn([]int32{10, 20, 30})
	blk.columnAt(1).appendColumn([]string{"x", "y", "z"})
	blk.columnAt(2).appendColumn([]bool{false, true, false})

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

	blk.appendRow([]interface{}{int32(1), "row"})
	blk.columnAt(0).appendColumn([]int32{2, 3})
	blk.columnAt(1).appendColumn([]string{"col1", "col2"})

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

	blk.appendRow([]interface{}{int32(42)})
	blk.appendRow([]interface{}{nil})
	blk.appendRow([]interface{}{int32(7)})

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

	blk.appendRow([]interface{}{
		int32(42), int64(9999999), float32(3.14), float64(2.718),
		"hello", true, dt, ts, []byte{0xDE, 0xAD},
	})

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

	blk.appendRow([]interface{}{[]string{"a", "b", "c"}})
	blk.appendRow([]interface{}{[]string{"d"}})

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

	blk.appendRow([]interface{}{[]int32{1, 2}})
	blk.appendRow([]interface{}{[]int32{}})     // empty array
	blk.appendRow([]interface{}{[]int32{3}})

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

	blk.appendRow([]interface{}{int32(1), []string{"a", "b"}})
	blk.appendRow([]interface{}{int32(2), []string{"c"}})

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
		blk.appendRow([]interface{}{int64(1), "a"})
		blk.appendRow([]interface{}{int64(2), "b"})
		return blk
	}
	makeColBlock := func() *block {
		blk, _ := newBlock([]string{"x", "y"}, []string{"long", "text"})
		blk.columnAt(0).appendColumn([]int64{1, 2})
		blk.columnAt(1).appendColumn([]string{"a", "b"})
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

	// Read back both and compare values (not raw bytes, since metadata may differ)
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
