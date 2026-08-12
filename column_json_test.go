package fireboltgosdk

import (
	"encoding/json"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestJSONColumnType(t *testing.T) {
	col, err := newColumn("attrs", "json")
	if err != nil {
		t.Fatalf("json should be a supported batch column type: %v", err)
	}
	if _, ok := col.(*jsonColumn); !ok {
		t.Fatalf("newColumn returned %T, want *jsonColumn", col)
	}
}

func TestJSONColumnNullable(t *testing.T) {
	col, err := newColumn("attrs", "json null")
	if err != nil {
		t.Fatalf("nullable json should be supported: %v", err)
	}
	nc, ok := col.(*nullableColumn)
	if !ok {
		t.Fatalf("newColumn returned %T, want *nullableColumn", col)
	}
	if _, ok := nc.inner.(*jsonColumn); !ok {
		t.Fatalf("inner column is %T, want *jsonColumn", nc.inner)
	}
}

// TestJSONColumnLogicalType covers the point of the whole type: a plain UTF8
// string node would not tell the engine the column is JSON.
func TestJSONColumnLogicalType(t *testing.T) {
	c := &jsonColumn{colName: "attrs"}
	node := c.parquetNode()

	lt := node.Type().LogicalType()
	if lt == nil || lt.Json == nil {
		t.Fatalf("parquetNode() logical type = %+v, want JSON", lt)
	}
	if got := node.Type().Kind(); got != parquet.ByteArray {
		t.Errorf("physical kind = %v, want ByteArray", got)
	}
}

func TestJSONColumnAppendRow(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"string", `{"a":1}`, `{"a":1}`},
		{"bytes", []byte(`{"b":2}`), `{"b":2}`},
		{"raw message", json.RawMessage(`{"c":3}`), `{"c":3}`},
		{"empty object", `{}`, `{}`},
		{"array document", `[1,2,3]`, `[1,2,3]`},
		{"scalar document", `"just a string"`, `"just a string"`},
		{"null document", `null`, `null`},
		{"unicode", `{"k":"日本語 🔥"}`, `{"k":"日本語 🔥"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &jsonColumn{colName: "attrs"}
			if err := c.appendRow(tt.value); err != nil {
				t.Fatalf("appendRow(%v): %v", tt.value, err)
			}
			if c.rows() != 1 {
				t.Fatalf("rows() = %d, want 1", c.rows())
			}
			if c.data[0] != tt.want {
				t.Errorf("stored %q, want %q", c.data[0], tt.want)
			}
		})
	}
}

func TestJSONColumnAppendRowRejectsNonJSON(t *testing.T) {
	c := &jsonColumn{colName: "attrs"}
	if err := c.appendRow(42); err == nil {
		t.Error("appendRow(int) should fail; JSON columns take text")
	}
}

func TestJSONColumnAppendColumn(t *testing.T) {
	c := &jsonColumn{colName: "attrs"}
	if err := c.appendColumn([]string{`{"a":1}`, `{"b":2}`}); err != nil {
		t.Fatalf("appendColumn: %v", err)
	}
	if c.rows() != 2 {
		t.Fatalf("rows() = %d, want 2", c.rows())
	}
	if c.data[0] != `{"a":1}` || c.data[1] != `{"b":2}` {
		t.Errorf("stored %v", c.data)
	}
}

// TestJSONColumnAppendZeroIsValidJSON matters because appendZero fills gaps in
// mixed row/columnar batches. An empty string is not a valid JSON document and
// would fail at ingest, far from the cause.
func TestJSONColumnAppendZeroIsValidJSON(t *testing.T) {
	c := &jsonColumn{colName: "attrs"}
	c.appendZero()

	if c.data[0] != "{}" {
		t.Errorf("appendZero stored %q, want %q", c.data[0], "{}")
	}
	var v interface{}
	if err := json.Unmarshal([]byte(c.data[0]), &v); err != nil {
		t.Errorf("appendZero produced invalid JSON %q: %v", c.data[0], err)
	}
}

func TestJSONColumnReset(t *testing.T) {
	c := &jsonColumn{colName: "attrs"}
	_ = c.appendRow(`{"a":1}`)
	c.reset()
	if c.rows() != 0 {
		t.Errorf("rows() = %d after reset, want 0", c.rows())
	}
}

// TestJSONRoundTrip serialises a block containing a JSON column and reads it
// back, which is the path a real batch insert takes.
func TestJSONRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"id", "attrs"}, []string{"int", "json"})
	if err != nil {
		t.Fatalf("newBlock: %v", err)
	}

	docs := []string{
		`{"service.name":"checkout","http.status":200}`,
		`{}`,
		`{"nested":{"a":[1,2,3]},"unicode":"日本語"}`,
	}
	for i, doc := range docs {
		if err := blk.appendRow([]interface{}{int32(i), doc}); err != nil {
			t.Fatalf("appendRow %d: %v", i, err)
		}
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != len(docs) {
		t.Fatalf("read %d rows, want %d", len(rows), len(docs))
	}

	attrsCol := colIndex(f, "attrs")
	for i, want := range docs {
		if got := string(rows[i][attrsCol].ByteArray()); got != want {
			t.Errorf("row %d attrs = %q, want %q", i, got, want)
		}
	}

	// The written schema must carry the JSON logical type, or the engine sees
	// an ordinary string column.
	var found bool
	for _, field := range f.Schema().Fields() {
		if field.Name() == "attrs" {
			found = true
			if lt := field.Type().LogicalType(); lt == nil || lt.Json == nil {
				t.Errorf("attrs logical type = %+v, want JSON", lt)
			}
		}
	}
	if !found {
		t.Error("attrs column missing from the written schema")
	}
}

func TestJSONColumnarRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"id", "attrs"}, []string{"int", "json"})
	if err != nil {
		t.Fatalf("newBlock: %v", err)
	}

	if err := blk.columnAt(0).appendColumn([]int32{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := blk.columnAt(1).appendColumn([]string{`{"a":1}`, `{"b":2}`}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, rows := readParquetRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("read %d rows, want 2", len(rows))
	}
	attrsCol := colIndex(f, "attrs")
	if got := string(rows[0][attrsCol].ByteArray()); got != `{"a":1}` {
		t.Errorf("row 0 = %q", got)
	}
	if got := string(rows[1][attrsCol].ByteArray()); got != `{"b":2}` {
		t.Errorf("row 1 = %q", got)
	}
}
