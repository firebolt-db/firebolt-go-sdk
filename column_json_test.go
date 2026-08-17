package fireboltgosdk

import (
	"encoding/json"
	"errors"
	"strings"
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
		{"surrounding whitespace", " \n{}\t", " \n{}\t"},
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
	if err := c.appendRow(`{"a":1}`); err != nil {
		t.Fatalf("appendRow: %v", err)
	}
	if c.rows() != 1 {
		t.Fatalf("rows() = %d before reset, want 1", c.rows())
	}
	c.reset()
	if c.rows() != 0 {
		t.Errorf("rows() = %d after reset, want 0", c.rows())
	}
}

func TestJSONColumnRejectsInvalidDocuments(t *testing.T) {
	for name, doc := range map[string]string{
		"whitespace only": " \n\t",
		"incomplete":      `{`,
		"trailing data":   `null trailing`,
		"invalid utf8":    string([]byte{'"', 0xff, '"'}),
	} {
		t.Run(name, func(t *testing.T) {
			c := &jsonColumn{colName: "attrs"}
			err := c.appendRow(doc)
			if !errors.Is(err, errInvalidJSON) {
				t.Fatalf("appendRow(%q) error = %v, want errInvalidJSON", doc, err)
			}
			if c.rows() != 0 {
				t.Errorf("invalid document left %d rows buffered, want 0", c.rows())
			}
		})
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

// TestJSONColumnRejectsEmptyValues covers the zero value of every accepted
// type. Storing "" would defer the failure to ingest, where Firebolt reports
// "Failed to parse JSON: Empty input" with no indication of which row.
func TestJSONColumnRejectsEmptyValues(t *testing.T) {
	for name, v := range map[string]interface{}{
		"nil bytes":       []byte(nil),
		"nil raw message": json.RawMessage(nil),
		"empty bytes":     []byte{},
		"empty string":    "",
	} {
		t.Run(name, func(t *testing.T) {
			c := &jsonColumn{colName: "attrs"}
			err := c.appendRow(v)
			if err == nil {
				t.Fatalf("appendRow(%#v) should be rejected; it stores an invalid JSON document", v)
			}
			if !strings.Contains(err.Error(), "empty value") {
				t.Errorf("error should explain the problem: %v", err)
			}
			if c.rows() != 0 {
				t.Errorf("a rejected value must not be stored, got %d rows", c.rows())
			}
		})
	}
}

// TestJSONArrayRejectsNullElements covers array(json null).
//
// parquet.Repeated overrides the element's repetition type, so the schema has a
// single definition level: 0 is "empty array", 1 is "element present". There is
// no level left for "present and null", and the writer emits such a value as ""
// -- which the engine rejects.
//
// The type must still be accepted, because a plain ARRAY(JSON) column reports
// as "array(json null) null" in the metadata PrepareBatch discovers. Only a
// null element is refused.
func TestJSONArrayRejectsNullElements(t *testing.T) {
	for _, typ := range []string{"array(json null)", "array(json null) null"} {
		t.Run(typ, func(t *testing.T) {
			blk, err := newBlock([]string{"docs"}, []string{typ})
			if err != nil {
				t.Fatalf("%s must be accepted, PrepareBatch discovers it: %v", typ, err)
			}

			err = blk.appendRow([]interface{}{[]interface{}{`{"a":1}`, nil}})
			if err == nil {
				t.Fatal("a null element was accepted; it cannot be encoded")
			}
			if !strings.Contains(err.Error(), "null element in a json array") {
				t.Errorf("error should say why: %v", err)
			}

			// Non-null documents must still round-trip.
			if err := blk.appendRow([]interface{}{
				[]interface{}{`{"a":1}`, `{"b":2}`},
			}); err != nil {
				t.Fatalf("appendRow: %v", err)
			}
			data, err := blk.toParquet()
			if err != nil {
				t.Fatalf("toParquet: %v", err)
			}
			f, rows := readParquetRows(t, data)
			vals := valuesFor(rows[0], colIndex(f, "docs"))
			if len(vals) != 2 {
				t.Fatalf("got %d elements, want 2: the rejected row leaked", len(vals))
			}
			for i, want := range []string{`{"a":1}`, `{"b":2}`} {
				if got := vals[i].String(); got != want {
					t.Errorf("element %d = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// A null json column, as opposed to a null element of a json array, is
// representable and must keep working.
func TestJSONColumnStillAcceptsNull(t *testing.T) {
	blk, err := newBlock([]string{"doc"}, []string{"json null"})
	if err != nil {
		t.Fatalf("newBlock: %v", err)
	}
	if err := blk.appendRow([]interface{}{nil}); err != nil {
		t.Fatalf("a null json column value must be accepted: %v", err)
	}
	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}
	f, rows := readParquetRows(t, data)
	if v := valuesFor(rows[0], colIndex(f, "doc"))[0]; !v.IsNull() {
		t.Errorf("value = %q, want null", v.String())
	}
}

func TestNullableJSONArrayRejectsNullArray(t *testing.T) {
	for _, typ := range []string{"array(json) null", "array(json null) null"} {
		t.Run(typ, func(t *testing.T) {
			col, err := newColumn("docs", typ)
			if err != nil {
				t.Fatalf("newColumn: %v", err)
			}
			if err := col.appendRow(nil); !errors.Is(err, errNullJSONArray) {
				t.Fatalf("appendRow(nil) error = %v, want errNullJSONArray", err)
			}
			if col.rows() != 0 {
				t.Fatalf("rejected null array left %d rows buffered, want 0", col.rows())
			}
			if err := col.appendRow([]string{}); err != nil {
				t.Fatalf("empty array must remain representable: %v", err)
			}
		})
	}
}

// TestJSONColumnRejectsEmptyValuesColumnar covers the columnar API, which has a
// []string fast path that bypasses appendRow.
func TestJSONColumnRejectsEmptyValuesColumnar(t *testing.T) {
	col, err := newColumn("doc", "json")
	if err != nil {
		t.Fatalf("newColumn: %v", err)
	}
	if err := col.appendColumn([]string{`{"a":1}`, "", `{"b":2}`}); err == nil {
		t.Fatal("AppendColumn accepted an empty value")
	} else if !strings.Contains(err.Error(), "[1]") {
		t.Errorf("error should name the offending index: %v", err)
	}
	if col.rows() != 0 {
		t.Errorf("rejected batch left %d rows buffered, want 0", col.rows())
	}
	if err := col.appendColumn([]string{`{"a":1}`, `{"b":2}`}); err != nil {
		t.Fatalf("valid batch: %v", err)
	}
	if col.rows() != 2 {
		t.Errorf("got %d rows, want 2", col.rows())
	}
}

func TestJSONColumnFallbackAppendIsAtomic(t *testing.T) {
	col, err := newColumn("doc", "json")
	if err != nil {
		t.Fatalf("newColumn: %v", err)
	}
	values := []json.RawMessage{
		json.RawMessage(`{"ok":true}`),
		nil,
	}
	if err := col.appendColumn(values); err == nil {
		t.Fatal("expected the nil raw message to be rejected")
	} else if !strings.Contains(err.Error(), "element [1]") {
		t.Errorf("error does not identify the rejected element: %v", err)
	}
	if got := col.rows(); got != 0 {
		t.Errorf("failed append retained %d rows, want 0", got)
	}
}

func TestJSONArrayColumnAppendIsAtomic(t *testing.T) {
	col, err := newColumn("docs", "array(json null)")
	if err != nil {
		t.Fatalf("newColumn: %v", err)
	}
	values := [][]interface{}{
		{`{"first":true}`},
		{`{"second":true}`, nil},
	}
	err = col.appendColumn(values)
	if err == nil {
		t.Fatal("expected the null json array element to be rejected")
	}
	for _, index := range []string{"row [1]", "element [1]"} {
		if !strings.Contains(err.Error(), index) {
			t.Errorf("error does not contain %q: %v", index, err)
		}
	}
	if got := col.rows(); got != 0 {
		t.Errorf("failed append retained %d complete rows, want 0", got)
	}
}
