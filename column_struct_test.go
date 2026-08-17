package fireboltgosdk

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStructArrayColumnType(t *testing.T) {
	col, err := newColumn("events", "array(struct(name text, count int))")
	if err != nil {
		t.Fatalf("array(struct(...)) should be supported: %v", err)
	}
	sc, ok := col.(*structArrayColumn)
	if !ok {
		t.Fatalf("newColumn returned %T, want *structArrayColumn", col)
	}
	if got := sc.fields; len(got) != 2 || got[0] != "name" || got[1] != "count" {
		t.Errorf("fields = %v, want [name count]", got)
	}
}

func TestStructArrayRejectsUnsupportedShapes(t *testing.T) {
	tests := map[string]string{
		"bare struct":            "struct(a text)",
		"nested array in struct": "array(struct(a array(text)))",
		"nested struct":          "array(struct(a struct(b text)))",
		"empty field list":       "array(struct())",
		"field without type":     "array(struct(a))",
		"duplicate field":        "array(struct(a text, a int))",
		"unknown field type":     "array(struct(a widget))",
	}
	for name, typ := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := newColumn("c", typ); err == nil {
				t.Errorf("%s should be rejected", typ)
			}
		})
	}
}

func TestParseStructFields(t *testing.T) {
	fields, err := parseStructFields("ts timestampntz, name text, attrs json")
	if err != nil {
		t.Fatal(err)
	}
	want := []structField{
		{"ts", "timestampntz"}, {"name", "text"}, {"attrs", "json"},
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d", len(fields), len(want))
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Errorf("field %d = %+v, want %+v", i, fields[i], want[i])
		}
	}
}

func TestParseStructFieldsWithQuotedNames(t *testing.T) {
	fields, err := parseStructFields("`a b` int, `c,d(e)` text null, plain json")
	if err != nil {
		t.Fatal(err)
	}
	want := []structField{
		{"a b", "int"}, {"c,d(e)", "text null"}, {"plain", "json"},
	}
	if len(fields) != len(want) {
		t.Fatalf("got %d fields, want %d: %+v", len(fields), len(want), fields)
	}
	for i := range want {
		if fields[i] != want[i] {
			t.Errorf("field %d = %+v, want %+v", i, fields[i], want[i])
		}
	}

	if _, err := newColumn("events", "array(struct(`a b` int null))"); err != nil {
		t.Fatalf("quoted field name from metadata should be supported: %v", err)
	}
	if _, err := parseStructFields("`unterminated int"); err == nil {
		t.Error("unterminated quoted field name should be rejected")
	}
}

func TestStructArrayQuotedNamesRoundTrip(t *testing.T) {
	blk, err := newBlock(
		[]string{"events"},
		[]string{"array(struct(`a b` int null, `c,d(e)` text null))"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]map[string]interface{}{
		{"a b": int32(7), "c,d(e)": "kept"},
	}}); err != nil {
		t.Fatal(err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatal(err)
	}
	f, rows := readParquetRows(t, data)
	if got := valuesFor(rows[0], colIndex(f, "events", "a b")); len(got) != 1 || got[0].Int32() != 7 {
		t.Fatalf("quoted integer field = %v, want 7", got)
	}
	if got := valuesFor(rows[0], colIndex(f, "events", "c,d(e)")); len(got) != 1 || string(got[0].ByteArray()) != "kept" {
		t.Fatalf("quoted text field = %v, want kept", got)
	}
}

// TestSplitTopLevelIgnoresNestedCommas is why splitting is depth-aware: a field
// type containing a comma must not split the field list.
func TestSplitTopLevelIgnoresNestedCommas(t *testing.T) {
	parts, err := splitTopLevel("a text, b decimal(10, 2), c int")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 3 {
		t.Fatalf("got %d parts %q, want 3", len(parts), parts)
	}
	if strings.TrimSpace(parts[1]) != "b decimal(10, 2)" {
		t.Errorf("part 1 = %q", parts[1])
	}

	if _, err := splitTopLevel("a text)"); err == nil {
		t.Error("unbalanced parentheses should be rejected")
	}
}

// TestStructArrayRoundTrip is the test that matters: values must survive
// serialization with correct repetition levels. A levels bug here corrupts data
// silently, so every row shape is asserted element by element.
func TestStructArrayRoundTrip(t *testing.T) { // NOSONAR - all row shapes share one serialization lifecycle.
	blk, err := newBlock(
		[]string{"id", "events"},
		[]string{"int", "array(struct(name text, count long))"})
	if err != nil {
		t.Fatalf("newBlock: %v", err)
	}

	// Row shapes chosen to exercise the encoding: several elements, exactly
	// one, and none at all. Empty is the case most likely to be mis-encoded.
	rows := [][]map[string]interface{}{
		{{"name": "a", "count": int64(1)}, {"name": "b", "count": int64(2)}},
		{{"name": "solo", "count": int64(7)}},
		{},
		{{"name": "last", "count": int64(9)}},
	}
	for i, r := range rows {
		if err := blk.appendRow([]interface{}{int32(i), r}); err != nil {
			t.Fatalf("appendRow %d: %v", i, err)
		}
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}

	f, got := readParquetRows(t, data)
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}

	idCol := colIndex(f, "id")
	nameCol := colIndex(f, "events", "name")
	countCol := colIndex(f, "events", "count")

	for r, want := range rows {
		if gotID := valuesFor(got[r], idCol); len(gotID) != 1 || gotID[0].Int32() != int32(r) {
			t.Errorf("row %d id = %v", r, gotID)
		}

		names := valuesFor(got[r], nameCol)
		counts := valuesFor(got[r], countCol)

		if len(want) == 0 {
			// An empty repeated group still occupies one value slot, carrying a
			// null rather than an element.
			if len(names) != 1 || !names[0].IsNull() {
				t.Errorf("row %d: empty events should be one null, got %v", r, names)
			}
			continue
		}

		if len(names) != len(want) || len(counts) != len(want) {
			t.Fatalf("row %d: got %d names and %d counts, want %d of each",
				r, len(names), len(counts), len(want))
		}
		for e, wantElem := range want {
			if got := string(names[e].ByteArray()); got != wantElem["name"] {
				t.Errorf("row %d element %d name = %q, want %q", r, e, got, wantElem["name"])
			}
			if got := counts[e].Int64(); got != wantElem["count"] {
				t.Errorf("row %d element %d count = %d, want %d", r, e, got, wantElem["count"])
			}
		}
	}
}

// TestStructArrayRejectsMissingField pins the rule that every field must be
// supplied. The array encoding cannot represent a null element distinctly from a
// present one, so accepting nil would write a zero that reads as real data.
func TestStructArrayRejectsMissingField(t *testing.T) {
	for _, typ := range []string{
		"array(struct(name text, count long))",
		// Nullable declarations make no difference: the encoding is the same.
		"array(struct(name text null, count long null))",
	} {
		t.Run(typ, func(t *testing.T) {
			blk, err := newBlock([]string{"events"}, []string{typ})
			if err != nil {
				t.Fatal(err)
			}
			err = blk.appendRow([]interface{}{
				[]map[string]interface{}{{"name": "only-name"}},
			})
			if err == nil {
				t.Fatal("omitting a struct field should be an error")
			}
			if !strings.Contains(err.Error(), "count") {
				t.Errorf("error should name the missing field: %v", err)
			}
		})
	}

	blk, err := newBlock([]string{"events"}, []string{"array(struct(name text))"})
	if err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{
		[]map[string]interface{}{{"name": nil}},
	}); err == nil {
		t.Error("an explicit nil field should be an error")
	}
}

func TestStructArrayRejectsUnknownField(t *testing.T) {
	col, err := newColumn("events", "array(struct(name text null))")
	if err != nil {
		t.Fatal(err)
	}
	err = col.appendRow([]map[string]interface{}{{"name": "kept", "typo": "must not disappear"}})
	if err == nil {
		t.Fatal("unknown field was silently discarded")
	}
	for _, want := range []string{"typo", "element 0"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not identify %q", err, want)
		}
	}
	if col.rows() != 0 {
		t.Fatalf("rejected row left %d rows buffered", col.rows())
	}
}

func TestStructArrayRejectsNullArray(t *testing.T) {
	for _, typ := range []string{
		"array(struct(name text null))",
		"array(struct(name text null) null) null",
	} {
		t.Run(typ, func(t *testing.T) {
			col, err := newColumn("events", typ)
			if err != nil {
				t.Fatal(err)
			}
			if err := col.appendRow(nil); !errors.Is(err, errNullStructArray) {
				t.Fatalf("appendRow(nil) error = %v, want errNullStructArray", err)
			}
			if col.rows() != 0 {
				t.Fatalf("rejected null left %d rows buffered", col.rows())
			}
			if err := col.appendRow([]interface{}{nil}); err == nil {
				t.Fatal("null struct element should be rejected")
			}
			if col.rows() != 0 {
				t.Fatalf("rejected null element left %d rows buffered", col.rows())
			}
			if err := col.appendRow([]map[string]interface{}(nil)); err != nil {
				t.Fatalf("typed nil slice should represent []: %v", err)
			}
			if col.rows() != 1 {
				t.Fatalf("empty slice left %d rows, want 1", col.rows())
			}
		})
	}
}

// TestStructArrayWithJSONField covers the shape the OTel exporter uses for span
// events: scalars plus a JSON field carrying the attributes.
func TestStructArrayWithJSONField(t *testing.T) {
	blk, err := newBlock(
		[]string{"events"},
		[]string{"array(struct(ts timestampntz null, name text null, attributes json null) null) null"})
	if err != nil {
		t.Fatal(err)
	}

	when := time.Date(2026, 8, 12, 10, 30, 0, 0, time.UTC)
	if err := blk.appendRow([]interface{}{
		[]map[string]interface{}{
			{"ts": when, "name": "exception", "attributes": `{"type":"IOError"}`},
			{"ts": when, "name": "sql-null", "attributes": nil},
			{"ts": when, "name": "json-null", "attributes": "null"},
		},
	}); err != nil {
		t.Fatalf("appendRow: %v", err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}
	f, rows := readParquetRows(t, data)

	attrs := valuesFor(rows[0], colIndex(f, "events", "attributes"))
	if len(attrs) != 3 || string(attrs[0].ByteArray()) != `{"type":"IOError"}` ||
		!attrs[1].IsNull() || string(attrs[2].ByteArray()) != "null" {
		t.Errorf("attributes = %v", attrs)
	}
	names := valuesFor(rows[0], colIndex(f, "events", "name"))
	if len(names) != 3 || string(names[0].ByteArray()) != "exception" ||
		string(names[1].ByteArray()) != "sql-null" || string(names[2].ByteArray()) != "json-null" {
		t.Errorf("name = %v", names)
	}
}

// TestStructArrayResetAndReuse covers batch reuse: Send resets the block, and a
// stale offsets slice would corrupt the next batch.
func TestStructArrayResetAndReuse(t *testing.T) {
	blk, err := newBlock([]string{"events"}, []string{"array(struct(name text))"})
	if err != nil {
		t.Fatal(err)
	}

	appendAndCheck := func(label, want string) {
		t.Helper()
		if err := blk.appendRow([]interface{}{
			[]map[string]interface{}{{"name": want}},
		}); err != nil {
			t.Fatalf("%s appendRow: %v", label, err)
		}
		data, err := blk.toParquet()
		if err != nil {
			t.Fatalf("%s toParquet: %v", label, err)
		}
		f, rows := readParquetRows(t, data)
		if len(rows) != 1 {
			t.Fatalf("%s: read %d rows, want 1", label, len(rows))
		}
		names := valuesFor(rows[0], colIndex(f, "events", "name"))
		if len(names) != 1 || string(names[0].ByteArray()) != want {
			t.Errorf("%s: name = %v, want %q", label, names, want)
		}
	}

	appendAndCheck("first batch", "one")
	blk.reset()
	if blk.blockRows() != 0 {
		t.Fatalf("blockRows = %d after reset, want 0", blk.blockRows())
	}
	appendAndCheck("second batch", "two")
}

// TestStructArrayValidateRowCount covers a struct column staying in step with
// scalar columns, which block.validate relies on.
func TestStructArrayValidateRowCount(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "events"},
		[]string{"int", "array(struct(name text))"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.columnAt(0).appendColumn([]int32{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := blk.validate(); err == nil {
		t.Error("a block with 2 ids and 0 events should not validate")
	}

	for i := 0; i < 2; i++ {
		if err := blk.columnAt(1).appendRow([]map[string]interface{}{{"name": "x"}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := blk.validate(); err != nil {
		t.Errorf("block should validate once row counts match: %v", err)
	}
}

// TestStructArrayAppendIsAtomic covers a partially applied row. Fields are
// separate Parquet leaves, so leaving some longer than others desynchronises
// their offsets: rows() follows the first field, block.validate passes when this
// is the only column, and the writer then panics indexing a shorter slice.
func TestStructArrayAppendIsAtomic(t *testing.T) {
	blk, err := newBlock([]string{"events"}, []string{"array(struct(name text, count long))"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{
		[]map[string]interface{}{{"name": "good", "count": int64(1)}},
	}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// count is not convertible, so the second field fails after the first has
	// already been written.
	err = blk.appendRow([]interface{}{
		[]map[string]interface{}{{"name": "bad", "count": "not-a-number"}},
	})
	if err == nil {
		t.Fatal("appending an unconvertible field value should fail")
	}

	sc := blk.columnAt(0).(*structArrayColumn)
	for i, field := range sc.fields {
		if got := sc.elems[i].rows(); got != 1 {
			t.Errorf("field %q has %d rows after a failed append, want 1: "+
				"the row was applied to some fields but not others", field, got)
		}
	}

	// The block must still serialise. Before the fix this panicked, because
	// leaf 0 carried two rows of offsets while leaf 1 carried one.
	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet after a failed append: %v", err)
	}
	f, rows := readParquetRows(t, data)
	if len(rows) != 1 {
		t.Fatalf("read %d rows, want 1", len(rows))
	}
	if got := string(valuesFor(rows[0], colIndex(f, "events", "name"))[0].ByteArray()); got != "good" {
		t.Errorf("surviving row = %q, want good", got)
	}
}

// TestStructArrayMissingFieldIsAtomic covers the other failure path, where the
// row is rejected for a missing field after earlier fields were written.
func TestStructArrayMissingFieldIsAtomic(t *testing.T) {
	blk, err := newBlock([]string{"events"}, []string{"array(struct(name text, count long))"})
	if err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{
		[]map[string]interface{}{{"name": "only-name"}},
	}); err == nil {
		t.Fatal("omitting a field should fail")
	}

	sc := blk.columnAt(0).(*structArrayColumn)
	for i, field := range sc.fields {
		if got := sc.elems[i].rows(); got != 0 {
			t.Errorf("field %q has %d rows after a rejected append, want 0", field, got)
		}
	}
}

// TestStructArrayRejectsNesting covers array(struct(...)) inside another array.
//
// Two levels of repetition need repetition levels this encoding does not
// produce. structArrayFields only matches a top-level struct array, so without
// an explicit guard the ordinary array path builds an arrayColumn over a
// structArrayColumn: the column constructs, rows append, and the failure
// surfaces only when serialising -- as a panic, because structArrayColumn
// returns no element values and the array path then mistakes it for a fusable
// element type.
func TestStructArrayRejectsNesting(t *testing.T) {
	for _, typ := range []string{
		"array(array(struct(a text)))",
		"array(array(struct(a text)) null)",
	} {
		t.Run(typ, func(t *testing.T) {
			col, err := newColumn("v", typ)
			if err == nil {
				t.Fatalf("%s was accepted, building %T; it cannot be encoded", typ, col)
			}
			if !strings.Contains(err.Error(), "cannot be nested") {
				t.Errorf("error should say why: %v", err)
			}
		})
	}

	// The supported shape must keep working.
	if _, err := newColumn("v", "array(struct(a text))"); err != nil {
		t.Errorf("array(struct(...)) must still be supported: %v", err)
	}
}

// TestStructArrayColumnarAndZero covers the struct-array paths the row-wise
// tests never reach: the columnar API, the empty-array zero value, and
// truncate.
func TestStructArrayColumnarAndZero(t *testing.T) { // NOSONAR - subtests cover the complete struct-array column lifecycle.
	t.Run("appendColumn", func(t *testing.T) {
		col, err := newColumn("events", "array(struct(name text, n int))")
		if err != nil {
			t.Fatalf("newColumn: %v", err)
		}
		// One slice entry per row, each row a slice of elements.
		err = col.appendColumn([]interface{}{
			[]interface{}{
				map[string]interface{}{"name": "a", "n": int32(1)},
			},
			[]interface{}{
				map[string]interface{}{"name": "b", "n": int32(2)},
				map[string]interface{}{"name": "c", "n": int32(3)},
			},
		})
		if err != nil {
			t.Fatalf("appendColumn: %v", err)
		}
		if got := col.rows(); got != 2 {
			t.Fatalf("rows() = %d, want 2", got)
		}

		// An element that fails must leave the column as it was.
		before := col.rows()
		if err := col.appendColumn([]interface{}{
			[]interface{}{map[string]interface{}{"name": "d"}}, // n missing
		}); err == nil {
			t.Fatal("expected an error for the missing field")
		}
		if got := col.rows(); got != before {
			t.Errorf("rows() = %d after a failed appendColumn, want %d", got, before)
		}
	})

	t.Run("appendZero", func(t *testing.T) {
		col, err := newColumn("events", "array(struct(name text, n int))")
		if err != nil {
			t.Fatalf("newColumn: %v", err)
		}
		col.appendZero()
		if got := col.rows(); got != 1 {
			t.Fatalf("rows() = %d after appendZero, want 1", got)
		}
		// The zero value is an empty array, so every field holds an empty row.
		sc := col.(*structArrayColumn)
		for i, e := range sc.elems {
			if got := e.rows(); got != 1 {
				t.Errorf("field %q has %d rows, want 1", sc.fields[i], got)
			}
			if got := e.elem.rows(); got != 0 {
				t.Errorf("field %q holds %d elements, want 0", sc.fields[i], got)
			}
		}
	})

	t.Run("truncate", func(t *testing.T) {
		col, err := newColumn("events", "array(struct(name text, n int))")
		if err != nil {
			t.Fatalf("newColumn: %v", err)
		}
		for _, name := range []string{"a", "b", "c"} {
			if err := col.appendRow([]interface{}{
				map[string]interface{}{"name": name, "n": int32(1)},
			}); err != nil {
				t.Fatalf("appendRow: %v", err)
			}
		}
		col.truncate(1)
		if got := col.rows(); got != 1 {
			t.Fatalf("rows() = %d after truncate(1), want 1", got)
		}
		// Every field must be trimmed, not just the first, and the elements
		// the dropped rows held must go with them.
		sc := col.(*structArrayColumn)
		for i, e := range sc.elems {
			if got := e.rows(); got != 1 {
				t.Errorf("field %q has %d rows after truncate, want 1", sc.fields[i], got)
			}
			if got := e.elem.rows(); got != 1 {
				t.Errorf("field %q holds %d elements after truncate, want 1", sc.fields[i], got)
			}
		}
	})
}

// TestStructArrayNullFields covers a null struct field.
//
// Firebolt requires struct fields to be nullable -- STRUCT(x TEXT NOT NULL) is
// rejected outright -- so every struct array has nullable fields and this is
// the ordinary case. A nullable field sits in `repeated group g { optional f }`
// where def 0 is an empty group, 1 is "element present, field null" and 2 is
// "field present", so the null must stay a level below its neighbours rather
// than being lifted with them.
//
// The empty string is here deliberately: it is a present value that carries no
// bytes, and encoding it as a null would be invisible to an element count.
func TestStructArrayNullFields(t *testing.T) {
	blk, err := newBlock([]string{"events"},
		[]string{"array(struct(name text null, tag text null))"})
	if err != nil {
		t.Fatalf("newBlock: %v", err)
	}
	if err := blk.appendRow([]interface{}{[]interface{}{
		map[string]interface{}{"name": "a", "tag": "x"},
		map[string]interface{}{"name": "b", "tag": nil},
		map[string]interface{}{"name": "c", "tag": ""},
	}}); err != nil {
		t.Fatalf("appendRow: %v", err)
	}

	data, err := blk.toParquet()
	if err != nil {
		t.Fatalf("toParquet: %v", err)
	}
	f, rows := readParquetRows(t, data)

	tags := valuesFor(rows[0], colIndex(f, "events", "tag"))
	if len(tags) != 3 {
		t.Fatalf("got %d tag values, want 3", len(tags))
	}
	for i, want := range []struct {
		isNull bool
		val    string
	}{{false, "x"}, {true, ""}, {false, ""}} {
		if got := tags[i].IsNull(); got != want.isNull {
			t.Errorf("tag %d: IsNull() = %v, want %v (value %q at definition level %d)",
				i, got, want.isNull, tags[i].String(), tags[i].DefinitionLevel())
		}
		if !want.isNull && tags[i].String() != want.val {
			t.Errorf("tag %d = %q, want %q", i, tags[i].String(), want.val)
		}
	}

	// A missing key is still an error: it usually means a misspelled field,
	// and treating it as null would hide that.
	if err := blk.appendRow([]interface{}{[]interface{}{
		map[string]interface{}{"name": "d"},
	}}); err == nil {
		t.Error("a missing field was accepted; only an explicit nil means null")
	}
}
