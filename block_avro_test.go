package fireboltgosdk

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// readAvroRows decodes all rows from an Avro OCF byte slice into
// []map[string]interface{}. The schema is read from the OCF header.
func readAvroRows(t *testing.T, data []byte) []map[string]interface{} {
	t.Helper()
	dec, err := ocf.NewDecoder(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ocf.NewDecoder: %v", err)
	}

	var rows []map[string]interface{}
	for dec.HasNext() {
		var row map[string]interface{}
		if err := dec.Decode(&row); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		rows = append(rows, row)
	}
	if err := dec.Error(); err != nil {
		t.Fatalf("decoder error: %v", err)
	}
	return rows
}

// avroRoundTrip serialises a block using Avro and returns the raw bytes.
func avroRoundTrip(t *testing.T, blk *block) []byte {
	t.Helper()
	blk.format = FormatAvro
	r, err := blk.NewReader()
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	return buf.Bytes()
}

func TestAvroScalarRoundTrip(t *testing.T) {
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

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]

	if got, ok := row["a_int"].(int); !ok || got != 42 {
		t.Errorf("a_int = %v, want 42", row["a_int"])
	}
	if got, ok := row["b_long"].(int64); !ok || got != 9999999 {
		t.Errorf("b_long = %v, want 9999999", row["b_long"])
	}
	if got, ok := row["c_float"].(float32); !ok || got != 3.14 {
		t.Errorf("c_float = %v, want 3.14", row["c_float"])
	}
	if got, ok := row["d_double"].(float64); !ok || got != 2.718 {
		t.Errorf("d_double = %v, want 2.718", row["d_double"])
	}
	if got, ok := row["e_text"].(string); !ok || got != "hello" {
		t.Errorf("e_text = %v, want hello", row["e_text"])
	}
	if got, ok := row["f_bool"].(bool); !ok || got != true {
		t.Errorf("f_bool = %v, want true", row["f_bool"])
	}

	if got, ok := row["g_date"].(time.Time); !ok || !got.Equal(dt) {
		t.Errorf("g_date = %v (%T), want %v", row["g_date"], row["g_date"], dt)
	}
	if got, ok := row["h_ts"].(time.Time); !ok || !got.Equal(ts) {
		t.Errorf("h_ts = %v (%T), want %v", row["h_ts"], row["h_ts"], ts)
	}
	if got, ok := row["i_bytea"].([]byte); !ok || len(got) != 2 || got[0] != 0xDE || got[1] != 0xAD {
		t.Errorf("i_bytea = %v, want [DE AD]", row["i_bytea"])
	}
}

func TestAvroNullableRoundTrip(t *testing.T) {
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

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Row 0: 42
	if got, ok := rows[0]["val"].(int); !ok || got != 42 {
		t.Errorf("row 0 val = %v (%T), want 42", rows[0]["val"], rows[0]["val"])
	}

	// Row 1: NULL
	if rows[1]["val"] != nil {
		t.Errorf("row 1: expected nil, got %v", rows[1]["val"])
	}

	// Row 2: 7
	if got, ok := rows[2]["val"].(int); !ok || got != 7 {
		t.Errorf("row 2 val = %v (%T), want 7", rows[2]["val"], rows[2]["val"])
	}
}

func TestAvroArrayRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"tags"}, []string{"array(text)"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{[]string{"a", "b", "c"}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]string{}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]string{"d"}}); err != nil {
		t.Fatal(err)
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	r0 := rows[0]["tags"].([]interface{})
	if len(r0) != 3 || r0[0] != "a" || r0[1] != "b" || r0[2] != "c" {
		t.Errorf("row 0 tags = %v, want [a b c]", r0)
	}

	r1 := rows[1]["tags"].([]interface{})
	if len(r1) != 0 {
		t.Errorf("row 1 tags = %v, want []", r1)
	}

	r2 := rows[2]["tags"].([]interface{})
	if len(r2) != 1 || r2[0] != "d" {
		t.Errorf("row 2 tags = %v, want [d]", r2)
	}
}

func TestAvroMixedColumnsRoundTrip(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "tags", "score"},
		[]string{"int", "array(text)", "double null"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{int32(1), []string{"x", "y"}, float64(9.5)}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(2), []string{}, nil}); err != nil {
		t.Fatal(err)
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	if got, ok := rows[0]["id"].(int); !ok || got != 1 {
		t.Errorf("row 0 id = %v, want 1", rows[0]["id"])
	}
	tags0 := rows[0]["tags"].([]interface{})
	if len(tags0) != 2 || tags0[0] != "x" || tags0[1] != "y" {
		t.Errorf("row 0 tags = %v, want [x y]", tags0)
	}

	if got, ok := rows[1]["id"].(int); !ok || got != 2 {
		t.Errorf("row 1 id = %v, want 2", rows[1]["id"])
	}
	if rows[1]["score"] != nil {
		t.Errorf("row 1 score = %v, want nil", rows[1]["score"])
	}
}

func TestAvroLargeBatchRoundTrip(t *testing.T) {
	const n = 5000
	blk, err := newBlock([]string{"id", "val"}, []string{"int", "text"})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		if err := blk.appendRow([]interface{}{int32(i), fmt.Sprintf("row_%d", i)}); err != nil {
			t.Fatalf("appendRow %d: %v", i, err)
		}
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != n {
		t.Fatalf("expected %d rows, got %d", n, len(rows))
	}

	for i := 0; i < n; i++ {
		if got := rows[i]["id"].(int); got != i {
			t.Errorf("row %d id = %d, want %d", i, got, i)
		}
		want := fmt.Sprintf("row_%d", i)
		if got := rows[i]["val"].(string); got != want {
			t.Errorf("row %d val = %q, want %q", i, got, want)
		}
	}
}

func TestAvroEmptyBlock(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	blk.format = FormatAvro
	r, err := blk.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected empty output, got %d bytes", buf.Len())
	}
}

func TestAvroSchemaJSON(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "name", "tags", "score"},
		[]string{"int", "text", "array(text)", "double null"})
	if err != nil {
		t.Fatal(err)
	}

	js, err := blk.avroSchemaJSON()
	if err != nil {
		t.Fatal(err)
	}

	schema, err := avro.Parse(js)
	if err != nil {
		t.Fatalf("avro.Parse: %v", err)
	}

	if schema.Type() != avro.Record {
		t.Errorf("schema type = %v, want record", schema.Type())
	}
}

func TestBuildAvroInsertQuery(t *testing.T) {
	got := buildAvroInsertQuery("my_table", []string{"col2", "col1"}, "batch_data")
	want := `INSERT INTO "my_table" ("col2", "col1") SELECT * FROM read_avro(URL => 'upload://batch_data')`
	if got != want {
		t.Errorf("buildAvroInsertQuery = %q, want %q", got, want)
	}
}

func TestBuildAvroInsertQueryPreservesOrder(t *testing.T) {
	got := buildAvroInsertQuery("t", []string{"z", "a", "m"}, "data")
	want := `INSERT INTO "t" ("z", "a", "m") SELECT * FROM read_avro(URL => 'upload://data')`
	if got != want {
		t.Errorf("buildAvroInsertQuery order = %q, want %q", got, want)
	}
}

func TestAvroNullableAllTypesRoundTrip(t *testing.T) {
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	dt := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

	blk, err := newBlock(
		[]string{"ni", "nl", "nf", "nd", "nt", "nb", "ndt", "nts", "nby"},
		[]string{"int null", "long null", "float null", "double null", "text null", "boolean null", "date null", "timestamp null", "bytea null"})
	if err != nil {
		t.Fatal(err)
	}

	// Row 0: all non-null
	if err := blk.appendRow([]interface{}{
		int32(42), int64(99), float32(1.5), float64(2.5),
		"hello", true, dt, ts, []byte{0xAB},
	}); err != nil {
		t.Fatal(err)
	}

	// Row 1: all null
	if err := blk.appendRow([]interface{}{nil, nil, nil, nil, nil, nil, nil, nil, nil}); err != nil {
		t.Fatal(err)
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Row 0: verify each non-null value
	if got, ok := rows[0]["ni"].(int); !ok || got != 42 {
		t.Errorf("row 0 ni = %v (%T), want 42", rows[0]["ni"], rows[0]["ni"])
	}
	if got, ok := rows[0]["nl"].(int64); !ok || got != 99 {
		t.Errorf("row 0 nl = %v (%T), want 99", rows[0]["nl"], rows[0]["nl"])
	}
	if got, ok := rows[0]["nf"].(float32); !ok || got != 1.5 {
		t.Errorf("row 0 nf = %v (%T), want 1.5", rows[0]["nf"], rows[0]["nf"])
	}
	if got, ok := rows[0]["nd"].(float64); !ok || got != 2.5 {
		t.Errorf("row 0 nd = %v (%T), want 2.5", rows[0]["nd"], rows[0]["nd"])
	}
	if got, ok := rows[0]["nt"].(string); !ok || got != "hello" {
		t.Errorf("row 0 nt = %v (%T), want hello", rows[0]["nt"], rows[0]["nt"])
	}
	if got, ok := rows[0]["nb"].(bool); !ok || got != true {
		t.Errorf("row 0 nb = %v (%T), want true", rows[0]["nb"], rows[0]["nb"])
	}
	if got, ok := rows[0]["ndt"].(time.Time); !ok || !got.Equal(dt) {
		t.Errorf("row 0 ndt = %v (%T), want %v", rows[0]["ndt"], rows[0]["ndt"], dt)
	}
	if got, ok := rows[0]["nts"].(time.Time); !ok || !got.Equal(ts) {
		t.Errorf("row 0 nts = %v (%T), want %v", rows[0]["nts"], rows[0]["nts"], ts)
	}
	if got, ok := rows[0]["nby"].([]byte); !ok || len(got) != 1 || got[0] != 0xAB {
		t.Errorf("row 0 nby = %v (%T), want [AB]", rows[0]["nby"], rows[0]["nby"])
	}

	// Row 1: all null
	for _, name := range []string{"ni", "nl", "nf", "nd", "nt", "nb", "ndt", "nts", "nby"} {
		if rows[1][name] != nil {
			t.Errorf("row 1 %s: expected nil, got %v", name, rows[1][name])
		}
	}
}

func TestAvroNullableArrayRoundTrip(t *testing.T) {
	blk, err := newBlock(
		[]string{"id", "tags"},
		[]string{"int", "array(text) null"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{int32(1), []string{"a", "b"}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(2), nil}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(3), []string{}}); err != nil {
		t.Fatal(err)
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Row 0: tags=["a", "b"]
	if rows[0]["tags"] == nil {
		t.Fatal("row 0 tags: expected non-null")
	}

	// Row 1: tags=NULL
	if rows[1]["tags"] != nil {
		t.Errorf("row 1 tags: expected nil, got %v", rows[1]["tags"])
	}

	// Row 2: tags=[] (empty array, non-null)
	if rows[2]["tags"] == nil {
		t.Fatal("row 2 tags: expected non-null (empty array)")
	}
}

func TestAvroResetAndReuse(t *testing.T) {
	blk, _ := newBlock([]string{"x"}, []string{"int"})
	blk.format = FormatAvro

	if err := blk.appendRow([]interface{}{int32(1)}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{int32(2)}); err != nil {
		t.Fatal(err)
	}

	data1 := avroRoundTrip(t, blk)
	rows1 := readAvroRows(t, data1)
	if len(rows1) != 2 {
		t.Fatalf("first batch: %d rows, want 2", len(rows1))
	}

	blk.reset()
	if err := blk.appendRow([]interface{}{int32(10)}); err != nil {
		t.Fatal(err)
	}

	data2 := avroRoundTrip(t, blk)
	rows2 := readAvroRows(t, data2)
	if len(rows2) != 1 {
		t.Fatalf("second batch: %d rows, want 1", len(rows2))
	}
	if got := rows2[0]["x"].(int); got != 10 {
		t.Errorf("second batch x = %d, want 10", got)
	}
}

func TestAvroArrayIntRoundTrip(t *testing.T) {
	blk, err := newBlock([]string{"nums"}, []string{"array(int)"})
	if err != nil {
		t.Fatal(err)
	}

	if err := blk.appendRow([]interface{}{[]int32{10, 20, 30}}); err != nil {
		t.Fatal(err)
	}
	if err := blk.appendRow([]interface{}{[]int32{}}); err != nil {
		t.Fatal(err)
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	r0 := rows[0]["nums"].([]interface{})
	if len(r0) != 3 {
		t.Errorf("row 0: got %d elements, want 3", len(r0))
	}
	if r0[0].(int) != 10 || r0[1].(int) != 20 || r0[2].(int) != 30 {
		t.Errorf("row 0 nums = %v, want [10 20 30]", r0)
	}

	r1 := rows[1]["nums"].([]interface{})
	if len(r1) != 0 {
		t.Errorf("row 1: got %d elements, want 0", len(r1))
	}
}

// ---------------------------------------------------------------------------
// Per-type scalar round-trip (table-driven, covers every supported type
// including timestamp variants)
// ---------------------------------------------------------------------------

func TestAvroEachScalarType(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	dt := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		fbType string
		val    interface{}
		check  func(t *testing.T, got interface{})
	}{
		{"int", "int", int32(42), func(t *testing.T, got interface{}) {
			if v, ok := got.(int); !ok || v != 42 {
				t.Errorf("got %v (%T), want 42", got, got)
			}
		}},
		{"long", "long", int64(1234567890), func(t *testing.T, got interface{}) {
			if v, ok := got.(int64); !ok || v != 1234567890 {
				t.Errorf("got %v (%T), want 1234567890", got, got)
			}
		}},
		{"float", "float", float32(3.14), func(t *testing.T, got interface{}) {
			if v, ok := got.(float32); !ok || v != 3.14 {
				t.Errorf("got %v (%T), want 3.14", got, got)
			}
		}},
		{"double", "double", float64(2.718281828), func(t *testing.T, got interface{}) {
			if v, ok := got.(float64); !ok || v != 2.718281828 {
				t.Errorf("got %v (%T), want 2.718281828", got, got)
			}
		}},
		{"text", "text", "hello world", func(t *testing.T, got interface{}) {
			if v, ok := got.(string); !ok || v != "hello world" {
				t.Errorf("got %v (%T), want hello world", got, got)
			}
		}},
		{"boolean", "boolean", true, func(t *testing.T, got interface{}) {
			if v, ok := got.(bool); !ok || v != true {
				t.Errorf("got %v (%T), want true", got, got)
			}
		}},
		{"date", "date", dt, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(dt) {
				t.Errorf("got %v (%T), want %v", got, got, dt)
			}
		}},
		{"pgdate", "pgdate", dt, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(dt) {
				t.Errorf("got %v (%T), want %v", got, got, dt)
			}
		}},
		{"timestamp", "timestamp", ts, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(ts) {
				t.Errorf("got %v (%T), want %v", got, got, ts)
			}
		}},
		{"timestamptz", "timestamptz", ts, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(ts) {
				t.Errorf("got %v (%T), want %v", got, got, ts)
			}
		}},
		{"timestampntz", "timestampntz", ts, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(ts) {
				t.Errorf("got %v (%T), want %v", got, got, ts)
			}
		}},
		{"bytea", "bytea", []byte{0xDE, 0xAD, 0xBE, 0xEF}, func(t *testing.T, got interface{}) {
			v, ok := got.([]byte)
			if !ok || len(v) != 4 || v[0] != 0xDE || v[1] != 0xAD || v[2] != 0xBE || v[3] != 0xEF {
				t.Errorf("got %v (%T), want [DE AD BE EF]", got, got)
			}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blk, err := newBlock([]string{"v"}, []string{tc.fbType})
			if err != nil {
				t.Fatalf("newBlock: %v", err)
			}
			if err := blk.appendRow([]interface{}{tc.val}); err != nil {
				t.Fatalf("appendRow: %v", err)
			}
			data := avroRoundTrip(t, blk)
			rows := readAvroRows(t, data)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			tc.check(t, rows[0]["v"])
		})
	}
}

// ---------------------------------------------------------------------------
// Type aliases round-trip through Avro
// ---------------------------------------------------------------------------

func TestAvroTypeAliases(t *testing.T) {
	ts := time.Date(2025, 3, 1, 12, 0, 0, 0, time.UTC)
	dt := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)

	aliases := []struct {
		fbType  string
		val     interface{}
		checkFn func(t *testing.T, got interface{})
	}{
		{"integer", int32(1), func(t *testing.T, got interface{}) {
			if v, ok := got.(int); !ok || v != 1 {
				t.Errorf("got %v (%T), want 1", got, got)
			}
		}},
		{"bigint", int64(2), func(t *testing.T, got interface{}) {
			if v, ok := got.(int64); !ok || v != 2 {
				t.Errorf("got %v (%T), want 2", got, got)
			}
		}},
		{"real", float32(3.0), func(t *testing.T, got interface{}) {
			if v, ok := got.(float32); !ok || v != 3.0 {
				t.Errorf("got %v (%T), want 3.0", got, got)
			}
		}},
		{"double precision", float64(4.0), func(t *testing.T, got interface{}) {
			if v, ok := got.(float64); !ok || v != 4.0 {
				t.Errorf("got %v (%T), want 4.0", got, got)
			}
		}},
		{"pgdate", dt, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(dt) {
				t.Errorf("got %v (%T), want %v", got, got, dt)
			}
		}},
		{"timestampntz", ts, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(ts) {
				t.Errorf("got %v (%T), want %v", got, got, ts)
			}
		}},
		{"timestamptz", ts, func(t *testing.T, got interface{}) {
			if v, ok := got.(time.Time); !ok || !v.Equal(ts) {
				t.Errorf("got %v (%T), want %v", got, got, ts)
			}
		}},
		{"geography", "POINT(0 0)", func(t *testing.T, got interface{}) {
			if v, ok := got.(string); !ok || v != "POINT(0 0)" {
				t.Errorf("got %v (%T), want POINT(0 0)", got, got)
			}
		}},
	}

	for _, a := range aliases {
		t.Run(a.fbType, func(t *testing.T) {
			blk, err := newBlock([]string{"v"}, []string{a.fbType})
			if err != nil {
				t.Fatalf("newBlock(%q): %v", a.fbType, err)
			}
			if err := blk.appendRow([]interface{}{a.val}); err != nil {
				t.Fatalf("appendRow: %v", err)
			}
			data := avroRoundTrip(t, blk)
			rows := readAvroRows(t, data)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			a.checkFn(t, rows[0]["v"])
		})
	}
}

// ---------------------------------------------------------------------------
// Array of every element type through Avro round-trip
// ---------------------------------------------------------------------------

func TestAvroArrayAllElementTypes(t *testing.T) {
	ts := time.Date(2025, 3, 15, 12, 0, 0, 0, time.UTC)
	dt := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		fbType string
		row0   interface{} // non-empty array
		row1   interface{} // empty array
		check  func(t *testing.T, elems []interface{})
	}{
		{
			name:   "array(text)",
			fbType: "array(text)",
			row0:   []string{"hello", "world"},
			row1:   []string{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 2 || elems[0] != "hello" || elems[1] != "world" {
					t.Errorf("got %v, want [hello world]", elems)
				}
			},
		},
		{
			name:   "array(int)",
			fbType: "array(int)",
			row0:   []int32{10, 20, 30},
			row1:   []int32{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 3 || elems[0].(int) != 10 || elems[1].(int) != 20 || elems[2].(int) != 30 {
					t.Errorf("got %v, want [10 20 30]", elems)
				}
			},
		},
		{
			name:   "array(long)",
			fbType: "array(long)",
			row0:   []int64{100, 200},
			row1:   []int64{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 2 || elems[0].(int64) != 100 || elems[1].(int64) != 200 {
					t.Errorf("got %v, want [100 200]", elems)
				}
			},
		},
		{
			name:   "array(float)",
			fbType: "array(float)",
			row0:   []float32{1.5, 2.5},
			row1:   []float32{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 2 || elems[0].(float32) != 1.5 || elems[1].(float32) != 2.5 {
					t.Errorf("got %v, want [1.5 2.5]", elems)
				}
			},
		},
		{
			name:   "array(double)",
			fbType: "array(double)",
			row0:   []float64{3.14, 2.72},
			row1:   []float64{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 2 || elems[0].(float64) != 3.14 || elems[1].(float64) != 2.72 {
					t.Errorf("got %v, want [3.14 2.72]", elems)
				}
			},
		},
		{
			name:   "array(boolean)",
			fbType: "array(boolean)",
			row0:   []bool{true, false, true},
			row1:   []bool{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 3 || elems[0].(bool) != true || elems[1].(bool) != false || elems[2].(bool) != true {
					t.Errorf("got %v, want [true false true]", elems)
				}
			},
		},
		{
			name:   "array(date)",
			fbType: "array(date)",
			row0:   []time.Time{dt},
			row1:   []time.Time{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 1 {
					t.Fatalf("got %d elements, want 1", len(elems))
				}
				if v, ok := elems[0].(time.Time); !ok || !v.Equal(dt) {
					t.Errorf("got %v (%T), want %v", elems[0], elems[0], dt)
				}
			},
		},
		{
			name:   "array(timestamp)",
			fbType: "array(timestamp)",
			row0:   []time.Time{ts},
			row1:   []time.Time{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 1 {
					t.Fatalf("got %d elements, want 1", len(elems))
				}
				if v, ok := elems[0].(time.Time); !ok || !v.Equal(ts) {
					t.Errorf("got %v (%T), want %v", elems[0], elems[0], ts)
				}
			},
		},
		{
			name:   "array(bytea)",
			fbType: "array(bytea)",
			row0:   [][]byte{{0xDE, 0xAD}, {0xBE, 0xEF}},
			row1:   [][]byte{},
			check: func(t *testing.T, elems []interface{}) {
				if len(elems) != 2 {
					t.Fatalf("got %d elements, want 2", len(elems))
				}
				b0, ok0 := elems[0].([]byte)
				b1, ok1 := elems[1].([]byte)
				if !ok0 || !ok1 || len(b0) != 2 || len(b1) != 2 {
					t.Errorf("got %v, want [[DE AD] [BE EF]]", elems)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			blk, err := newBlock([]string{"v"}, []string{tc.fbType})
			if err != nil {
				t.Fatalf("newBlock: %v", err)
			}
			if err := blk.appendRow([]interface{}{tc.row0}); err != nil {
				t.Fatalf("appendRow(non-empty): %v", err)
			}
			if err := blk.appendRow([]interface{}{tc.row1}); err != nil {
				t.Fatalf("appendRow(empty): %v", err)
			}

			data := avroRoundTrip(t, blk)
			rows := readAvroRows(t, data)
			if len(rows) != 2 {
				t.Fatalf("expected 2 rows, got %d", len(rows))
			}

			r0 := rows[0]["v"].([]interface{})
			tc.check(t, r0)

			r1 := rows[1]["v"].([]interface{})
			if len(r1) != 0 {
				t.Errorf("empty array row: got %d elements, want 0", len(r1))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Timestamp variants: verify adjusted vs non-adjusted use different
// Avro logical types and both round-trip correctly
// ---------------------------------------------------------------------------

func TestAvroTimestampVariants(t *testing.T) {
	ts := time.Date(2025, 7, 4, 15, 30, 45, 0, time.UTC)

	variants := []struct {
		name   string
		fbType string
	}{
		{"timestamp (adjusted)", "timestamp"},
		{"timestamptz (adjusted)", "timestamptz"},
		{"timestampntz (non-adjusted)", "timestampntz"},
	}

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			blk, err := newBlock([]string{"ts"}, []string{v.fbType})
			if err != nil {
				t.Fatalf("newBlock: %v", err)
			}
			if err := blk.appendRow([]interface{}{ts}); err != nil {
				t.Fatalf("appendRow: %v", err)
			}

			// Verify the schema uses the correct Avro logical type
			schemaJSON, err := blk.avroSchemaJSON()
			if err != nil {
				t.Fatalf("avroSchemaJSON: %v", err)
			}
			if v.fbType == "timestampntz" {
				if !bytes.Contains([]byte(schemaJSON), []byte("local-timestamp-micros")) {
					t.Errorf("schema should contain local-timestamp-micros for %s, got: %s", v.fbType, schemaJSON)
				}
			} else {
				if !bytes.Contains([]byte(schemaJSON), []byte(`"timestamp-micros"`)) {
					t.Errorf("schema should contain timestamp-micros for %s, got: %s", v.fbType, schemaJSON)
				}
			}

			data := avroRoundTrip(t, blk)
			rows := readAvroRows(t, data)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			got, ok := rows[0]["ts"].(time.Time)
			if !ok {
				t.Fatalf("ts = %v (%T), want time.Time", rows[0]["ts"], rows[0]["ts"])
			}
			if !got.Equal(ts) {
				t.Errorf("ts = %v, want %v", got, ts)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Date edge cases: epoch, pre-epoch, far future
// ---------------------------------------------------------------------------

func TestAvroDateEdgeCases(t *testing.T) {
	dates := []struct {
		name string
		dt   time.Time
	}{
		{"epoch", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"pre-epoch", time.Date(1969, 7, 20, 0, 0, 0, 0, time.UTC)},
		{"recent", time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)},
		{"far-future", time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tc := range dates {
		t.Run(tc.name, func(t *testing.T) {
			blk, err := newBlock([]string{"d"}, []string{"date"})
			if err != nil {
				t.Fatal(err)
			}
			if err := blk.appendRow([]interface{}{tc.dt}); err != nil {
				t.Fatal(err)
			}
			data := avroRoundTrip(t, blk)
			rows := readAvroRows(t, data)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			got, ok := rows[0]["d"].(time.Time)
			if !ok {
				t.Fatalf("d = %v (%T), want time.Time", rows[0]["d"], rows[0]["d"])
			}
			if !got.Equal(tc.dt) {
				t.Errorf("d = %v, want %v", got, tc.dt)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Timestamp edge cases: epoch, sub-second precision
// ---------------------------------------------------------------------------

func TestAvroTimestampEdgeCases(t *testing.T) {
	timestamps := []struct {
		name string
		ts   time.Time
	}{
		{"epoch", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"sub-second", time.Date(2025, 6, 15, 10, 30, 45, 123456000, time.UTC)},
		{"midnight", time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)},
		{"end-of-day", time.Date(2025, 6, 15, 23, 59, 59, 999999000, time.UTC)},
	}

	for _, tc := range timestamps {
		t.Run(tc.name, func(t *testing.T) {
			blk, err := newBlock([]string{"ts"}, []string{"timestamp"})
			if err != nil {
				t.Fatal(err)
			}
			if err := blk.appendRow([]interface{}{tc.ts}); err != nil {
				t.Fatal(err)
			}
			data := avroRoundTrip(t, blk)
			rows := readAvroRows(t, data)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			got, ok := rows[0]["ts"].(time.Time)
			if !ok {
				t.Fatalf("ts = %v (%T), want time.Time", rows[0]["ts"], rows[0]["ts"])
			}
			// Truncate to microsecond precision since that's what we store
			want := tc.ts.Truncate(time.Microsecond)
			if !got.Equal(want) {
				t.Errorf("ts = %v, want %v", got, want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Multiple rows with date/timestamp to verify batch encoding
// ---------------------------------------------------------------------------

func TestAvroDateTimestampMultiRow(t *testing.T) {
	dates := []time.Time{
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 6, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	timestamps := []time.Time{
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 6, 15, 12, 30, 0, 0, time.UTC),
		time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
	}

	blk, err := newBlock([]string{"d", "ts"}, []string{"date", "timestamp"})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := blk.appendRow([]interface{}{dates[i], timestamps[i]}); err != nil {
			t.Fatalf("appendRow %d: %v", i, err)
		}
	}

	data := avroRoundTrip(t, blk)
	rows := readAvroRows(t, data)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i := 0; i < 3; i++ {
		gotD, ok := rows[i]["d"].(time.Time)
		if !ok || !gotD.Equal(dates[i]) {
			t.Errorf("row %d d = %v (%T), want %v", i, rows[i]["d"], rows[i]["d"], dates[i])
		}
		gotTS, ok := rows[i]["ts"].(time.Time)
		if !ok || !gotTS.Equal(timestamps[i]) {
			t.Errorf("row %d ts = %v (%T), want %v", i, rows[i]["ts"], rows[i]["ts"], timestamps[i])
		}
	}
}
