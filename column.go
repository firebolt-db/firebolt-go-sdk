package fireboltgosdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

var epoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// column represents a single typed column buffer used during batch insertion.
// Data is buffered in Go slices and converted to Parquet at send time.
type column interface {
	name() string
	rows() int
	appendRow(v interface{}) error
	appendColumn(v interface{}) error
	appendZero()
	reset()
	parquetNode() parquet.Node
	parquetValues(colIdx int) []parquet.Value
}

// appendColumnFallback iterates over any slice/array via reflection and
// delegates each element to appendRow.
func appendColumnFallback(col column, v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return fmt.Errorf("AppendColumn requires a slice or array, got %T", v)
	}
	for i := 0; i < rv.Len(); i++ {
		if err := col.appendRow(rv.Index(i).Interface()); err != nil {
			return fmt.Errorf("element [%d]: %w", i, err)
		}
	}
	return nil
}

// newColumn creates a column buffer from a Firebolt type string.
func newColumn(colName, fireboltType string) (column, error) {
	return newColumnFromType(colName, fireboltType)
}

func newColumnFromType(colName, fireboltType string) (column, error) {
	if strings.HasSuffix(fireboltType, " null") {
		inner, err := newColumnFromType(colName, fireboltType[:len(fireboltType)-len(" null")])
		if err != nil {
			return nil, err
		}
		return &nullableColumn{colName: colName, inner: inner}, nil
	}

	if strings.HasPrefix(fireboltType, "array(") && strings.HasSuffix(fireboltType, ")") {
		elemType := fireboltType[len("array(") : len(fireboltType)-1]
		inner, err := newColumnFromType("", elemType)
		if err != nil {
			return nil, err
		}
		// A null element is not representable. parquet.Repeated overrides the
		// element's repetition type, so Repeated(Optional(x)) collapses to a
		// single definition level: 0 already means "empty array" and 1 means
		// "element present". Nothing is left to encode "element present and
		// null" with, and the writer emits a null at level 1 as the type's
		// zero value -- for json that is "", which the engine rejects on
		// ingest with "Failed to parse JSON: Empty input".
		//
		// Only json is refused here. The same flattening loses null elements
		// for every other type too, but there the zero value at least ingests,
		// and changing that is a wire-format change beyond this type's scope.
		if nc, ok := inner.(*nullableColumn); ok {
			if _, isJSON := nc.inner.(*jsonColumn); isJSON {
				return nil, fmt.Errorf("unsupported column type for batch insert: %s; "+
					"a null json array element cannot be encoded, use array(json) and "+
					"pass {} for absent documents", fireboltType)
			}
		}
		return &arrayColumn{colName: colName, elem: inner}, nil
	}

	switch fireboltType {
	case "int", "integer":
		return &int32Column{colName: colName}, nil
	case "long", "bigint":
		return &int64Column{colName: colName}, nil
	case "float", "real":
		return &float32Column{colName: colName}, nil
	case "double", "double precision":
		return &float64Column{colName: colName}, nil
	case "text", "geography":
		return &stringColumn{colName: colName}, nil
	case "json":
		return &jsonColumn{colName: colName}, nil
	case "boolean":
		return &boolColumn{colName: colName}, nil
	case "date", "pgdate":
		return &dateColumn{colName: colName}, nil
	case "timestamp":
		return &timestampColumn{colName: colName, adjusted: true}, nil
	case "timestampntz":
		return &timestampColumn{colName: colName, adjusted: false}, nil
	case "timestamptz":
		return &timestampColumn{colName: colName, adjusted: true}, nil
	case "bytea":
		return &byteaColumn{colName: colName}, nil
	default:
		return nil, fmt.Errorf("unsupported column type for batch insert: %s", fireboltType)
	}
}

// ---------------------------------------------------------------------------
// Numeric conversion helpers
// ---------------------------------------------------------------------------

func toInt32(v interface{}) (int32, error) {
	switch val := v.(type) {
	case int32:
		return val, nil
	case int:
		return int32(val), nil
	case int64:
		return int32(val), nil
	case int16:
		return int32(val), nil
	case int8:
		return int32(val), nil
	case uint8:
		return int32(val), nil
	case uint16:
		return int32(val), nil
	case float64:
		return int32(val), nil
	case float32:
		return int32(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int32", v)
	}
}

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func toFloat32(v interface{}) (float32, error) {
	switch val := v.(type) {
	case float32:
		return val, nil
	case float64:
		return float32(val), nil
	case int:
		return float32(val), nil
	case int32:
		return float32(val), nil
	case int64:
		return float32(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float32", v)
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case int64:
		return float64(val), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toTime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time.Time", v)
	}
}

// ---------------------------------------------------------------------------
// int32Column
// ---------------------------------------------------------------------------

type int32Column struct {
	colName string
	data    []int32
}

func (c *int32Column) name() string { return c.colName }
func (c *int32Column) rows() int    { return len(c.data) }

func (c *int32Column) appendRow(v interface{}) error {
	val, err := toInt32(v)
	if err != nil {
		return err
	}
	c.data = append(c.data, val)
	return nil
}

func (c *int32Column) appendColumn(v interface{}) error {
	if vals, ok := v.([]int32); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *int32Column) appendZero()               { c.data = append(c.data, 0) }
func (c *int32Column) reset()                    { c.data = c.data[:0] }
func (c *int32Column) parquetNode() parquet.Node { return parquet.Leaf(parquet.Int32Type) }
func (c *int32Column) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.Int32Value(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// int64Column
// ---------------------------------------------------------------------------

type int64Column struct {
	colName string
	data    []int64
}

func (c *int64Column) name() string { return c.colName }
func (c *int64Column) rows() int    { return len(c.data) }

func (c *int64Column) appendRow(v interface{}) error {
	val, err := toInt64(v)
	if err != nil {
		return err
	}
	c.data = append(c.data, val)
	return nil
}

func (c *int64Column) appendColumn(v interface{}) error {
	if vals, ok := v.([]int64); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *int64Column) appendZero()               { c.data = append(c.data, 0) }
func (c *int64Column) reset()                    { c.data = c.data[:0] }
func (c *int64Column) parquetNode() parquet.Node { return parquet.Leaf(parquet.Int64Type) }
func (c *int64Column) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.Int64Value(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// float32Column
// ---------------------------------------------------------------------------

type float32Column struct {
	colName string
	data    []float32
}

func (c *float32Column) name() string { return c.colName }
func (c *float32Column) rows() int    { return len(c.data) }

func (c *float32Column) appendRow(v interface{}) error {
	val, err := toFloat32(v)
	if err != nil {
		return err
	}
	c.data = append(c.data, val)
	return nil
}

func (c *float32Column) appendColumn(v interface{}) error {
	if vals, ok := v.([]float32); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *float32Column) appendZero()               { c.data = append(c.data, 0) }
func (c *float32Column) reset()                    { c.data = c.data[:0] }
func (c *float32Column) parquetNode() parquet.Node { return parquet.Leaf(parquet.FloatType) }
func (c *float32Column) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.FloatValue(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// float64Column
// ---------------------------------------------------------------------------

type float64Column struct {
	colName string
	data    []float64
}

func (c *float64Column) name() string { return c.colName }
func (c *float64Column) rows() int    { return len(c.data) }

func (c *float64Column) appendRow(v interface{}) error {
	val, err := toFloat64(v)
	if err != nil {
		return err
	}
	c.data = append(c.data, val)
	return nil
}

func (c *float64Column) appendColumn(v interface{}) error {
	if vals, ok := v.([]float64); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *float64Column) appendZero()               { c.data = append(c.data, 0) }
func (c *float64Column) reset()                    { c.data = c.data[:0] }
func (c *float64Column) parquetNode() parquet.Node { return parquet.Leaf(parquet.DoubleType) }
func (c *float64Column) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.DoubleValue(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// stringColumn
// ---------------------------------------------------------------------------

type stringColumn struct {
	colName string
	data    []string
}

func (c *stringColumn) name() string { return c.colName }
func (c *stringColumn) rows() int    { return len(c.data) }

func (c *stringColumn) appendRow(v interface{}) error {
	switch val := v.(type) {
	case string:
		c.data = append(c.data, val)
		return nil
	default:
		return fmt.Errorf("cannot convert %T to string", v)
	}
}

func (c *stringColumn) appendColumn(v interface{}) error {
	if vals, ok := v.([]string); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *stringColumn) appendZero()               { c.data = append(c.data, "") }
func (c *stringColumn) reset()                    { c.data = c.data[:0] }
func (c *stringColumn) parquetNode() parquet.Node { return parquet.String() }
func (c *stringColumn) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, s := range c.data {
		// unsafe.Slice avoids allocating+copying a []byte per string; the
		// column buffer copies the data during WriteValues so the reference
		// only needs to survive that call.
		var b []byte
		if len(s) > 0 {
			b = unsafe.Slice(unsafe.StringData(s), len(s))
		}
		vals[i] = parquet.ByteArrayValue(b).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// jsonColumn
// ---------------------------------------------------------------------------

// jsonColumn buffers values destined for a Firebolt JSON column.
//
// Values are accepted as JSON text (string or []byte) and written as a Parquet
// leaf carrying the JSON logical type, which is how Parquet represents
// semi-structured data in a byte array. The engine reads that back into its
// native JSON type.
//
// The contents are not parsed or validated here. Validating would mean paying a
// full JSON parse per value on the hot path of a bulk load, and the engine
// rejects malformed documents on ingest anyway, so the check would be duplicated
// rather than added.
type jsonColumn struct {
	colName string
	data    []string
}

// errEmptyJSON is shared by both append paths so they reject identically.
var errEmptyJSON = errors.New("cannot store an empty value in a json column: " +
	"pass a JSON document such as {}, or untyped nil for a nullable column")

func (c *jsonColumn) name() string { return c.colName }
func (c *jsonColumn) rows() int    { return len(c.data) }

func (c *jsonColumn) appendRow(v interface{}) error {
	var doc string
	switch val := v.(type) {
	case string:
		doc = val
	case []byte:
		// Common when the caller already has marshalled JSON in hand.
		doc = string(val)
	case json.RawMessage:
		doc = string(val)
	default:
		return fmt.Errorf("cannot convert %T to json; pass JSON text as string, []byte, or json.RawMessage", v)
	}

	// An empty value is not a JSON document: the engine rejects it with
	// "Failed to parse JSON: Empty input". A nil []byte or json.RawMessage is
	// the zero value of both accepted types, so it arrives here easily, and
	// storing "" would defer the failure to ingest where the cause is no longer
	// visible. appendZero writes "{}" for a gap it invents; a value the caller
	// supplied is different, and guessing at it would be inventing data.
	if doc == "" {
		return errEmptyJSON
	}

	c.data = append(c.data, doc)
	return nil
}

func (c *jsonColumn) appendColumn(v interface{}) error {
	if vals, ok := v.([]string); ok {
		// The same empty check appendRow makes, hoisted out of the bulk copy
		// so the fast path stays a single append. Skipping it here would let
		// the columnar API buffer "" and fail on ingest instead.
		for i, s := range vals {
			if s == "" {
				return fmt.Errorf("element [%d]: %w", i, errEmptyJSON)
			}
		}
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

// appendZero writes an empty JSON object rather than an empty string, because
// "" is not a valid JSON document and would fail on ingest.
func (c *jsonColumn) appendZero()               { c.data = append(c.data, "{}") }
func (c *jsonColumn) reset()                    { c.data = c.data[:0] }
func (c *jsonColumn) parquetNode() parquet.Node { return parquet.JSON() }
func (c *jsonColumn) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, s := range c.data {
		// See stringColumn.parquetValues for why unsafe.Slice is safe here.
		var b []byte
		if len(s) > 0 {
			b = unsafe.Slice(unsafe.StringData(s), len(s))
		}
		vals[i] = parquet.ByteArrayValue(b).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// boolColumn
// ---------------------------------------------------------------------------

type boolColumn struct {
	colName string
	data    []bool
}

func (c *boolColumn) name() string { return c.colName }
func (c *boolColumn) rows() int    { return len(c.data) }

func (c *boolColumn) appendRow(v interface{}) error {
	switch val := v.(type) {
	case bool:
		c.data = append(c.data, val)
		return nil
	default:
		return fmt.Errorf("cannot convert %T to bool", v)
	}
}

func (c *boolColumn) appendColumn(v interface{}) error {
	if vals, ok := v.([]bool); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *boolColumn) appendZero()               { c.data = append(c.data, false) }
func (c *boolColumn) reset()                    { c.data = c.data[:0] }
func (c *boolColumn) parquetNode() parquet.Node { return parquet.Leaf(parquet.BooleanType) }
func (c *boolColumn) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.BooleanValue(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// dateColumn — Parquet DATE (INT32, days since 1970-01-01)
// ---------------------------------------------------------------------------

type dateColumn struct {
	colName string
	data    []int32
}

func (c *dateColumn) name() string { return c.colName }
func (c *dateColumn) rows() int    { return len(c.data) }

func (c *dateColumn) appendRow(v interface{}) error {
	t, err := toTime(v)
	if err != nil {
		return err
	}
	days := int32(t.Sub(epoch) / (24 * time.Hour))
	c.data = append(c.data, days)
	return nil
}

func (c *dateColumn) appendColumn(v interface{}) error {
	if vals, ok := v.([]time.Time); ok {
		for _, t := range vals {
			c.data = append(c.data, int32(t.Sub(epoch)/(24*time.Hour)))
		}
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *dateColumn) appendZero()               { c.data = append(c.data, 0) }
func (c *dateColumn) reset()                    { c.data = c.data[:0] }
func (c *dateColumn) parquetNode() parquet.Node { return parquet.Date() }
func (c *dateColumn) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.Int32Value(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// timestampColumn — Parquet TIMESTAMP (INT64, microseconds since epoch)
// ---------------------------------------------------------------------------

type timestampColumn struct {
	colName  string
	adjusted bool // IsAdjustedToUTC
	data     []int64
}

func (c *timestampColumn) name() string { return c.colName }
func (c *timestampColumn) rows() int    { return len(c.data) }

func (c *timestampColumn) appendRow(v interface{}) error {
	t, err := toTime(v)
	if err != nil {
		return err
	}
	c.data = append(c.data, t.UnixMicro())
	return nil
}

func (c *timestampColumn) appendColumn(v interface{}) error {
	if vals, ok := v.([]time.Time); ok {
		for _, t := range vals {
			c.data = append(c.data, t.UnixMicro())
		}
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *timestampColumn) appendZero() { c.data = append(c.data, 0) }
func (c *timestampColumn) reset()      { c.data = c.data[:0] }

func (c *timestampColumn) parquetNode() parquet.Node {
	return parquet.TimestampAdjusted(parquet.Microsecond, c.adjusted)
}

func (c *timestampColumn) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, v := range c.data {
		vals[i] = parquet.Int64Value(v).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// byteaColumn
// ---------------------------------------------------------------------------

type byteaColumn struct {
	colName string
	data    [][]byte
}

func (c *byteaColumn) name() string { return c.colName }
func (c *byteaColumn) rows() int    { return len(c.data) }

func (c *byteaColumn) appendRow(v interface{}) error {
	switch val := v.(type) {
	case []byte:
		c.data = append(c.data, val)
		return nil
	case string:
		c.data = append(c.data, []byte(val))
		return nil
	default:
		return fmt.Errorf("cannot convert %T to []byte", v)
	}
}

func (c *byteaColumn) appendColumn(v interface{}) error {
	if vals, ok := v.([][]byte); ok {
		c.data = append(c.data, vals...)
		return nil
	}
	return appendColumnFallback(c, v)
}

func (c *byteaColumn) appendZero()               { c.data = append(c.data, nil) }
func (c *byteaColumn) reset()                    { c.data = c.data[:0] }
func (c *byteaColumn) parquetNode() parquet.Node { return parquet.Leaf(parquet.ByteArrayType) }
func (c *byteaColumn) parquetValues(colIdx int) []parquet.Value {
	vals := make([]parquet.Value, len(c.data))
	for i, b := range c.data {
		vals[i] = parquet.ByteArrayValue(b).Level(0, 0, colIdx)
	}
	return vals
}

// ---------------------------------------------------------------------------
// nullableColumn — wraps any column to add NULL support
// ---------------------------------------------------------------------------

type nullableColumn struct {
	colName string
	nulls   []bool
	inner   column
}

func (c *nullableColumn) name() string { return c.colName }
func (c *nullableColumn) rows() int    { return len(c.nulls) }

func (c *nullableColumn) appendRow(v interface{}) error {
	if v == nil {
		c.nulls = append(c.nulls, true)
		c.inner.appendZero()
		return nil
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && rv.IsNil() {
		c.nulls = append(c.nulls, true)
		c.inner.appendZero()
		return nil
	}
	c.nulls = append(c.nulls, false)
	return c.inner.appendRow(v)
}

func (c *nullableColumn) appendColumn(v interface{}) error {
	return appendColumnFallback(c, v)
}

func (c *nullableColumn) appendZero() {
	c.nulls = append(c.nulls, true)
	c.inner.appendZero()
}

func (c *nullableColumn) reset() {
	c.nulls = c.nulls[:0]
	c.inner.reset()
}

func (c *nullableColumn) parquetNode() parquet.Node {
	if _, ok := c.inner.(*arrayColumn); ok {
		return c.inner.parquetNode()
	}
	return parquet.Optional(c.inner.parquetNode())
}

func (c *nullableColumn) parquetValues(colIdx int) []parquet.Value {
	if ac, ok := c.inner.(*arrayColumn); ok {
		return c.nullableArrayValues(ac, colIdx)
	}
	innerVals := c.inner.parquetValues(colIdx)
	vals := make([]parquet.Value, len(c.nulls))
	for i, isNull := range c.nulls {
		if isNull {
			vals[i] = parquet.Value{}.Level(0, 0, colIdx)
		} else {
			vals[i] = innerVals[i].Level(0, 1, colIdx)
		}
	}
	return vals
}

// nullableArrayValues handles the OPTIONAL + REPEATED case. In Parquet's
// flat encoding, REPEATED already carries presence semantics via definition
// levels, so Optional(Repeated(X)) shares the same max def level. Null
// rows are stored as empty arrays in the inner column and encoded
// identically to empty arrays (def=0 sentinel).
func (c *nullableColumn) nullableArrayValues(ac *arrayColumn, colIdx int) []parquet.Value {
	return ac.parquetValues(colIdx)
}
