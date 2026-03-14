package fireboltgosdk

import (
	"fmt"
	"reflect"
	"strings"
	"time"

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
	parquetValue(rowIdx, colIdx int) parquet.Value
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
func (c *int32Column) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.Int32Value(c.data[rowIdx]).Level(0, 0, colIdx)
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
func (c *int64Column) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.Int64Value(c.data[rowIdx]).Level(0, 0, colIdx)
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
func (c *float32Column) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.FloatValue(c.data[rowIdx]).Level(0, 0, colIdx)
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
func (c *float64Column) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.DoubleValue(c.data[rowIdx]).Level(0, 0, colIdx)
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
func (c *stringColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.ByteArrayValue([]byte(c.data[rowIdx])).Level(0, 0, colIdx)
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
func (c *boolColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.BooleanValue(c.data[rowIdx]).Level(0, 0, colIdx)
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
func (c *dateColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.Int32Value(c.data[rowIdx]).Level(0, 0, colIdx)
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

func (c *timestampColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.Int64Value(c.data[rowIdx]).Level(0, 0, colIdx)
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
func (c *byteaColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	return parquet.ByteArrayValue(c.data[rowIdx]).Level(0, 0, colIdx)
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
	return parquet.Optional(c.inner.parquetNode())
}

func (c *nullableColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	if c.nulls[rowIdx] {
		return parquet.Value{}.Level(0, 0, colIdx)
	}
	v := c.inner.parquetValue(rowIdx, colIdx)
	return v.Level(0, 1, colIdx)
}

// ---------------------------------------------------------------------------
// arrayColumn — wraps an element column for array types
// Rows are stored flattened with an offset array, but for Parquet we
// fall back to row-level reflection because LIST encoding requires
// per-element repetition levels that are simplest to handle at write time.
// ---------------------------------------------------------------------------

type arrayColumn struct {
	colName string
	offsets []uint64
	elem    column
}

func (c *arrayColumn) name() string { return c.colName }
func (c *arrayColumn) rows() int    { return len(c.offsets) }

func (c *arrayColumn) appendRow(v interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return fmt.Errorf("expected slice or array for Array column, got %T", v)
	}
	for i := 0; i < rv.Len(); i++ {
		if err := c.elem.appendRow(rv.Index(i).Interface()); err != nil {
			return err
		}
	}
	var prev uint64
	if len(c.offsets) > 0 {
		prev = c.offsets[len(c.offsets)-1]
	}
	c.offsets = append(c.offsets, prev+uint64(rv.Len()))
	return nil
}

func (c *arrayColumn) appendColumn(v interface{}) error {
	return appendColumnFallback(c, v)
}

func (c *arrayColumn) appendZero() {
	var prev uint64
	if len(c.offsets) > 0 {
		prev = c.offsets[len(c.offsets)-1]
	}
	c.offsets = append(c.offsets, prev)
}

func (c *arrayColumn) reset() {
	c.offsets = c.offsets[:0]
	c.elem.reset()
}

func (c *arrayColumn) parquetNode() parquet.Node {
	return parquet.Repeated(c.elem.parquetNode())
}

// parquetValue is not used for array columns; see block.toParquet which
// handles arrays via appendParquetArrayValues.
func (c *arrayColumn) parquetValue(rowIdx, colIdx int) parquet.Value {
	panic("parquetValue should not be called on arrayColumn; use appendParquetArrayValues")
}

// appendParquetArrayValues appends Parquet values for one row of this array
// to the given row slice.
func (c *arrayColumn) appendParquetArrayValues(row parquet.Row, rowIdx, colIdx int) parquet.Row {
	var start uint64
	if rowIdx > 0 {
		start = c.offsets[rowIdx-1]
	}
	end := c.offsets[rowIdx]

	if start == end {
		// Empty array: emit a null sentinel at rep=0, def=0 so the Parquet
		// writer records the row and doesn't silently drop it.
		row = append(row, parquet.Value{}.Level(0, 0, colIdx))
		return row
	}

	for i := start; i < end; i++ {
		v := c.elem.parquetValue(int(i), colIdx)
		rep := 1
		if i == start {
			rep = 0
		}
		row = append(row, v.Level(rep, 1, colIdx))
	}
	return row
}
