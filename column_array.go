package fireboltgosdk

import (
	"fmt"
	"reflect"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"
)

// ---------------------------------------------------------------------------
// arrayColumn — wraps an element column for array types.
// Rows are stored flattened in the element column with an offset array
// tracking boundaries. parquetValues builds all Parquet values for the
// column in one pass, setting repetition/definition levels for the
// REPEATED encoding.
// ---------------------------------------------------------------------------

type arrayColumn struct {
	colName string
	offsets []uint64
	elem    column
}

func (c *arrayColumn) name() string { return c.colName }
func (c *arrayColumn) rows() int    { return len(c.offsets) }

func (c *arrayColumn) appendRow(v interface{}) error {
	var n int
	switch vals := v.(type) {
	case []string:
		n = len(vals)
		if sc, ok := c.elem.(*stringColumn); ok {
			sc.data = append(sc.data, vals...)
		} else {
			for _, s := range vals {
				if err := c.elem.appendRow(s); err != nil {
					return err
				}
			}
		}
	case []int32:
		n = len(vals)
		if ic, ok := c.elem.(*int32Column); ok {
			ic.data = append(ic.data, vals...)
		} else {
			for _, val := range vals {
				if err := c.elem.appendRow(val); err != nil {
					return err
				}
			}
		}
	case []int64:
		n = len(vals)
		if ic, ok := c.elem.(*int64Column); ok {
			ic.data = append(ic.data, vals...)
		} else {
			for _, val := range vals {
				if err := c.elem.appendRow(val); err != nil {
					return err
				}
			}
		}
	case []float32:
		n = len(vals)
		if fc, ok := c.elem.(*float32Column); ok {
			fc.data = append(fc.data, vals...)
		} else {
			for _, val := range vals {
				if err := c.elem.appendRow(val); err != nil {
					return err
				}
			}
		}
	case []float64:
		n = len(vals)
		if fc, ok := c.elem.(*float64Column); ok {
			fc.data = append(fc.data, vals...)
		} else {
			for _, val := range vals {
				if err := c.elem.appendRow(val); err != nil {
					return err
				}
			}
		}
	case []bool:
		n = len(vals)
		if bc, ok := c.elem.(*boolColumn); ok {
			bc.data = append(bc.data, vals...)
		} else {
			for _, val := range vals {
				if err := c.elem.appendRow(val); err != nil {
					return err
				}
			}
		}
	case []time.Time:
		n = len(vals)
		for _, val := range vals {
			if err := c.elem.appendRow(val); err != nil {
				return err
			}
		}
	case [][]byte:
		n = len(vals)
		if bc, ok := c.elem.(*byteaColumn); ok {
			bc.data = append(bc.data, vals...)
		} else {
			for _, val := range vals {
				if err := c.elem.appendRow(val); err != nil {
					return err
				}
			}
		}
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			return fmt.Errorf("expected slice or array for Array column, got %T", v)
		}
		n = rv.Len()
		for i := 0; i < n; i++ {
			if err := c.elem.appendRow(rv.Index(i).Interface()); err != nil {
				return err
			}
		}
	}
	var prev uint64
	if len(c.offsets) > 0 {
		prev = c.offsets[len(c.offsets)-1]
	}
	c.offsets = append(c.offsets, prev+uint64(n))
	return nil
}

func (c *arrayColumn) appendColumn(v interface{}) error {
	switch vals := v.(type) {
	case [][]string:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][]int32:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][]int64:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][]float32:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][]float64:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][]bool:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][]time.Time:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	case [][][]byte:
		for _, row := range vals {
			if err := c.appendRow(row); err != nil {
				return err
			}
		}
		return nil
	default:
		return appendColumnFallback(c, v)
	}
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

func (c *arrayColumn) parquetValues(colIdx int) []parquet.Value {
	numRows := len(c.offsets)
	if numRows == 0 {
		return nil
	}

	totalElems := c.offsets[numRows-1]

	var emptyCount uint64
	var prev uint64
	for _, off := range c.offsets {
		if off == prev {
			emptyCount++
		}
		prev = off
	}
	vals := make([]parquet.Value, 0, int(totalElems+emptyCount))

	// For element types handled by appendElemValues (concrete scalars and
	// nullableColumn), we use the fused path that builds values with final
	// levels directly. For anything else, pre-build all element values once
	// upfront so the per-row loop only indexes into the result.
	var elemVals []parquet.Value
	if !c.canFuseElemValues() {
		elemVals = c.elem.parquetValues(colIdx)
	}

	prev = 0
	for _, end := range c.offsets {
		if prev == end {
			vals = append(vals, parquet.Value{}.Level(0, 0, colIdx))
		} else if elemVals == nil {
			vals = c.appendElemValues(vals, prev, end, colIdx)
		} else {
			for i := prev; i < end; i++ {
				rep := 1
				if i == prev {
					rep = 0
				}
				vals = append(vals, elemVals[i].Level(rep, 1, colIdx))
			}
		}
		prev = end
	}
	return vals
}

// canFuseElemValues reports whether appendElemValues can handle c.elem
// directly (without pre-building an intermediate []Value).
func (c *arrayColumn) canFuseElemValues() bool {
	switch elem := c.elem.(type) {
	case *stringColumn, *int32Column, *int64Column, *float32Column,
		*float64Column, *boolColumn, *dateColumn, *timestampColumn,
		*byteaColumn:
		return true
	case *nullableColumn:
		switch elem.inner.(type) {
		case *stringColumn, *int32Column, *int64Column, *float32Column,
			*float64Column, *boolColumn, *dateColumn, *timestampColumn,
			*byteaColumn:
			return true
		}
	}
	return false
}

// appendElemValues appends parquet values for elements [start, end) with
// correct repetition/definition levels. It type-switches on the element
// column to construct values with final levels in a single step, avoiding
// the intermediate []Value from elem.parquetValues and a second
// Value.Level call per element.
func (c *arrayColumn) appendElemValues(vals []parquet.Value, start, end uint64, colIdx int) []parquet.Value {
	switch elem := c.elem.(type) {
	case *stringColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			s := elem.data[i]
			var b []byte
			if len(s) > 0 {
				b = unsafe.Slice(unsafe.StringData(s), len(s))
			}
			vals = append(vals, parquet.ByteArrayValue(b).Level(rep, 1, colIdx))
		}
	case *int32Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.Int32Value(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *int64Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.Int64Value(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *float32Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.FloatValue(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *float64Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.DoubleValue(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *boolColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.BooleanValue(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *dateColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.Int32Value(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *timestampColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.Int64Value(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *byteaColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			vals = append(vals, parquet.ByteArrayValue(elem.data[i]).Level(rep, 1, colIdx))
		}
	case *nullableColumn:
		vals = c.appendNullableElemValues(vals, elem, start, end, colIdx)
	default:
		panic("appendElemValues called for unhandled type; canFuseElemValues should have returned false")
	}
	return vals
}

// appendNullableElemValues handles array elements wrapped in nullableColumn.
// It type-switches on the inner column to produce values with final levels
// directly, matching the behavior of nullableColumn.parquetValues followed
// by a Level(rep, 1, colIdx) override.
func (c *arrayColumn) appendNullableElemValues(vals []parquet.Value, nc *nullableColumn, start, end uint64, colIdx int) []parquet.Value {
	switch inner := nc.inner.(type) {
	case *stringColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				s := inner.data[i]
				var b []byte
				if len(s) > 0 {
					b = unsafe.Slice(unsafe.StringData(s), len(s))
				}
				vals = append(vals, parquet.ByteArrayValue(b).Level(rep, 1, colIdx))
			}
		}
	case *int32Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.Int32Value(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *int64Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.Int64Value(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *float32Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.FloatValue(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *float64Column:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.DoubleValue(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *boolColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.BooleanValue(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *dateColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.Int32Value(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *timestampColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.Int64Value(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	case *byteaColumn:
		for i := start; i < end; i++ {
			rep := 1
			if i == start {
				rep = 0
			}
			if nc.nulls[i] {
				vals = append(vals, parquet.Value{}.Level(rep, 1, colIdx))
			} else {
				vals = append(vals, parquet.ByteArrayValue(inner.data[i]).Level(rep, 1, colIdx))
			}
		}
	default:
		panic("appendNullableElemValues called for unhandled inner type; canFuseElemValues should have returned false")
	}
	return vals
}
