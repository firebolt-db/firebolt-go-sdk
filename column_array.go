package fireboltgosdk

import (
	"fmt"
	"reflect"
	"time"

	"github.com/parquet-go/parquet-go"
)

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
