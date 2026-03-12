package fireboltgosdk

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// block holds column data and serialises it to Parquet format.
type block struct {
	columns []column
}

func newBlock(columnNames []string, fireboltTypes []string) (*block, error) {
	if len(columnNames) != len(fireboltTypes) {
		return nil, fmt.Errorf("column names (%d) and types (%d) length mismatch",
			len(columnNames), len(fireboltTypes))
	}

	cols := make([]column, len(columnNames))
	for i, colName := range columnNames {
		col, err := newColumn(colName, fireboltTypes[i])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", colName, err)
		}
		cols[i] = col
	}
	return &block{columns: cols}, nil
}

func (b *block) numColumns() int { return len(b.columns) }

func (b *block) columnAt(index int) column { return b.columns[index] }

func (b *block) blockRows() int {
	if len(b.columns) == 0 {
		return 0
	}
	return b.columns[0].rows()
}

func (b *block) appendRow(values []interface{}) error {
	if len(values) != len(b.columns) {
		return fmt.Errorf("expected %d values, got %d", len(b.columns), len(values))
	}
	for i, col := range b.columns {
		if err := col.appendRow(values[i]); err != nil {
			return fmt.Errorf("column %q (index %d): %w", col.name(), i, err)
		}
	}
	return nil
}

func (b *block) validate() error {
	if len(b.columns) == 0 {
		return nil
	}
	expected := b.columns[0].rows()
	for i := 1; i < len(b.columns); i++ {
		if b.columns[i].rows() != expected {
			return fmt.Errorf("column %q has %d rows, but column %q has %d",
				b.columns[i].name(), b.columns[i].rows(),
				b.columns[0].name(), expected)
		}
	}
	return nil
}

func (b *block) reset() {
	for _, col := range b.columns {
		col.reset()
	}
}

// hasArrayColumn returns true if any column is an array type.
func (b *block) hasArrayColumn() bool {
	for _, col := range b.columns {
		if _, ok := col.(*arrayColumn); ok {
			return true
		}
	}
	return false
}

// leafColumnIndices computes the Parquet leaf column index for each of our
// columns. parquet.Group sorts fields alphabetically, so the leaf indices
// follow that sorted order rather than our insertion order.
func (b *block) leafColumnIndices() []int {
	names := make([]string, len(b.columns))
	for i, col := range b.columns {
		names[i] = col.name()
	}
	sorted := make([]string, len(names))
	copy(sorted, names)
	sort.Strings(sorted)

	nameToIdx := make(map[string]int, len(sorted))
	for i, n := range sorted {
		nameToIdx[n] = i
	}

	indices := make([]int, len(b.columns))
	for i, col := range b.columns {
		indices[i] = nameToIdx[col.name()]
	}
	return indices
}

// toParquet serialises all buffered data into Parquet format.
func (b *block) toParquet() ([]byte, error) {
	numRows := b.blockRows()
	if numRows == 0 {
		return nil, nil
	}

	group := make(parquet.Group, len(b.columns))
	for _, col := range b.columns {
		group[col.name()] = col.parquetNode()
	}
	schema := parquet.NewSchema("firebolt", group)

	leafIndices := b.leafColumnIndices()

	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, schema)

	hasArrays := b.hasArrayColumn()

	const batchSize = 4096
	rowBatch := make([]parquet.Row, 0, batchSize)

	// Build a permutation so we emit values in ascending leaf-index order,
	// which is required by parquet.Writer.WriteRows.
	order := make([]int, len(b.columns))
	for j := range order {
		order[j] = j
	}
	sort.Slice(order, func(a, bIdx int) bool {
		return leafIndices[order[a]] < leafIndices[order[bIdx]]
	})

	for i := 0; i < numRows; i++ {
		row := make(parquet.Row, 0, len(b.columns))
		for _, j := range order {
			col := b.columns[j]
			leafIdx := leafIndices[j]
			if ac, ok := col.(*arrayColumn); ok && hasArrays {
				row = ac.appendParquetArrayValues(row, i, leafIdx)
			} else {
				row = append(row, col.parquetValue(i, leafIdx))
			}
		}
		rowBatch = append(rowBatch, row)
		if len(rowBatch) >= batchSize {
			if _, err := w.WriteRows(rowBatch); err != nil {
				return nil, fmt.Errorf("error writing parquet rows: %w", err)
			}
			rowBatch = rowBatch[:0]
		}
	}

	if len(rowBatch) > 0 {
		if _, err := w.WriteRows(rowBatch); err != nil {
			return nil, fmt.Errorf("error writing parquet rows: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("error closing parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}
