package fireboltgosdk

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

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

	seen := make(map[string]struct{}, len(columnNames))
	for _, name := range columnNames {
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate column name %q", name)
		}
		seen[name] = struct{}{}
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

// leafColumnIndices computes the Parquet leaf column index for each of our
// columns. parquet.Group sorts fields alphabetically, so the leaf indices
// follow that sorted order rather than our insertion order.
func (b *block) leafColumnIndices() []int {
	type nameIdx struct {
		name string
		orig int
	}
	items := make([]nameIdx, len(b.columns))
	for i, col := range b.columns {
		items[i] = nameIdx{col.name(), i}
	}
	// Sort by name to match parquet.Group's alphabetical ordering.
	slices.SortFunc(items, func(a, c nameIdx) int {
		return cmp.Compare(a.name, c.name)
	})
	indices := make([]int, len(b.columns))
	for leafIdx, item := range items {
		indices[item.orig] = leafIdx
	}
	return indices
}

// toParquet serialises all buffered data into Parquet format.
//
// Values are built column-by-column in bulk (one allocation per column via
// parquetValues), then assembled into rows row-by-row and written through
// Writer.WriteRows in batches.
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

	// Build all values column-by-column (bulk, one alloc per column).
	type colVals struct {
		leafIdx  int
		values   []parquet.Value
		isArray  bool
		offsets  []uint64
		arrayPos int // read cursor for variable-length array values
	}
	cvs := make([]colVals, len(b.columns))
	for i, col := range b.columns {
		cv := colVals{
			leafIdx: leafIndices[i],
			values:  col.parquetValues(leafIndices[i]),
		}
		if ac, ok := col.(*arrayColumn); ok {
			cv.isArray = true
			cv.offsets = ac.offsets
		} else if nc, ok := col.(*nullableColumn); ok {
			if ac, ok := nc.inner.(*arrayColumn); ok {
				cv.isArray = true
				cv.offsets = ac.offsets
			}
		}
		cvs[i] = cv
	}
	// Sort by leaf index so row values are in ascending column order
	// as required by Writer.WriteRows.
	slices.SortFunc(cvs, func(a, c colVals) int {
		return cmp.Compare(a.leafIdx, c.leafIdx)
	})

	// Compute exact values-per-row so we can back all rows by one flat slab.
	numScalar := 0
	for _, cv := range cvs {
		if !cv.isArray {
			numScalar++
		}
	}
	totalValues := numScalar * numRows
	for _, cv := range cvs {
		if cv.isArray {
			totalValues += len(cv.values)
		}
	}

	flat := make([]parquet.Value, 0, totalValues)
	rows := make([]parquet.Row, numRows)

	// Assemble rows row-by-row (good write locality) from pre-built column
	// values (no per-value function call or string→byte copy).
	var out bytes.Buffer
	w := parquet.NewWriter(&out, schema)

	const batchSize = 4096

	for r := range numRows {
		rowStart := len(flat)
		for ci := range cvs {
			cv := &cvs[ci]
			if !cv.isArray {
				flat = append(flat, cv.values[r])
			} else {
				var start uint64
				if r > 0 {
					start = cv.offsets[r-1]
				}
				end := cv.offsets[r]
				pos := cv.arrayPos
				if start == end {
					flat = append(flat, cv.values[pos])
					cv.arrayPos = pos + 1
				} else {
					n := int(end - start)
					flat = append(flat, cv.values[pos:pos+n]...)
					cv.arrayPos = pos + n
				}
			}
		}
		rows[r] = flat[rowStart:len(flat):len(flat)]

		if (r+1)%batchSize == 0 {
			start := r + 1 - batchSize
			if _, err := w.WriteRows(rows[start : r+1]); err != nil {
				return nil, fmt.Errorf("error writing parquet rows: %w", err)
			}
		}
	}

	if tail := numRows - (numRows/batchSize)*batchSize; tail > 0 {
		if _, err := w.WriteRows(rows[numRows-tail:]); err != nil {
			return nil, fmt.Errorf("error writing parquet rows: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("error closing parquet writer: %w", err)
	}
	return out.Bytes(), nil
}
