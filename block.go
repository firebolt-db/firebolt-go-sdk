package fireboltgosdk

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// DefaultBufferSize is the default number of rows buffered before the
// serialiser flushes to the underlying writer, enabling true streaming:
// compressed data flows to the HTTP transport incrementally instead of
// buffering the entire file in memory. Override via WithBufferSize.
const DefaultBufferSize int64 = 16384

// blockReader implements io.Reader over a block's Parquet serialisation.
// Each Read call drives the parquet.Writer forward, producing compressed
// output into a small internal buffer that is drained before producing more.
type blockReader struct {
	buf       bytes.Buffer
	pw        *parquet.GenericWriter[any]
	rows      []parquet.Row
	numRows   int
	nextRow   int
	batchSize int
	done      bool
}

func (br *blockReader) Read(p []byte) (int, error) {
	for br.buf.Len() == 0 {
		if br.done {
			return 0, io.EOF
		}
		if br.nextRow < br.numRows {
			end := br.nextRow + br.batchSize
			if end > br.numRows {
				end = br.numRows
			}
			if _, err := br.pw.WriteRows(br.rows[br.nextRow:end]); err != nil {
				return 0, fmt.Errorf("error writing parquet rows: %w", err)
			}
			br.nextRow = end
		} else {
			if err := br.pw.Close(); err != nil {
				return 0, fmt.Errorf("error closing parquet writer: %w", err)
			}
			br.done = true
		}
	}
	return br.buf.Read(p)
}

// block holds column data and serialises it to the configured format.
// columnLeaf is one Parquet leaf contributed by a column. Scalar and array
// columns contribute exactly one; ARRAY(STRUCT(...)) contributes one per field.
type columnLeaf struct {
	// path is the leaf's path in the schema, used to resolve its column index.
	path []string
	// values returns the leaf's values, tagged with the given column index.
	values func(colIdx int) []parquet.Value
	// offsets returns the per-row cumulative element count for a repeated
	// leaf, or nil for a scalar one. It is a function because the backing
	// slice grows as rows are appended, long after the leaf is described.
	offsets func() []uint64
}

// leafProvider is implemented by columns spanning more than one Parquet leaf.
type leafProvider interface {
	leaves() []columnLeaf
}

type block struct {
	columns             []column
	schema              *parquet.Schema
	leaves              []blockLeaf
	bufferSize          int64
	format              SerializationFormat
	compression         CompressionCodec
	compressionLevel    int
	compressionLevelSet bool
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

	blk := &block{
		columns:    cols,
		bufferSize: DefaultBufferSize,
	}

	group := make(parquet.Group, len(cols))
	for _, col := range cols {
		group[col.name()] = col.parquetNode()
	}
	blk.schema = parquet.NewSchema("firebolt", group)

	leaves, err := blk.buildLeaves()
	if err != nil {
		return nil, err
	}
	blk.leaves = leaves

	return blk, nil
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
		before := col.rows()
		if err := col.appendRow(values[i]); err != nil {
			// The failing column may have applied part of the value before
			// returning. Every earlier column completed exactly one appendRow,
			// so undo those rows as well and leave the whole operation atomic.
			col.truncate(before)
			for j := 0; j < i; j++ {
				prev := b.columns[j]
				prev.truncate(prev.rows() - 1)
			}
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

// blockLeaf is a columnLeaf with its resolved Parquet column index.
type blockLeaf struct {
	columnLeaf
	leafIdx int
}

// buildLeaves enumerates every Parquet leaf the block writes, resolving each
// one's column index from the schema.
//
// Indices come from the schema rather than from re-deriving parquet.Group's
// field ordering: Lookup is authoritative, and it stays correct for nested
// groups where the ordering rules are not just "sort the top level".
func (b *block) buildLeaves() ([]blockLeaf, error) {
	var out []blockLeaf
	for _, col := range b.columns {
		var ls []columnLeaf
		if ml, ok := col.(leafProvider); ok {
			ls = ml.leaves()
		} else {
			c := col
			ls = []columnLeaf{{
				path:    []string{c.name()},
				values:  c.parquetValues,
				offsets: repeatedOffsets(c),
			}}
		}

		for _, l := range ls {
			leaf, ok := b.schema.Lookup(l.path...)
			if !ok {
				return nil, fmt.Errorf("column %q: leaf %q is not in the schema",
					col.name(), strings.Join(l.path, "."))
			}
			out = append(out, blockLeaf{columnLeaf: l, leafIdx: leaf.ColumnIndex})
		}
	}
	return out, nil
}

// repeatedOffsets returns an accessor for a repeated column's per-row element
// counts, or nil when the column is scalar.
func repeatedOffsets(col column) func() []uint64 {
	switch c := col.(type) {
	case *arrayColumn:
		return func() []uint64 { return c.offsets }
	case *nullableColumn:
		if ac, ok := c.inner.(*arrayColumn); ok {
			return func() []uint64 { return ac.offsets }
		}
	}
	return nil
}

// NewReader returns an io.Reader that produces the serialised contents of
// the block in the configured format. Each call returns a fresh, independent
// reader so the same block can be retried on auth failure.
func (b *block) NewReader() (io.Reader, error) {
	return b.newParquetReader()
}

// newParquetReader produces a Parquet-serialised io.Reader.
func (b *block) newParquetReader() (io.Reader, error) {
	numRows := b.blockRows()
	if numRows == 0 {
		return bytes.NewReader(nil), nil
	}

	type leafVals struct {
		leafIdx int
		values  []parquet.Value
		// offsets is nil for a scalar leaf, giving one value per row.
		offsets []uint64
		pos     int
	}

	lvs := make([]leafVals, len(b.leaves))
	for i, l := range b.leaves {
		lv := leafVals{
			leafIdx: l.leafIdx,
			values:  l.values(l.leafIdx),
		}
		if l.offsets != nil {
			lv.offsets = l.offsets()
		}
		lvs[i] = lv
	}
	// Rows must present their values in leaf order.
	slices.SortFunc(lvs, func(a, c leafVals) int {
		return cmp.Compare(a.leafIdx, c.leafIdx)
	})

	totalValues := 0
	for _, lv := range lvs {
		if lv.offsets == nil {
			totalValues += numRows
		} else {
			totalValues += len(lv.values)
		}
	}

	flat := make([]parquet.Value, 0, totalValues)
	rows := make([]parquet.Row, numRows)

	for r := range numRows {
		rowStart := len(flat)
		for li := range lvs {
			lv := &lvs[li]
			if lv.offsets == nil {
				flat = append(flat, lv.values[r])
				continue
			}
			var start uint64
			if r > 0 {
				start = lv.offsets[r-1]
			}
			end := lv.offsets[r]
			if start == end {
				// An empty repeated field still contributes one value, which
				// carries the null that encodes "no elements".
				flat = append(flat, lv.values[lv.pos])
				lv.pos++
				continue
			}
			n := int(end - start)
			flat = append(flat, lv.values[lv.pos:lv.pos+n]...)
			lv.pos += n
		}
		rows[r] = flat[rowStart:len(flat):len(flat)]
	}

	batchSize := int(b.bufferSize)
	if batchSize <= 0 {
		batchSize = int(DefaultBufferSize)
	}
	br := &blockReader{numRows: numRows, rows: rows, batchSize: batchSize}
	br.pw = parquet.NewGenericWriter[any](&br.buf, b.schema,
		parquet.Compression(b.parquetCodec()),
		parquet.DataPageStatistics(false),
		parquet.MaxRowsPerRowGroup(b.bufferSize),
	)
	return br, nil
}

func (b *block) parquetCodec() compress.Codec {
	switch b.compression {
	case CompressZstd:
		c := &zstd.Codec{}
		if b.compressionLevelSet {
			c.Level = zstd.Level(b.compressionLevel)
		}
		return c
	case CompressGzip:
		c := &gzip.Codec{}
		if b.compressionLevelSet {
			c.Level = b.compressionLevel
		}
		return c
	case CompressUncompressed:
		return &uncompressed.Codec{}
	case CompressLZ4:
		c := &lz4.Codec{}
		if b.compressionLevelSet {
			c.Level = lz4.Level(b.compressionLevel)
		}
		return c
	case CompressBrotli:
		c := &brotli.Codec{}
		if b.compressionLevelSet {
			c.Quality = b.compressionLevel
		}
		return c
	default:
		return &snappy.Codec{}
	}
}

// toParquet serialises all buffered data into Parquet format in memory.
func (b *block) toParquet() ([]byte, error) {
	r, err := b.NewReader()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return nil, err
	}
	if buf.Len() == 0 {
		return nil, nil
	}
	return buf.Bytes(), nil
}
