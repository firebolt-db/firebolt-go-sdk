package fireboltgosdk

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"slices"

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

// block holds column data and serialises it to Parquet or Avro format.
type block struct {
	columns             []column
	schema              *parquet.Schema
	leafIndices         []int
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
	blk.leafIndices = blk.leafColumnIndices()

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

// NewReader returns an io.Reader that produces the serialised contents of
// the block in the configured format (Parquet or Avro). Each call returns
// a fresh, independent reader so the same block can be retried on auth
// failure.
func (b *block) NewReader() (io.Reader, error) {
	if b.format == FormatAvro {
		return b.newAvroReader()
	}
	return b.newParquetReader()
}

// newParquetReader produces a Parquet-serialised io.Reader.
func (b *block) newParquetReader() (io.Reader, error) {
	numRows := b.blockRows()
	if numRows == 0 {
		return bytes.NewReader(nil), nil
	}

	type colVals struct {
		leafIdx  int
		values   []parquet.Value
		isArray  bool
		offsets  []uint64
		arrayPos int
	}
	cvs := make([]colVals, len(b.columns))
	for i, col := range b.columns {
		cv := colVals{
			leafIdx: b.leafIndices[i],
			values:  col.parquetValues(b.leafIndices[i]),
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
	slices.SortFunc(cvs, func(a, c colVals) int {
		return cmp.Compare(a.leafIdx, c.leafIdx)
	})

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
