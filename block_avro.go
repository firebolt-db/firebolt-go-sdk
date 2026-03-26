package fireboltgosdk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hamba/avro/v2/ocf"
	"github.com/klauspost/compress/zstd"
)

// avroReader implements io.Reader over a block's Avro OCF serialisation.
// Each Read call drives the OCF encoder forward, producing compressed
// output into an internal buffer that is drained before producing more.
type avroReader struct {
	buf     bytes.Buffer
	enc     *ocf.Encoder
	cols    []column
	numRows int
	nextRow int
	batch   int
	done    bool
	row     map[string]interface{}
}

func (ar *avroReader) Read(p []byte) (int, error) {
	for ar.buf.Len() == 0 {
		if ar.done {
			return 0, io.EOF
		}
		if ar.nextRow < ar.numRows {
			end := ar.nextRow + ar.batch
			if end > ar.numRows {
				end = ar.numRows
			}
			for r := ar.nextRow; r < end; r++ {
				populateAvroRow(ar.row, ar.cols, r)
				if err := ar.enc.Encode(ar.row); err != nil {
					return 0, fmt.Errorf("error encoding avro row %d: %w", r, err)
				}
			}
			ar.nextRow = end
			if err := ar.enc.Flush(); err != nil {
				return 0, fmt.Errorf("error flushing avro block: %w", err)
			}
		} else {
			if err := ar.enc.Close(); err != nil {
				return 0, fmt.Errorf("error closing avro encoder: %w", err)
			}
			ar.done = true
		}
	}
	return ar.buf.Read(p)
}

// newAvroReader constructs a streaming io.Reader that produces Avro OCF data.
func (b *block) newAvroReader() (io.Reader, error) {
	numRows := b.blockRows()
	if numRows == 0 {
		return bytes.NewReader(nil), nil
	}

	schemaJSON, err := b.avroSchemaJSON()
	if err != nil {
		return nil, fmt.Errorf("error building avro schema: %w", err)
	}

	batchSize := int(b.bufferSize)
	if batchSize <= 0 {
		batchSize = int(DefaultBufferSize)
	}

	ar := &avroReader{
		cols:    b.columns,
		numRows: numRows,
		batch:   batchSize,
		row:     make(map[string]interface{}, len(b.columns)),
	}

	encOpts := append(b.avroEncoderOpts(), ocf.WithBlockLength(batchSize))
	enc, err := ocf.NewEncoder(schemaJSON, &ar.buf, encOpts...)
	if err != nil {
		return nil, fmt.Errorf("error creating avro OCF encoder: %w", err)
	}
	ar.enc = enc

	return ar, nil
}

// avroSchemaJSON builds an Avro record schema JSON string from the block's
// column definitions.
func (b *block) avroSchemaJSON() (string, error) {
	type avroField struct {
		Name string      `json:"name"`
		Type interface{} `json:"type"`
	}
	type avroRecord struct {
		Type   string      `json:"type"`
		Name   string      `json:"name"`
		Fields []avroField `json:"fields"`
	}

	fields := make([]avroField, len(b.columns))
	for i, col := range b.columns {
		fields[i] = avroField{
			Name: col.name(),
			Type: avroTypeForColumn(col),
		}
	}

	rec := avroRecord{
		Type:   "record",
		Name:   "firebolt",
		Fields: fields,
	}

	js, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("error marshalling avro schema: %w", err)
	}
	return string(js), nil
}

// avroTypeForColumn returns the Avro schema type representation for a column.
func avroTypeForColumn(col column) interface{} {
	switch c := col.(type) {
	case *int32Column:
		return "int"
	case *int64Column:
		return "long"
	case *float32Column:
		return "float"
	case *float64Column:
		return "double"
	case *stringColumn:
		return "string"
	case *boolColumn:
		return "boolean"
	case *dateColumn:
		return map[string]interface{}{"type": "int", "logicalType": "date"}
	case *timestampColumn:
		if c.adjusted {
			return map[string]interface{}{"type": "long", "logicalType": "timestamp-micros"}
		}
		return map[string]interface{}{"type": "long", "logicalType": "local-timestamp-micros"}
	case *byteaColumn:
		return "bytes"
	case *nullableColumn:
		inner := avroTypeForColumn(c.inner)
		return []interface{}{"null", inner}
	case *arrayColumn:
		elemType := avroTypeForColumn(c.elem)
		return map[string]interface{}{
			"type":  "array",
			"items": elemType,
		}
	default:
		return "string"
	}
}

// populateAvroRow fills the reusable map with values from row r.
func populateAvroRow(row map[string]interface{}, cols []column, r int) {
	for _, col := range cols {
		row[col.name()] = avroValueForColumn(col, r)
	}
}

// avroValueForColumn extracts the Go value for row r suitable for the Avro
// encoder (hamba/avro).
func avroValueForColumn(col column, r int) interface{} {
	switch c := col.(type) {
	case *int32Column:
		return int(c.data[r])
	case *int64Column:
		return c.data[r]
	case *float32Column:
		return c.data[r]
	case *float64Column:
		return c.data[r]
	case *stringColumn:
		return c.data[r]
	case *boolColumn:
		return c.data[r]
	case *dateColumn:
		return time.Unix(int64(c.data[r])*86400, 0).UTC()
	case *timestampColumn:
		return time.UnixMicro(c.data[r]).UTC()
	case *byteaColumn:
		return c.data[r]
	case *nullableColumn:
		if c.nulls[r] {
			return nil
		}
		return avroValueForColumn(c.inner, r)
	case *arrayColumn:
		return avroArrayValueForColumn(c, r)
	default:
		return nil
	}
}

// avroArrayValueForColumn extracts array elements for row r as a Go slice.
func avroArrayValueForColumn(ac *arrayColumn, r int) interface{} {
	var start uint64
	if r > 0 {
		start = ac.offsets[r-1]
	}
	end := ac.offsets[r]
	n := int(end - start)

	switch elem := ac.elem.(type) {
	case *int32Column:
		result := make([]int, n)
		for i := 0; i < n; i++ {
			result[i] = int(elem.data[start+uint64(i)])
		}
		return result
	case *int64Column:
		if n == 0 {
			return []int64{}
		}
		result := make([]int64, n)
		copy(result, elem.data[start:end])
		return result
	case *float32Column:
		if n == 0 {
			return []float32{}
		}
		result := make([]float32, n)
		copy(result, elem.data[start:end])
		return result
	case *float64Column:
		if n == 0 {
			return []float64{}
		}
		result := make([]float64, n)
		copy(result, elem.data[start:end])
		return result
	case *stringColumn:
		if n == 0 {
			return []string{}
		}
		result := make([]string, n)
		copy(result, elem.data[start:end])
		return result
	case *boolColumn:
		if n == 0 {
			return []bool{}
		}
		result := make([]bool, n)
		copy(result, elem.data[start:end])
		return result
	case *dateColumn:
		result := make([]time.Time, n)
		for i := 0; i < n; i++ {
			result[i] = time.Unix(int64(elem.data[start+uint64(i)])*86400, 0).UTC()
		}
		return result
	case *timestampColumn:
		result := make([]time.Time, n)
		for i := 0; i < n; i++ {
			result[i] = time.UnixMicro(elem.data[start+uint64(i)]).UTC()
		}
		return result
	case *byteaColumn:
		if n == 0 {
			return [][]byte{}
		}
		result := make([][]byte, n)
		copy(result, elem.data[start:end])
		return result
	default:
		result := make([]interface{}, n)
		for i := 0; i < n; i++ {
			result[i] = avroValueForColumn(ac.elem, int(start)+i)
		}
		return result
	}
}

// avroEncoderOpts returns the OCF encoder options for codec selection and
// optional compression level. LZ4 and Brotli are rejected earlier in
// PrepareBatch, so they never reach here.
func (b *block) avroEncoderOpts() []ocf.EncoderFunc {
	switch b.compression {
	case CompressGzip:
		if b.compressionLevelSet {
			return []ocf.EncoderFunc{ocf.WithCompressionLevel(b.compressionLevel)}
		}
		return []ocf.EncoderFunc{ocf.WithCodec(ocf.Deflate)}
	case CompressZstd:
		opts := []ocf.EncoderFunc{ocf.WithCodec(ocf.ZStandard)}
		if b.compressionLevelSet {
			opts = append(opts, ocf.WithZStandardEncoderOptions(zstd.WithEncoderLevel(zstd.EncoderLevel(b.compressionLevel))))
		}
		return opts
	case CompressUncompressed:
		return []ocf.EncoderFunc{ocf.WithCodec(ocf.Null)}
	default:
		return []ocf.EncoderFunc{ocf.WithCodec(ocf.Snappy)}
	}
}
