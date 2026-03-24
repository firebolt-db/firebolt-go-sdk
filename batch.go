package fireboltgosdk

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/client"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

// BatchMetric records timing for one Send() call, split into the
// serialisation phase and the network upload phase.
type BatchMetric struct {
	SerializeStart   time.Time
	SerializeSeconds float64
	UploadStart      time.Time
	UploadSeconds    float64
}

const parquetUploadName = "batch_data"

// BatchOption configures batch behaviour. Pass to PrepareBatch.
type BatchOption func(*batchConfig)

type batchConfig struct {
	bufferSize int64
}

// WithBufferSize sets the maximum number of rows buffered before the
// serialiser flushes to the underlying writer. Smaller values produce more
// incremental streaming (less peak memory) at a small metadata cost.
//
// The default is DefaultBufferSize (16 384).
// Setting n <= 0 reverts to the default.
func WithBufferSize(n int64) BatchOption {
	return func(c *batchConfig) {
		c.bufferSize = n
	}
}

// BatchConnection provides access to batch insert functionality.
// Obtain it via database/sql (*sql.Conn).Raw:
//
//	conn.Raw(func(driverConn interface{}) error {
//	    batch, err := driverConn.(fireboltgosdk.BatchConnection).PrepareBatch(
//	        ctx, "INSERT INTO my_table (col1, col2, col3)")
//	    if err != nil { return err }
//	    for _, row := range rows {
//	        if err := batch.Append(row.Col1, row.Col2, row.Col3); err != nil {
//	            return err
//	        }
//	    }
//	    return batch.Send(ctx)
//	})
type BatchConnection interface {
	PrepareBatch(ctx context.Context, query string, opts ...BatchOption) (Batch, error)
}

// Batch represents an in-progress batch insert operation.
// Rows are buffered client-side and serialised when Send is called.
// The serialised payload is uploaded to the engine via a multipart form POST.
//
// Two insertion modes are supported:
//
// Row-wise — call Append once per row:
//
//	batch.Append(col1Val, col2Val, col3Val)
//
// Columnar — obtain a column handle and append an entire typed slice at once:
//
//	batch.Column(0).Append([]int32{1, 2, 3})
//	batch.Column(1).Append([]string{"a", "b", "c"})
//
// Both modes can be mixed freely; the only requirement is that all columns
// have the same number of rows by the time Send is called.
type Batch interface {
	// Append buffers a single row. The number of arguments must match the
	// column count, and each value must be convertible to the column's type.
	Append(v ...interface{}) error

	// Column returns a handle for columnar appends to the column at the
	// given index. The returned BatchColumn is valid for the lifetime of
	// the batch.
	Column(index int) BatchColumn

	// Send serialises all buffered rows and uploads them to the engine.
	// The batch is reset after a successful send and can be reused.
	Send(ctx context.Context) error

	// Abort discards all buffered rows without sending.
	Abort() error

	// GetMetrics returns timing metrics for each Send() call made on this
	// batch (one entry per call, in chronological order).
	GetMetrics() []BatchMetric
}

// BatchColumn is returned by Batch.Column and supports appending an entire
// typed slice of values to a single column (columnar insertion).
type BatchColumn interface {
	// Append appends all values in the given slice to this column.
	// The slice element type must be compatible with the column's Firebolt
	// type (e.g. []int32 for an "int" column, []string for "text").
	Append(v interface{}) error
}

type fireboltBatch struct {
	conn      *fireboltConnection
	tableName string
	colNames  []string
	blk       *block
	metrics   []BatchMetric
}

type fireboltBatchColumn struct {
	blk   *block
	index int
}

// PrepareBatch prepares a batch insert. The query must be an INSERT INTO
// statement with an explicit column list:
//
//	INSERT INTO table_name (col1, col2, ...)
//
// Column types are discovered automatically by querying the table schema.
// Data is serialised and uploaded via multipart form POST.
//
// Optional BatchOption values can tune serialisation, e.g.:
//
//	batch, err := bc.PrepareBatch(ctx, query,
//	    fireboltgosdk.WithBufferSize(32768))
func (c *fireboltConnection) PrepareBatch(ctx context.Context, query string, opts ...BatchOption) (Batch, error) {
	if c.client == nil || c.engineUrl == "" {
		return nil, fmt.Errorf("connection is not initialized")
	}

	tableName, columnNames, err := parseInsertQuery(query)
	if err != nil {
		return nil, errorUtils.ConstructNestedError("error parsing INSERT query", err)
	}

	columnTypes, err := c.discoverColumnTypes(ctx, tableName, columnNames)
	if err != nil {
		return nil, errorUtils.ConstructNestedError("error discovering column types", err)
	}

	blk, err := newBlock(columnNames, columnTypes)
	if err != nil {
		return nil, errorUtils.ConstructNestedError("error creating block", err)
	}

	cfg := batchConfig{bufferSize: DefaultBufferSize}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.bufferSize > 0 {
		blk.bufferSize = cfg.bufferSize
	}

	return &fireboltBatch{
		conn:      c,
		tableName: tableName,
		colNames:  columnNames,
		blk:       blk,
	}, nil
}

// Append buffers one row of values.
func (b *fireboltBatch) Append(v ...interface{}) error {
	return b.blk.appendRow(v)
}

// Column returns a BatchColumn handle for columnar appends.
func (b *fireboltBatch) Column(index int) BatchColumn {
	return &fireboltBatchColumn{blk: b.blk, index: index}
}

// Append appends all values in the given slice to this column.
func (c *fireboltBatchColumn) Append(v interface{}) error {
	if c.index < 0 || c.index >= c.blk.numColumns() {
		return fmt.Errorf("column index %d out of range [0, %d)", c.index, c.blk.numColumns())
	}
	return c.blk.columnAt(c.index).appendColumn(v)
}

// Send serialises buffered rows and uploads them via multipart form POST.
// Serialisation is streamed directly into the HTTP body so the full payload
// never resides in a single buffer.
// The batch is reset on success and can accept new rows.
func (b *fireboltBatch) Send(ctx context.Context) error {
	if err := b.blk.validate(); err != nil {
		return errorUtils.ConstructNestedError("batch column length mismatch", err)
	}
	if b.blk.blockRows() == 0 {
		return nil
	}

	sql := buildParquetInsertQuery(b.tableName, b.colNames, parquetUploadName)

	control := client.ConnectionControl{
		UpdateParameters: b.conn.setParameter,
		SetEngineURL:     b.conn.setEngineURL,
		ResetParameters:  b.conn.resetParameters,
	}

	var m BatchMetric
	m.SerializeStart = time.Now()
	m.UploadStart = m.SerializeStart
	resp, err := b.conn.client.UploadParquet(ctx, b.conn.engineUrl, sql, b.blk, parquetUploadName, b.conn.parameters, control)
	elapsed := time.Since(m.UploadStart).Seconds()
	m.UploadSeconds = elapsed
	b.metrics = append(b.metrics, m)

	if err != nil {
		return errorUtils.ConstructNestedError("error uploading batch data", err)
	}
	resp.Body().Close()

	b.blk.reset()
	return nil
}

// GetMetrics returns timing metrics recorded for each Send() call.
func (b *fireboltBatch) GetMetrics() []BatchMetric {
	return b.metrics
}

// Abort discards all buffered rows without sending.
func (b *fireboltBatch) Abort() error {
	b.blk.reset()
	return nil
}

// ---------------------------------------------------------------------------
// INSERT query parsing
// ---------------------------------------------------------------------------

// parseInsertQuery extracts the table name and column list from a query like
// "INSERT INTO table_name (col1, col2, ...) [VALUES]".
func parseInsertQuery(query string) (tableName string, columns []string, err error) {
	trimmed := strings.TrimSpace(query)

	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "INSERT INTO ") {
		return "", nil, fmt.Errorf("query must start with INSERT INTO")
	}

	rest := strings.TrimSpace(trimmed[len("INSERT INTO "):])

	parenIdx := strings.Index(rest, "(")
	if parenIdx < 0 {
		return "", nil, fmt.Errorf("column list required, e.g. INSERT INTO table (col1, col2)")
	}

	tableName = strings.TrimSpace(rest[:parenIdx])
	if tableName == "" {
		return "", nil, fmt.Errorf("table name is empty")
	}
	if strings.ContainsAny(tableName, ";'\\") {
		return "", nil, fmt.Errorf("table name %q contains invalid characters", tableName)
	}

	closeIdx := strings.Index(rest[parenIdx:], ")")
	if closeIdx < 0 {
		return "", nil, fmt.Errorf("missing closing parenthesis in column list")
	}

	colStr := rest[parenIdx+1 : parenIdx+closeIdx]
	for _, part := range strings.Split(colStr, ",") {
		col := strings.TrimSpace(part)
		col = strings.Trim(col, "\"")
		col = strings.TrimSpace(col)
		if col != "" {
			columns = append(columns, col)
		}
	}

	if len(columns) == 0 {
		return "", nil, fmt.Errorf("no columns specified in INSERT query")
	}
	return tableName, columns, nil
}

// buildParquetInsertQuery constructs:
//
//	INSERT INTO table ("col1", "col2") SELECT * FROM read_parquet('upload://<fileName>')
//
// The column list is sorted alphabetically to match the Parquet schema order
// (parquet.Group sorts fields alphabetically). SELECT * reads columns in
// schema order, and the server maps them positionally to the INSERT column
// list, so both must be in the same order.
func buildParquetInsertQuery(tableName string, columnNames []string, fileName string) string {
	sorted := make([]string, len(columnNames))
	copy(sorted, columnNames)
	sort.Strings(sorted)

	quoted := make([]string, len(sorted))
	for i, name := range sorted {
		quoted[i] = fmt.Sprintf("\"%s\"", name)
	}
	quotedTable := tableName
	if !strings.Contains(tableName, "\"") {
		quotedTable = fmt.Sprintf("\"%s\"", tableName)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) SELECT * FROM read_parquet('upload://%s')",
		quotedTable, strings.Join(quoted, ", "), fileName)
}

// ---------------------------------------------------------------------------
// Schema discovery
// ---------------------------------------------------------------------------

// discoverColumnTypes runs a zero-row SELECT to retrieve the Firebolt type
// string for each column (e.g. "int", "text null", "array(long)").
func (c *fireboltConnection) discoverColumnTypes(ctx context.Context, tableName string, columnNames []string) ([]string, error) {
	quotedCols := make([]string, len(columnNames))
	for i, name := range columnNames {
		quotedCols[i] = fmt.Sprintf("\"%s\"", name)
	}
	quotedTable := tableName
	if !strings.Contains(tableName, "\"") {
		quotedTable = fmt.Sprintf("\"%s\"", tableName)
	}
	schemaSQL := fmt.Sprintf("SELECT %s FROM %s LIMIT 0",
		strings.Join(quotedCols, ", "), quotedTable)

	control := client.ConnectionControl{
		UpdateParameters: c.setParameter,
		SetEngineURL:     c.setEngineURL,
		ResetParameters:  c.resetParameters,
	}

	resp, err := c.client.Query(ctx, c.engineUrl, schemaSQL, c.parameters, control)
	if err != nil {
		return nil, fmt.Errorf("schema query failed: %w", err)
	}

	content, err := resp.Content()
	if err != nil {
		return nil, fmt.Errorf("error reading schema response: %w", err)
	}

	var qr types.QueryResponse
	if err := json.Unmarshal(content, &qr); err != nil {
		return nil, fmt.Errorf("error parsing schema response: %w", err)
	}

	if len(qr.Meta) != len(columnNames) {
		return nil, fmt.Errorf("expected %d columns in schema, got %d",
			len(columnNames), len(qr.Meta))
	}

	columnTypes := make([]string, len(qr.Meta))
	for i, col := range qr.Meta {
		columnTypes[i] = col.Type
	}
	return columnTypes, nil
}
