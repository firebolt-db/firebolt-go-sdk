package rows

import (
	"reflect"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type columnRecord struct {
	name   string
	fbType fireboltType
}

type ColumnReader struct {
	columns []columnRecord
}

func (r *ColumnReader) setColumns(columns []types.Column) error {
	r.columns = make([]columnRecord, len(columns))
	for i, column := range columns {
		fbType, err := parseType(column.Type)
		if err != nil {
			return err
		}
		r.columns[i] = columnRecord{
			name:   column.Name,
			fbType: fbType,
		}
	}
	return nil

}

// Columns returns a list of column names in the current row set
func (r *ColumnReader) Columns() []string {
	numColumns := len(r.columns)
	result := make([]string, 0, numColumns)

	for _, column := range r.columns {
		result = append(result, column.name)
	}

	return result
}

func (r *ColumnReader) ColumnTypeScanType(index int) reflect.Type {
	return r.columns[index].fbType.goType
}

func (r *ColumnReader) ColumnTypeDatabaseTypeName(index int) string {
	return r.columns[index].fbType.dbType
}

func (r *ColumnReader) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.columns[index].fbType.isNullable, true
}

func (r *ColumnReader) ColumnTypeLength(index int) (length int64, ok bool) {
	if r.columns[index].fbType.length > 0 {
		return r.columns[index].fbType.length, true
	}
	return 0, false
}

// max returns the maximum of two int64 values. Go only has integer max starting from Go 1.21
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (r *ColumnReader) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	hasPrecisionScale := r.columns[index].fbType.precision > 0 || r.columns[index].fbType.scale > 0
	return max(0, r.columns[index].fbType.precision), max(0, r.columns[index].fbType.scale), hasPrecisionScale
}
