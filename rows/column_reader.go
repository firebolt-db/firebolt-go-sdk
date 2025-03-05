package rows

import "github.com/firebolt-db/firebolt-go-sdk/types"

type ColumnReader struct {
	columns []types.Column
}

// Columns returns a list of column names in the current row set
func (r *ColumnReader) Columns() []string {
	numColumns := len(r.columns)
	result := make([]string, 0, numColumns)

	for _, column := range r.columns {
		result = append(result, column.Name)
	}

	return result
}
