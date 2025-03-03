package rows

import "database/sql/driver"

type StreamRows struct {
	// TODO: add fields
}

// Columns returns a list of Meta names in response
func (r *StreamRows) Columns() []string {
	// TODO: implement this method
	return nil
}

// Close makes the rows unusable
func (r *StreamRows) Close() error {
	// TODO: implement this method
	return nil
}

// Next fetches the values of the next row, returns io.EOF if it was the end
func (r *StreamRows) Next(dest []driver.Value) error {
	// TODO: implement this method
	return nil
}

// HasNextResultSet reports whether there is another result set available
func (r *StreamRows) HasNextResultSet() bool {
	// TODO: implement this method
	return false
}

// NextResultSet advances to the next result set, if it is available, otherwise returns io.EOF
func (r *StreamRows) NextResultSet() error {
	// TODO: implement this method
	return nil
}
