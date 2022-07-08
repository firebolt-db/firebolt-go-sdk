package fireboltgosdk

type FireboltResult struct {
	str string
}

// LastInsertId returns last inserted ID, not supported by firebolt
func (r FireboltResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected returns a number of affected rows, not supported by firebolt
func (r FireboltResult) RowsAffected() (int64, error) {
	return 0, nil
}
