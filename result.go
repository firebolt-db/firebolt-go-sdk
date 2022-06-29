package fireboltgosdk

type FireboltResult struct {
	str string
}

func (r FireboltResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r FireboltResult) RowsAffected() (int64, error) {
	return 0, nil
}
