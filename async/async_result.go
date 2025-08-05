package async

type FireboltAsyncResult struct {
	token string
}

func NewFireboltAsyncResult(token string) FireboltAsyncResult {
	return FireboltAsyncResult{
		token: token,
	}
}

func (r FireboltAsyncResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r FireboltAsyncResult) RowsAffected() (int64, error) {
	return 0, nil
}

func (r FireboltAsyncResult) GetToken() string {
	return r.token
}
