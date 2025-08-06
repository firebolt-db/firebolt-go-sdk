package async

import (
	"database/sql"
	"errors"
	"time"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
)

type QueryStatus string

const (
	QueryStatusRunning    QueryStatus = "RUNNING"
	QueryStatusSuccessful QueryStatus = "ENDED_SUCCESSFULLY"
)

type QueryStatusResponse struct {
	accountName   string
	userName      string
	submittedTime time.Time
	startTime     time.Time
	endTime       time.Time
	status        QueryStatus
	requestId     string
	queryId       string
	errorMessage  string
	scannedBytes  int64
	scannedRows   int64
	retries       int64
}

func getAsyncQueryStatus(db *sql.DB, token AsyncResult) (*QueryStatusResponse, error) {
	if token.IsEmpty() {
		return nil, errors.New("async query token is empty")
	}
	rows, err := db.Query(token.GetMonitorSQL())
	if err != nil {
		return nil, errorUtils.ConstructNestedError("failed to get async query status", err)
	}
	var queryStatus QueryStatusResponse
	err = rows.Scan(
		&queryStatus.accountName, &queryStatus.userName, &queryStatus.submittedTime,
		&queryStatus.startTime, &queryStatus.endTime, &queryStatus.status,
		&queryStatus.requestId, &queryStatus.queryId, &queryStatus.errorMessage,
		&queryStatus.scannedBytes, &queryStatus.scannedRows, &queryStatus.retries,
	)
	if err != nil {
		return nil, errorUtils.ConstructNestedError("failed to get async query status", err)
	}
	return &queryStatus, nil
}

func IsAsyncQueryRunning(db *sql.DB, token AsyncResult) (bool, error) {
	queryStatus, err := getAsyncQueryStatus(db, token)
	if err != nil {
		return false, err
	}
	return queryStatus.status == QueryStatusRunning, nil
}

func IsAsyncQuerySuccessful(db *sql.DB, token AsyncResult) (bool, error) {
	queryStatus, err := getAsyncQueryStatus(db, token)
	if err != nil {
		return false, err
	}
	return queryStatus.status == QueryStatusSuccessful, nil
}

const cancelSQL = "CANCEL QUERY WHERE query_id=?"

func CancelAsyncQuery(db *sql.DB, token AsyncResult) error {
	queryStatus, err := getAsyncQueryStatus(db, token)
	if err != nil {
		return errorUtils.ConstructNestedError("failed to cancel async query", err)
	}
	_, err = db.Exec(cancelSQL, queryStatus.queryId)
	return err
}
