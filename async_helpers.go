package fireboltgosdk

import (
	"database/sql"
	"errors"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/logging"
	"github.com/firebolt-db/firebolt-go-sdk/rows"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
)

type QueryStatus string

const (
	QueryStatusRunning    QueryStatus = "RUNNING"
	QueryStatusSuccessful QueryStatus = "ENDED_SUCCESSFULLY"
	QueryStatusCanceled   QueryStatus = "CANCELED_EXECUTION"
)

type QueryStatusResponse struct {
	accountName   *string
	userName      *string
	submittedTime *time.Time
	startTime     *time.Time
	endTime       *time.Time
	status        *QueryStatus
	requestId     *string
	queryId       *string
	errorMessage  *string
	scannedBytes  *int64
	scannedRows   *int64
	retries       *int64
}

func getAsyncQueryStatus(db *sql.DB, token rows.AsyncResult) (*QueryStatusResponse, error) {
	asyncQueryStatusError := "failed to get async query status"
	if token.IsEmpty() {
		return nil, errorUtils.ConstructNestedError(asyncQueryStatusError, errors.New("async query token is empty"))
	}
	rows, err := db.Query(token.GetMonitorSQL())
	if err != nil {
		return nil, errorUtils.ConstructNestedError(asyncQueryStatusError, err)
	}
	var queryStatus QueryStatusResponse
	if rows.Next() {
		err = rows.Scan(
			&queryStatus.accountName, &queryStatus.userName, &queryStatus.submittedTime,
			&queryStatus.startTime, &queryStatus.endTime, &queryStatus.status,
			&queryStatus.requestId, &queryStatus.queryId, &queryStatus.errorMessage,
			&queryStatus.scannedBytes, &queryStatus.scannedRows, &queryStatus.retries,
		)
		if err != nil {
			return nil, errorUtils.ConstructNestedError(asyncQueryStatusError, err)
		}
		logging.Infolog.Printf("Async query status: %v", queryStatus)
		return &queryStatus, nil
	} else {
		return nil, errorUtils.ConstructNestedError(asyncQueryStatusError, errors.New("no rows found"))
	}
}

func IsAsyncQueryRunning(db *sql.DB, token rows.AsyncResult) (bool, error) {
	queryStatus, err := getAsyncQueryStatus(db, token)
	if err != nil {
		return false, err
	}
	if queryStatus.status == nil {
		return false, errorUtils.ConstructNestedError("failed to check async query running status", errors.New("query status is nil"))
	}
	return *queryStatus.status == QueryStatusRunning, nil
}

func IsAsyncQuerySuccessful(db *sql.DB, token rows.AsyncResult) (bool, error) {
	queryStatus, err := getAsyncQueryStatus(db, token)
	if err != nil {
		return false, err
	}
	if queryStatus.status == nil {
		return false, errorUtils.ConstructNestedError("failed to check async query success", errors.New("query status is nil"))
	}
	return *queryStatus.status == QueryStatusSuccessful, nil
}

const cancelSQL = "CANCEL QUERY WHERE query_id=?"

func CancelAsyncQuery(db *sql.DB, token rows.AsyncResult) error {
	queryStatus, err := getAsyncQueryStatus(db, token)
	if queryStatus.queryId == nil {
		return errorUtils.ConstructNestedError("failed to cancel async query", errors.New("query ID is nil"))
	}
	if err != nil {
		return errorUtils.ConstructNestedError("failed to cancel async query", err)
	}
	_, err = db.Exec(cancelSQL, *queryStatus.queryId)
	return err
}
