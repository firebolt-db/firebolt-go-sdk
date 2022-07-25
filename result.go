package fireboltgosdk

import (
	"log"
)

type FireboltResult struct {
}

// LastInsertId returns last inserted ID, not supported by firebolt
func (r FireboltResult) LastInsertId() (int64, error) {
	log.Printf("result LastInsertedId is called and always returns 0")
	return 0, nil
}

// RowsAffected returns a number of affected rows, not supported by firebolt
func (r FireboltResult) RowsAffected() (int64, error) {
	log.Printf("result RowsAffected is called and always returns 0")
	return 0, nil
}
