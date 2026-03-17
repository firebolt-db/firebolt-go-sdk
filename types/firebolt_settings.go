package types

import "time"

type FireboltSettings struct {
	ClientID           string
	ClientSecret       string
	Database           string
	EngineName         string
	AccountName        string
	Url                string
	NewVersion         bool
	ClientSideLB       bool
	DNSTTL             time.Duration
	DefaultQueryParams map[string]string
}
