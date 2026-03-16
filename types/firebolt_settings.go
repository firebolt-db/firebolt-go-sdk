package types

type FireboltSettings struct {
	ClientID           string
	ClientSecret       string
	Database           string
	EngineName         string
	AccountName        string
	Url                string
	NewVersion         bool
	ClientSideLB       bool
	DefaultQueryParams map[string]string
}
