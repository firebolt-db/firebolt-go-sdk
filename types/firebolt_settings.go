package types

type FireboltSettings struct {
	ClientID           string
	ClientSecret       string
	Database           string
	EngineName         string
	AccountName        string
	Url                string
	NewVersion         bool
	DefaultQueryParams map[string]string
}
