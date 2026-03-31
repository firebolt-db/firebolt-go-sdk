package types

import (
	"net/http"
	"time"
)

type FireboltSettings struct {
	ClientID               string
	ClientSecret           string
	Database               string
	EngineName             string
	AccountName            string
	Url                    string
	NewVersion             bool
	ClientSideLB           bool
	DNSTTL                 time.Duration
	ClientSideLBHC         bool
	ClientSideLBHCURL      string
	ClientSideLBHCInterval time.Duration
	Transport              http.RoundTripper
	DefaultQueryParams     map[string]string
}
