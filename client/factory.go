package client

import (
	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

// ClientFactory sends an authentication request, and returns a newly constructed client object
func ClientFactory(settings *types.FireboltSettings, apiEndpoint string) (Client, error) {
	userAgent := ConstructUserAgentString()

	if settings.NewVersion {
		_, err := getAccessTokenServiceAccount(settings.ClientID, settings.ClientSecret, apiEndpoint, userAgent)
		if err != nil {
			return nil, errors.ConstructNestedError("error while getting access token", err)
		} else {
			return MakeClient(settings, apiEndpoint)
		}
	} else {
		_, err := getAccessTokenUsernamePassword(settings.ClientID, settings.ClientSecret, apiEndpoint, userAgent)
		if err != nil {
			return nil, errors.ConstructNestedError("error while getting access token", err)
		} else {
			return MakeClientV0(settings, apiEndpoint)
		}
	}
}
