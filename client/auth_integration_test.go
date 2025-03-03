//go:build integration || integration_v0
// +build integration integration_v0

package client

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

// TestAuthHappyPath tests normal authentication, and that the access token is actually set
func TestAuthHappyPath(t *testing.T) {
	if len(getCachedAccessToken(clientMock.ClientID, clientMock.ApiEndpoint)) == 0 {
		t.Errorf("Token is not set properly")
	}
}

// TestAuthWrongCredential checks that authentication with wrong credentials returns an error
func TestAuthWrongCredential(t *testing.T) {
	if _, err := ClientFactory(&types.FireboltSettings{
		ClientID:     "TestAuthWrongCredential",
		ClientSecret: "wrong_secret",
		NewVersion:   true,
	}, GetHostNameURL()); err == nil {
		t.Errorf("Authentication with wrong credentials didn't return an error for service account authentication")
	}

	if _, err := ClientFactory(&types.FireboltSettings{
		ClientID:     "TestAuthWrongCredential",
		ClientSecret: "wrong_password",
		NewVersion:   false,
	}, GetHostNameURL()); err == nil {
		t.Errorf("Authentication with wrong credentials didn't return an error for username/password authentication")
	}
}

// TestAuthEmptyCredential checks that authentication with empty password returns an error
func TestAuthEmptyCredential(t *testing.T) {
	if _, err := ClientFactory(&types.FireboltSettings{
		ClientID:     "TestAuthEmptyCredential",
		ClientSecret: "",
		NewVersion:   true,
	}, GetHostNameURL()); err == nil {
		t.Errorf("Authentication with empty password didn't return an error for service account authentication")
	}

	if _, err := ClientFactory(&types.FireboltSettings{
		ClientID:     "TestAuthEmptyCredential",
		ClientSecret: "",
		NewVersion:   false,
	}, GetHostNameURL()); err == nil {
		t.Errorf("Authentication with empty password didn't return an error for username/password authentication")
	}
}
