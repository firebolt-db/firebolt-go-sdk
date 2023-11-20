//go:build integration_v0
// +build integration_v0

package fireboltgosdk

import (
	"testing"
)

// TestAuthHappyPath tests normal authentication, and that the access token is actually set
func TestAuthHappyPath(t *testing.T) {
	if len(getCachedAccessToken(clientMock.Username, clientMock.ApiEndpoint)) == 0 {
		t.Errorf("Token is not set properly")
	}
}

// TestAuthWrongCredential checks that authentication with wrong credentials returns an error
func TestAuthWrongCredential(t *testing.T) {
	if _, err := Authenticate("TestAuthWrongCredential", "wrong_password", GetHostNameURL()); err == nil {
		t.Errorf("Authentication with wrong credentials didn't return an error")
	}
}

// TestAuthEmptyCredential checks that authentication with empty password returns an error
func TestAuthEmptyCredential(t *testing.T) {
	if _, err := Authenticate("TestAuthEmptyCredential", "", GetHostNameURL()); err == nil {
		t.Errorf("Authentication with empty password didn't return an error")
	}
}
