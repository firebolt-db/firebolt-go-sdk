package fireboltgosdk

import (
	"testing"
)

// TestAuthHappyPath tests normal authentication, and that the access token is actually set
func TestAuthHappyPath(t *testing.T) {
	markIntegrationTest(t)

	if len(clientMock.AccessToken) == 0 {
		t.Errorf("Token is not set properly")
	}
}

// TestAuthWrongCredential checks that authentication with wrong credentials returns an error
func TestAuthWrongCredential(t *testing.T) {
	markIntegrationTest(t)

	if _, err := Authenticate(usernameMock, "wrong_password"); err == nil {
		t.Errorf("Authentication with wrong credentials didn't return an error")
	}
}

// TestAuthEmptyCredential checks that authentication with empty password returns an error
func TestAuthEmptyCredential(t *testing.T) {
	markIntegrationTest(t)

	if _, err := Authenticate(usernameMock, ""); err == nil {
		t.Errorf("Authentication with empty password didn't return an error")
	}
}
