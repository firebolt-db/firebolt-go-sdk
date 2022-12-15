//go:build integration
// +build integration

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

//
//// TestAuthServiceAccount checks that authentication with service account works
//func TestAuthServiceAccount(t *testing.T) {
//	serviceAccountClientId = os.Getenv("SERVICE_ACCOUNT_CLIENT_ID")
//	serviceAccountClientSecret = os.Getenv("SERVICE_ACCOUNT_CLIENT_SECRET")
//	if len(serviceAccountClientId) == 0 {
//		t.Errorf("Could not run the test because the client id is not set (environment var: SERVICE_ACCOUNT_CLIENT_ID")
//	}
//	if len(serviceAccountClientSecret) == 0 {
//		t.Errorf("Could not run the test because the client secret is not set (environment var: SERVICE_ACCOUNT_CLIENT_SECRET")
//	}
//	client, err := Authenticate(serviceAccountClientId, serviceAccountClientSecret, GetHostNameURL())
//	if err != nil {
//		t.Errorf("Could not authenticate using service account")
//	}
//
//	if serviceAccountClientId != client.Username {
//		t.Errorf("The username set in the client doesn't match the service account client id")
//	}
//
//	if serviceAccountClientSecret != client.Password {
//		t.Errorf("The password set in the client doesn't match the service account client secret")
//	}
//	if len(getCachedAccessToken(serviceAccountClientId, GetHostNameURL())) == 0 {
//		t.Errorf("Token is not set properly")
//	}
//
//}
