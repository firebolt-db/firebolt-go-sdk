//go:build integration || integration_v0
// +build integration integration_v0

package client

import (
	"errors"
	"fmt"
	"testing"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"

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
	for _, newVersion := range []bool{true, false} {
		t.Run(fmt.Sprintf("newVersion=%v", newVersion), func(t *testing.T) {
			_, err := ClientFactory(&types.FireboltSettings{
				ClientID:     "TestAuthWrongCredential",
				ClientSecret: "wrong_secret",
				NewVersion:   newVersion,
			}, GetHostNameURL())
			if err == nil {
				t.Fatalf("Authentication with wrong secret didn't return an error for newVersion=%v", newVersion)
			}
			if !errors.Is(err, errorUtils.AuthenticationError) {
				t.Fatalf("Expected error to be of type AuthenticationError, got %v", err)
			}
		})
	}
}

// TestAuthEmptyCredential checks that authentication with empty password returns an error
func TestAuthEmptyCredential(t *testing.T) {
	for _, newVersion := range []bool{true, false} {
		t.Run(fmt.Sprintf("newVersion=%v", newVersion), func(t *testing.T) {
			_, err := ClientFactory(&types.FireboltSettings{
				ClientID:     "TestAuthEmptyCredential",
				ClientSecret: "",
				NewVersion:   newVersion,
			}, GetHostNameURL())
			if err == nil {
				t.Fatalf("Authentication with empty secret didn't return an error for newVersion=%v", newVersion)
			}
			if !errors.Is(err, errorUtils.AuthenticationError) {
				t.Fatalf("Expected error to be of type AuthenticationError, got %v", err)
			}
		})
	}
}
