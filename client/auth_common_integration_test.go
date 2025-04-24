//go:build integration || integration_v0
// +build integration integration_v0

package client

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

// TestAuthHappyPath tests normal authentication, and that the access token is actually set
func TestAuthHappyPath(t *testing.T) {
	if len(getCachedAccessToken(clientMock.ClientID, clientMock.ApiEndpoint)) == 0 {
		t.Errorf("Token is not set properly")
	}
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func testAuthWrongCredential(t *testing.T, newVersion bool) {
	wrong_client_id := "wrong_client_id" + randomString(10)
	wrong_secret := "wrong_secret" + randomString(10)
	_, err := ClientFactory(&types.FireboltSettings{
		ClientID:     wrong_client_id,
		ClientSecret: wrong_secret,
		NewVersion:   newVersion,
	}, GetHostNameURL())
	if err == nil {
		t.Fatalf("Authentication with wrong secret didn't return an error for newVersion=%v", newVersion)
	}
	if !errors.Is(err, errorUtils.AuthenticationError) {
		t.Fatalf("Expected error to be of type AuthenticationError, got %v", err)
	}
}

func testAuthEmptyCredential(t *testing.T, newVersion bool) {
	_, err := ClientFactory(&types.FireboltSettings{
		ClientID:     "test_auth_empty_credential",
		ClientSecret: "",
		NewVersion:   newVersion,
	}, GetHostNameURL())
	if err == nil {
		t.Fatalf("Authentication with empty secret didn't return an error for newVersion=%v", newVersion)
	}
	if !errors.Is(err, errorUtils.AuthenticationError) {
		t.Fatalf("Expected error to be of type AuthenticationError, got %v", err)
	}
}
