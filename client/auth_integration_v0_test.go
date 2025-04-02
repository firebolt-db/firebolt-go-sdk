//go:build integration_v0
// +build integration_v0

package client

import (
	"testing"
)

// TestAuthWrongCredential checks that authentication with wrong credentials returns an error
func TestAuthWrongCredential(t *testing.T) {
	testAuthWrongCredential(t, false)
}

// TestAuthEmptyCredential checks that authentication with empty password returns an error
func TestAuthEmptyCredential(t *testing.T) {
	testAuthEmptyCredential(t, false)
}
