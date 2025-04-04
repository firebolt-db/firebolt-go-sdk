//go:build integration
// +build integration

package client

import (
	"testing"
)

// TestAuthWrongCredential checks that authentication with wrong credentials returns an error
func TestAuthWrongCredential(t *testing.T) {
	testAuthWrongCredential(t, true)
}

// TestAuthEmptyCredential checks that authentication with empty password returns an error
func TestAuthEmptyCredential(t *testing.T) {
	testAuthEmptyCredential(t, true)
}
