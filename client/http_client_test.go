package client

import (
	"context"
	"strings"
	"testing"
)

// TestDoHttpRequestMalformedURL tests that DoHttpRequest returns an error (not panic)
// when given a malformed URL that causes http.NewRequestWithContext to fail
func TestDoHttpRequestMalformedURL(t *testing.T) {
	// Test with a malformed URL that will cause url.Parse to fail
	// "%zz" is an invalid percent-encoded sequence
	malformedURL := "http://%zz"

	reqParams := requestParameters{
		ctx:         context.Background(),
		accessToken: "",
		method:      "GET",
		url:         malformedURL,
		userAgent:   "test-agent",
		params:      nil,
		bodyStr:     "",
		contentType: "",
	}

	// This should return an error, not panic
	resp := DoHttpRequest(reqParams)

	if resp.err == nil {
		t.Error("Expected DoHttpRequest to return an error for malformed URL, got nil")
	}

	// Verify the error message contains useful information
	errorMsg := resp.err.Error()
	if errorMsg == "" {
		t.Error("Expected non-empty error message")
	}

	// Verify it mentions the method and URL in the error
	if !strings.Contains(errorMsg, "GET") {
		t.Errorf("Expected error message to contain method 'GET', got: %s", errorMsg)
	}
}

// TestDoHttpRequestMalformedURLWithPercent2 tests another malformed URL case
func TestDoHttpRequestMalformedURLWithPercent2(t *testing.T) {
	// "%2" is an incomplete percent-encoded sequence
	malformedURL := "http://%2"

	reqParams := requestParameters{
		ctx:         context.Background(),
		accessToken: "",
		method:      "POST",
		url:         malformedURL,
		userAgent:   "test-agent",
		params:      nil,
		bodyStr:     "",
		contentType: "",
	}

	resp := DoHttpRequest(reqParams)

	if resp.err == nil {
		t.Error("Expected DoHttpRequest to return an error for malformed URL, got nil")
	}

	errorMsg := resp.err.Error()
	if !strings.Contains(errorMsg, "POST") {
		t.Errorf("Expected error message to contain method 'POST', got: %s", errorMsg)
	}
}
