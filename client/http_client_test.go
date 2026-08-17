package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type responseReadCloser struct {
	data       []byte
	readErr    error
	closeErr   error
	readCalls  int
	closeCalls int
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func (r *responseReadCloser) Read(p []byte) (int, error) {
	r.readCalls++
	if r.data == nil {
		return 0, io.EOF
	}
	data := r.data
	r.data = nil
	return copy(p, data), r.readErr
}

func (r *responseReadCloser) Close() error {
	r.closeCalls++
	return r.closeErr
}

func TestResponseContentPreservesReadAndCloseErrors(t *testing.T) {
	readErr := errors.New("read failed")
	closeErr := errors.New("close failed")
	body := &responseReadCloser{data: []byte("partial"), readErr: readErr, closeErr: closeErr}
	response := MakeResponse(body, 200, nil, nil)

	content, err := response.Content()
	if string(content) != "partial" || !errors.Is(err, readErr) || !errors.Is(err, closeErr) {
		t.Fatalf("Content() = %q, %v; want partial content and both errors", content, err)
	}
	reads := body.readCalls
	if _, cachedErr := response.Content(); !errors.Is(cachedErr, readErr) || body.readCalls != reads || body.closeCalls != 1 {
		t.Fatalf("cached Content() re-read or lost its error: reads=%d closes=%d err=%v", body.readCalls, body.closeCalls, cachedErr)
	}
}

func TestResponseContentClosesSuccessfulBody(t *testing.T) {
	body := &responseReadCloser{data: []byte("complete")}
	response := MakeResponse(body, 200, nil, nil)

	content, err := response.Content()
	if err != nil || string(content) != "complete" || body.closeCalls != 1 {
		t.Fatalf("Content() = %q, %v; close calls = %d", content, err, body.closeCalls)
	}
}

func TestQueryClosesBodyWhenResponseHeadersAreInvalid(t *testing.T) {
	body := &responseReadCloser{data: []byte("unused")}
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{updateEndpointHeader: []string{"%zz"}},
			Body:       body,
		}, nil
	})}
	baseClient := &BaseClient{
		HttpClient:        httpClient,
		UserAgent:         "test-agent",
		AccessTokenGetter: func() (string, error) { return "token", nil },
		ParameterGetter: func(context.Context, map[string]string) (map[string]string, error) {
			return nil, nil
		},
	}

	if _, err := baseClient.Query(context.Background(), "http://example.com", "SELECT 1", nil, ConnectionControl{}); err == nil {
		t.Fatal("Query() error = nil, want invalid response-header error")
	}
	if body.closeCalls != 1 {
		t.Fatalf("response body close calls = %d, want 1", body.closeCalls)
	}
}

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
	resp := DoHttpRequest(nil, reqParams)

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

	resp := DoHttpRequest(nil, reqParams)

	if resp.err == nil {
		t.Error("Expected DoHttpRequest to return an error for malformed URL, got nil")
	}

	errorMsg := resp.err.Error()
	if !strings.Contains(errorMsg, "POST") {
		t.Errorf("Expected error message to contain method 'POST', got: %s", errorMsg)
	}
}
