package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"strings"
	"time"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

// NewHttpClient creates an *http.Client with a transport configured for
// connection pooling. Callers should reuse the returned client across
// requests so that idle TCP/TLS connections are kept alive.
func NewHttpClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// NewHttpClientForLB creates an *http.Client suitable for client-side load
// balancing. When the resolver rewrites URLs to use raw IP addresses,
// tlsServerName ensures TLS certificate verification still uses the original
// hostname. Pass an empty string if TLS is not used (plain HTTP).
func NewHttpClientForLB(tlsServerName string) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if tlsServerName != "" {
		transport.TLSClientConfig = &tls.Config{ServerName: tlsServerName}
	}
	return &http.Client{Transport: transport}
}

type Response struct {
	body       io.ReadCloser
	content    []byte
	statusCode int
	headers    http.Header
	err        error
}

func MakeResponse(body io.ReadCloser, statusCode int, headers http.Header, err error) *Response {
	response := &Response{body, nil, statusCode, headers, err}

	if response.err == nil && (statusCode < 200 || statusCode >= 300) {
		if err := checkErrorResponse(response); err != nil {
			response.err = errorUtils.ConstructNestedError("request returned an error", err)
		} else if statusCode == 500 {
			response.err = errors.New(string(response.content))
		} else {
			response.err = errorUtils.ConstructNestedError("request returned non ok status code", errors.New(string(response.content)))
		}
	}
	return response
}

func (r *Response) Body() io.ReadCloser {
	return r.body
}

func (r *Response) Content() ([]byte, error) {
	var err error
	if r.content == nil {
		if r.body == nil {
			r.content = []byte{}
		} else {
			r.content, err = io.ReadAll(r.body)
			if err != nil {
				err = r.body.Close()
			}
		}
	}
	return r.content, err
}

func (r *Response) IsAsyncResponse() bool {
	return r.statusCode == 202
}

// Collect arguments for request function
type requestParameters struct {
	ctx          context.Context
	accessToken  string
	method       string
	url          string
	userAgent    string
	params       map[string]string
	bodyStr      string
	contentType  string
	hostOverride string // when non-empty, sent as the Host header (used by client-side LB)
}

// requestParametersMultipart collects arguments for a multipart form upload.
type requestParametersMultipart struct {
	ctx          context.Context
	accessToken  string
	url          string
	userAgent    string
	params       map[string]string
	sql          string
	fileData     []byte
	fileName     string
	hostOverride string // when non-empty, sent as the Host header (used by client-side LB)
}

// checkErrorResponse, checks whether error Response is returned instead of a desired Response.
func checkErrorResponse(response *Response) error {
	// ErrorResponse definition of any Response with some error
	type ErrorResponse struct {
		Error   string        `json:"error"`
		Code    int           `json:"code"`
		Message string        `json:"message"`
		Details []interface{} `json:"details"`
	}

	content, err := response.Content()
	if err != nil {
		return errorUtils.ConstructNestedError("error during reading response from the server", err)
	}

	var errorResponse ErrorResponse

	if err := json.Unmarshal(content, &errorResponse); err == nil && errorResponse.Code != 0 {
		// return error only if error Response was
		// unmarshalled correctly and error code is not zero
		return errors.New(errorResponse.Message)
	}
	return nil
}

func extractAdditionalHeaders(ctx context.Context) map[string]string {
	additionalHeaders, ok := contextUtils.GetAdditionalHeaders(ctx)
	if ok {
		// only take headers that start with Firebolt- prefix
		filteredHeaders := make(map[string]string)
		for key, value := range additionalHeaders {
			if strings.HasPrefix(key, "Firebolt-") {
				filteredHeaders[key] = value
			}
		}
		return filteredHeaders
	}
	return map[string]string{}
}

func resolveHttpClient(c *http.Client) *http.Client {
	if c != nil {
		return c
	}
	return http.DefaultClient
}

// DoHttpRequest sends a request using "POST" or "GET" method on a specified url
// additionally it passes the parameters and a bodyStr as a payload
// if accessToken is passed, it is used for authorization
// returns Response struct.
// httpClient may be nil, in which case http.DefaultClient is used.
func DoHttpRequest(httpClient *http.Client, reqParams requestParameters) *Response {
	canonicalUrl := MakeCanonicalUrl(reqParams.url)
	req, err := http.NewRequestWithContext(reqParams.ctx, reqParams.method, canonicalUrl, strings.NewReader(reqParams.bodyStr))
	if err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError(
			fmt.Sprintf("error creating HTTP request: method=%s, url=%s", reqParams.method, canonicalUrl), err))
	}

	if reqParams.hostOverride != "" {
		req.Host = reqParams.hostOverride
	}

	req.Header.Set("User-Agent", reqParams.userAgent)
	req.Header.Set(protocolVersionHeader, protocolVersion)

	if len(reqParams.accessToken) > 0 {
		var bearer = "Bearer " + reqParams.accessToken
		req.Header.Add("Authorization", bearer)
	}

	if len(reqParams.contentType) > 0 {
		req.Header.Set("Content-Type", reqParams.contentType)
	}

	for key, value := range extractAdditionalHeaders(reqParams.ctx) {
		req.Header.Set(key, value)
	}

	q := req.URL.Query()
	for key, value := range reqParams.params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := resolveHttpClient(httpClient).Do(req)
	if err != nil {
		logging.Infolog.Println(err)
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error during a request execution", err))
	}

	return MakeResponse(resp.Body, resp.StatusCode, resp.Header, nil)
}

// DoHttpRequestMultipart sends a multipart form POST with an "sql" text field
// and a file attachment containing the Parquet data.
// httpClient may be nil, in which case http.DefaultClient is used.
func DoHttpRequestMultipart(httpClient *http.Client, reqParams requestParametersMultipart) *Response {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)

	if err := mw.WriteField("sql", reqParams.sql); err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error writing sql form field", err))
	}

	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition",
		fmt.Sprintf(`form-data; name="%s"; filename="%s.parquet"`, reqParams.fileName, reqParams.fileName))
	partHeader.Set("Content-Type", "application/octet-stream")

	part, err := mw.CreatePart(partHeader)
	if err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error creating file form part", err))
	}
	if _, err := part.Write(reqParams.fileData); err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error writing file form data", err))
	}

	if err := mw.Close(); err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error closing multipart writer", err))
	}

	canonicalUrl := MakeCanonicalUrl(reqParams.url)
	req, err := http.NewRequestWithContext(reqParams.ctx, "POST", canonicalUrl, &body)
	if err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError(
			fmt.Sprintf("error creating HTTP request: url=%s", canonicalUrl), err))
	}

	if reqParams.hostOverride != "" {
		req.Host = reqParams.hostOverride
	}

	req.Header.Set("User-Agent", reqParams.userAgent)
	req.Header.Set(protocolVersionHeader, protocolVersion)
	req.Header.Set("Content-Type", mw.FormDataContentType())

	if len(reqParams.accessToken) > 0 {
		req.Header.Add("Authorization", "Bearer "+reqParams.accessToken)
	}

	for key, value := range extractAdditionalHeaders(reqParams.ctx) {
		req.Header.Set(key, value)
	}

	q := req.URL.Query()
	for key, value := range reqParams.params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := resolveHttpClient(httpClient).Do(req)
	if err != nil {
		logging.Infolog.Println(err)
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error during a request execution", err))
	}

	return MakeResponse(resp.Body, resp.StatusCode, resp.Header, nil)
}

// MakeCanonicalUrl checks whether url starts with https:// and if not prepends it
func MakeCanonicalUrl(url string) string {
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return url
	} else {
		return fmt.Sprintf("https://%s", url)
	}
}
