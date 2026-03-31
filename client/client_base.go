package client

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const jsonOutputFormat = "JSON_Compact"
const jsonLinesOutputFormat = "JSONLines_Compact"
const protocolVersionHeader = "Firebolt-Protocol-Version"
const protocolVersion = "2.4"

const updateParametersHeader = "Firebolt-Update-Parameters"
const updateEndpointHeader = "Firebolt-Update-Endpoint"
const resetSessionHeader = "Firebolt-Reset-Session"
const removeParametersHeader = "Firebolt-Remove-Parameters"

// BatchPayload can produce a fresh io.Reader over the serialised batch
// body. NewReader may be called more than once (e.g. on auth retry), and each
// call must return an independent reader starting from the beginning.
type BatchPayload interface {
	NewReader() (io.Reader, error)
}

type Client interface {
	GetConnectionParameters(ctx context.Context, engineName string, databaseName string) (string, map[string]string, error)
	Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control ConnectionControl) (*Response, error)
	UploadBatch(ctx context.Context, engineUrl, sql string, payload BatchPayload, fileName, fileExt string, parameters map[string]string, control ConnectionControl) (*Response, error)
}

type BaseClient struct {
	ClientID          string
	ClientSecret      string
	ApiEndpoint       string
	UserAgent         string
	HttpClient        *http.Client
	ParameterGetter   func(context.Context, map[string]string) (map[string]string, error)
	AccessTokenGetter func() (string, error)
	URLResolver       *RoundRobinResolver // nil disables client-side load balancing
}

// Close releases resources held by the client, including idle HTTP
// connections and any background health-check goroutine.
func (c *BaseClient) Close() error {
	if c.URLResolver != nil {
		c.URLResolver.Close()
	}
	if c.HttpClient != nil {
		c.HttpClient.CloseIdleConnections()
	}
	return nil
}

// ConnectionControl is a struct that holds methods for updating connection properties
// it's passed to Query method to allow it to update connection parameters and engine URL
type ConnectionControl struct {
	UpdateParameters func(string, string)
	ResetParameters  func(*[]string) // if list is nil, reset all parameters
	SetEngineURL     func(string)
}

// Query sends a query to the engine URL and populates queryResponse, if query was successful
func (c *BaseClient) Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control ConnectionControl) (*Response, error) {
	logging.Infolog.Printf("Query engine '%s' with '%s'", engineUrl, query)

	if c.ParameterGetter == nil {
		return nil, errors.New("ParameterGetter is not set")
	}
	params, err := c.ParameterGetter(ctx, parameters)
	if err != nil {
		return nil, err
	}

	resp := c.requestWithAuthRetry(ctx, "POST", engineUrl, params, query)
	if resp.err != nil {
		return nil, errorUtils.ConstructNestedError("error during query request", resp.err)
	}

	if err = c.processResponseHeaders(resp.headers, control); err != nil {
		return nil, errorUtils.ConstructNestedError("error during processing response headers", err)
	}
	return resp, nil
}

func handleUpdateParameters(updateParameters func(string, string), updateParametersRaw string) {
	updateParametersPairs := strings.Split(updateParametersRaw, ",")
	for _, parameter := range updateParametersPairs {
		kv := strings.Split(parameter, "=")
		if len(kv) != 2 {
			logging.Infolog.Printf("Warning: invalid parameter assignment %s", parameter)
			continue
		}
		updateParameters(kv[0], kv[1])
	}
}

func splitEngineEndpoint(endpoint string) (string, url.Values, error) {
	parsedUrl, err := url.Parse(endpoint)
	if err != nil {
		return "", nil, err
	}
	parameters, err := url.ParseQuery(parsedUrl.RawQuery)
	if err != nil {
		return "", nil, err
	}
	return parsedUrl.Host + parsedUrl.Path, parameters, nil
}

func (c *BaseClient) handleUpdateEndpoint(updateEndpointRaw string, control ConnectionControl) error {
	// split URL contained into updateEndpointRaw into endpoint and parameters
	// Update parameters and set client engine endpoint

	corruptUrlError := errors.New("failed to execute USE ENGINE command: corrupt update endpoint - contact support")
	updateEndpoint, newParameters, err := splitEngineEndpoint(updateEndpointRaw)
	if err != nil {
		return corruptUrlError
	}
	// set engine URL as a full URL excluding query parameters
	control.SetEngineURL(updateEndpoint)
	// update client parameters with new parameters
	for k, v := range newParameters {
		control.UpdateParameters(k, v[0])
	}
	return nil
}

func (c *BaseClient) processResponseHeaders(headers http.Header, control ConnectionControl) error {
	if updateParametersRaw, ok := headers[updateParametersHeader]; ok {
		handleUpdateParameters(control.UpdateParameters, updateParametersRaw[0])
	}

	if updateEndpoint, ok := headers[updateEndpointHeader]; ok {
		if err := c.handleUpdateEndpoint(updateEndpoint[0], control); err != nil {
			return err
		}
	}
	if _, ok := headers[resetSessionHeader]; ok {
		control.ResetParameters(nil)
	}
	if parameters, ok := headers[removeParametersHeader]; ok {
		control.ResetParameters(&parameters)
	}

	return nil
}

// UploadBatch sends a serialised file via multipart form upload for batch INSERT.
// The form has two parts:
//
//	"sql"       — the INSERT query
//	"<fileName>"— the file bytes (e.g. Parquet)
//
// fileExt is the extension including the dot (e.g. ".parquet") used in the
// Content-Disposition header.
func (c *BaseClient) UploadBatch(ctx context.Context, engineUrl, sql string, payload BatchPayload, fileName, fileExt string, parameters map[string]string, control ConnectionControl) (*Response, error) {
	logging.Infolog.Printf("UploadBatch to engine '%s' with query '%s'", engineUrl, sql)

	if c.ParameterGetter == nil {
		return nil, errors.New("ParameterGetter is not set")
	}
	params, err := c.ParameterGetter(ctx, parameters)
	if err != nil {
		return nil, err
	}

	resp := c.requestMultipartWithAuthRetry(ctx, engineUrl, params, sql, payload, fileName, fileExt)
	if resp.err != nil {
		return nil, errorUtils.ConstructNestedError("error during batch upload request", resp.err)
	}

	if err = c.processResponseHeaders(resp.headers, control); err != nil {
		return nil, errorUtils.ConstructNestedError("error during processing response headers", err)
	}
	return resp, nil
}

// resolveURL returns the (possibly rewritten) URL and host override for the
// next request. When no URLResolver is configured, the URL is returned as-is.
// If the engine URL has been changed at runtime (e.g. via a
// Firebolt-Update-Endpoint response header), the resolver is bypassed
// because it is configured for the original hostname only.
func (c *BaseClient) resolveURL(ctx context.Context, rawURL string) (string, string) {
	if c.URLResolver == nil {
		return rawURL, ""
	}
	if MakeCanonicalUrl(rawURL) != c.URLResolver.originalURL.String() {
		return rawURL, ""
	}
	resolved, originalHost, err := c.URLResolver.Next(ctx)
	if err != nil {
		logging.Infolog.Printf("client-side LB resolution failed, using original URL: %v", err)
		return rawURL, ""
	}
	return resolved, originalHost
}

func (c *BaseClient) requestMultipartWithAuthRetry(ctx context.Context, rawURL string, params map[string]string, sql string, payload BatchPayload, fileName, fileExt string) *Response {
	if c.AccessTokenGetter == nil {
		return MakeResponse(nil, 0, nil, errors.New("AccessTokenGetter is not set"))
	}

	maxDialRetries := c.maxDialRetries(rawURL)

	for attempt := 0; ; attempt++ {
		resolvedURL, hostOverride := c.resolveURL(ctx, rawURL)

		accessToken, err := c.AccessTokenGetter()
		if err != nil {
			return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error while getting access token", err))
		}
		reader, err := payload.NewReader()
		if err != nil {
			return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error creating batch reader", err))
		}
		resp := DoHttpRequestMultipart(c.HttpClient, requestParametersMultipart{ctx, accessToken, resolvedURL, c.UserAgent, params, sql, reader, fileName, fileExt, hostOverride})

		if resp.err != nil && isDialError(resp.err) && attempt < maxDialRetries {
			c.reportDialFailure(resolvedURL)
			continue
		}

		if resp.statusCode == http.StatusUnauthorized {
			deleteAccessTokenFromCache(c.ClientID, c.ApiEndpoint)

			accessToken, err = c.AccessTokenGetter()
			if err != nil {
				return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error while getting access token", err))
			}
			reader, err = payload.NewReader()
			if err != nil {
				return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error creating batch reader for retry", err))
			}
			resp = DoHttpRequestMultipart(c.HttpClient, requestParametersMultipart{ctx, accessToken, resolvedURL, c.UserAgent, params, sql, reader, fileName, fileExt, hostOverride})

			if resp.err != nil && isDialError(resp.err) && attempt < maxDialRetries {
				c.reportDialFailure(resolvedURL)
				continue
			}

			if resp.statusCode == http.StatusUnauthorized {
				resp.err = errorUtils.Wrap(errorUtils.AuthorizationError, resp.err)
			}
		}
		return resp
	}
}

// requestWithAuthRetry fetches an access token from the cache or re-authenticate when the access token is not available in the cache
// and sends a request using that token. When health checking is enabled
// and the TCP dial fails, the failing IP is marked unhealthy and the
// request is retried against the next healthy IP.
func (c *BaseClient) requestWithAuthRetry(ctx context.Context, method string, rawURL string, params map[string]string, bodyStr string) *Response {
	if c.AccessTokenGetter == nil {
		return MakeResponse(nil, 0, nil, errors.New("AccessTokenGetter is not set"))
	}

	maxDialRetries := c.maxDialRetries(rawURL)

	for attempt := 0; ; attempt++ {
		resolvedURL, hostOverride := c.resolveURL(ctx, rawURL)

		accessToken, err := c.AccessTokenGetter()
		if err != nil {
			return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error while getting access token", err))
		}
		resp := DoHttpRequest(c.HttpClient, requestParameters{ctx, accessToken, method, resolvedURL, c.UserAgent, params, bodyStr, ContentTypeJSON, hostOverride})

		if resp.err != nil && isDialError(resp.err) && attempt < maxDialRetries {
			c.reportDialFailure(resolvedURL)
			continue
		}

		if resp.statusCode == http.StatusUnauthorized {
			deleteAccessTokenFromCache(c.ClientID, c.ApiEndpoint)

			accessToken, err = c.AccessTokenGetter()
			if err != nil {
				return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error while getting access token", err))
			}
			resp = DoHttpRequest(c.HttpClient, requestParameters{ctx, accessToken, method, resolvedURL, c.UserAgent, params, bodyStr, ContentTypeJSON, hostOverride})

			if resp.err != nil && isDialError(resp.err) && attempt < maxDialRetries {
				c.reportDialFailure(resolvedURL)
				continue
			}

			if resp.statusCode == http.StatusUnauthorized {
				resp.err = errorUtils.Wrap(errorUtils.AuthorizationError, resp.err)
			}
		}
		return resp
	}
}

// maxDialRetries returns the number of additional IPs to try when a
// TCP dial fails. Returns 0 when health checking is disabled or when
// the URL doesn't match the resolver's target.
func (c *BaseClient) maxDialRetries(rawURL string) int {
	if c.URLResolver == nil || c.URLResolver.healthChecker == nil {
		return 0
	}
	if MakeCanonicalUrl(rawURL) != c.URLResolver.originalURL.String() {
		return 0
	}
	count := c.URLResolver.HealthyIPCount()
	if count <= 1 {
		return 0
	}
	return count - 1
}

func (c *BaseClient) reportDialFailure(resolvedURL string) {
	if c.URLResolver == nil {
		return
	}
	ip := extractIPFromURL(resolvedURL)
	if ip != "" {
		c.URLResolver.ReportDialFailure(ip)
	}
}

// isDialError reports whether err (or any error in its chain) is a TCP
// dial failure. Only dial-level failures trigger retry; HTTP-level
// errors (timeouts, 5xx, etc.) do not.
func isDialError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return opErr.Op == "dial"
	}
	return false
}

func extractIPFromURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}
