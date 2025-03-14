package client

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/firebolt-db/firebolt-go-sdk/utils"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

const jsonOutputFormat = "JSON_Compact"
const jsonLinesOutputFormat = "JSONLines_Compact"
const protocolVersionHeader = "Firebolt-Protocol-Version"
const protocolVersion = "2.1"

const updateParametersHeader = "Firebolt-Update-Parameters"
const updateEndpointHeader = "Firebolt-Update-Endpoint"
const resetSessionHeader = "Firebolt-Reset-Session"

var allowedUpdateParameters = []string{"database"}

type Client interface {
	GetConnectionParameters(ctx context.Context, engineName string, databaseName string) (string, map[string]string, error)
	Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control ConnectionControl) (*Response, error)
}

type BaseClient struct {
	ClientID          string
	ClientSecret      string
	ApiEndpoint       string
	UserAgent         string
	ParameterGetter   func(context.Context, map[string]string) (map[string]string, error)
	AccessTokenGetter func() (string, error)
}

// ConnectionControl is a struct that holds methods for updating connection properties
// it's passed to Query method to allow it to update connection parameters and engine URL
type ConnectionControl struct {
	UpdateParameters func(string, string)
	ResetParameters  func()
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
		if utils.ContainsString(allowedUpdateParameters, kv[0]) {
			updateParameters(kv[0], kv[1])
		} else {
			logging.Infolog.Printf("Warning: received unknown update parameter %s", kv[0])
		}
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
		control.ResetParameters()
	}

	return nil
}

// requestWithAuthRetry fetches an access token from the cache or re-authenticate when the access token is not available in the cache
// and sends a request using that token
func (c *BaseClient) requestWithAuthRetry(ctx context.Context, method string, url string, params map[string]string, bodyStr string) *Response {
	var err error

	if c.AccessTokenGetter == nil {
		return MakeResponse(nil, 0, nil, errors.New("AccessTokenGetter is not set"))
	}

	accessToken, err := c.AccessTokenGetter()
	if err != nil {
		return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error while getting access token", err))
	}
	resp := DoHttpRequest(requestParameters{ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON})
	if resp.statusCode == http.StatusUnauthorized {
		deleteAccessTokenFromCache(c.ClientID, c.ApiEndpoint)

		// Refreshing the access token as it is expired
		accessToken, err = c.AccessTokenGetter()
		if err != nil {
			return MakeResponse(nil, 0, nil, errorUtils.ConstructNestedError("error while getting access token", err))
		}
		// Trying to send the same request again now that the access token has been refreshed
		resp = DoHttpRequest(requestParameters{ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON})
	}
	return resp
}
