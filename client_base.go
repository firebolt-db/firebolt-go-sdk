package fireboltgosdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	errors2 "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

const outputFormat = "JSON_Compact"
const protocolVersionHeader = "Firebolt-Protocol-Version"
const protocolVersion = "2.1"

const updateParametersHeader = "Firebolt-Update-Parameters"
const updateEndpointHeader = "Firebolt-Update-Endpoint"
const resetSessionHeader = "Firebolt-Reset-Session"

var allowedUpdateParameters = []string{"database"}

type Client interface {
	GetConnectionParameters(ctx context.Context, engineName string, databaseName string) (string, map[string]string, error)
	Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control connectionControl) (*types.QueryResponse, error)
}

type BaseClient struct {
	ClientID          string
	ClientSecret      string
	ApiEndpoint       string
	UserAgent         string
	parameterGetter   func(map[string]string) (map[string]string, error)
	accessTokenGetter func() (string, error)
}

type response struct {
	data       []byte
	statusCode int
	headers    http.Header
	err        error
}

// connectionControl is a struct that holds methods for updating connection properties
// it's passed to Query method to allow it to update connection parameters and engine URL
type connectionControl struct {
	updateParameters func(string, string)
	resetParameters  func()
	setEngineURL     func(string)
}

// Query sends a query to the engine URL and populates queryResponse, if query was successful
func (c *BaseClient) Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control connectionControl) (*types.QueryResponse, error) {
	logging.Infolog.Printf("Query engine '%s' with '%s'", engineUrl, query)

	if c.parameterGetter == nil {
		return nil, errors.New("parameterGetter is not set")
	}
	params, err := c.parameterGetter(parameters)
	if err != nil {
		return nil, err
	}

	resp := c.request(ctx, "POST", engineUrl, params, query)
	if resp.err != nil {
		return nil, errors2.ConstructNestedError("error during query request", resp.err)
	}

	if err = c.processResponseHeaders(resp.headers, control); err != nil {
		return nil, errors2.ConstructNestedError("error during processing response headers", err)
	}

	var queryResponse types.QueryResponse
	if len(resp.data) == 0 {
		// response could be empty, which doesn't mean it is an error
		return &queryResponse, nil
	}

	if err = json.Unmarshal(resp.data, &queryResponse); err != nil {
		return nil, errors2.ConstructNestedError("wrong response", errors.New(string(resp.data)))
	}

	logging.Infolog.Printf("Query was successful")
	return &queryResponse, nil
}

// check whether a string is present in a slice
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func handleUpdateParameters(updateParameters func(string, string), updateParametersRaw string) {
	updateParametersPairs := strings.Split(updateParametersRaw, ",")
	for _, parameter := range updateParametersPairs {
		kv := strings.Split(parameter, "=")
		if len(kv) != 2 {
			logging.Infolog.Printf("Warning: invalid parameter assignment %s", parameter)
			continue
		}
		if contains(allowedUpdateParameters, kv[0]) {
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

func (c *BaseClient) handleUpdateEndpoint(updateEndpointRaw string, control connectionControl) error {
	// split URL containted into updateEndpointRaw into endpoint and parameters
	// Update parameters and set client engine endpoint

	corruptUrlError := errors.New("failed to execute USE ENGINE command: corrupt update endpoint - contact support")
	updateEndpoint, newParameters, err := splitEngineEndpoint(updateEndpointRaw)
	if err != nil {
		return corruptUrlError
	}
	// set engine URL as a full URL excluding query parameters
	control.setEngineURL(updateEndpoint)
	// update client parameters with new parameters
	for k, v := range newParameters {
		control.updateParameters(k, v[0])
	}
	return nil
}

func (c *BaseClient) processResponseHeaders(headers http.Header, control connectionControl) error {
	if updateParametersRaw, ok := headers[updateParametersHeader]; ok {
		handleUpdateParameters(control.updateParameters, updateParametersRaw[0])
	}

	if updateEndpoint, ok := headers[updateEndpointHeader]; ok {
		if err := c.handleUpdateEndpoint(updateEndpoint[0], control); err != nil {
			return err
		}
	}
	if _, ok := headers[resetSessionHeader]; ok {
		control.resetParameters()
	}

	return nil
}

// request fetches an access token from the cache or re-authenticate when the access token is not available in the cache
// and sends a request using that token
func (c *BaseClient) request(ctx context.Context, method string, url string, params map[string]string, bodyStr string) response {
	var err error

	if c.accessTokenGetter == nil {
		return response{nil, 0, nil, errors.New("accessTokenGetter is not set")}
	}

	accessToken, err := c.accessTokenGetter()
	if err != nil {
		return response{nil, 0, nil, errors2.ConstructNestedError("error while getting access token", err)}
	}
	resp := request(requestParameters{ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON})
	if resp.statusCode == http.StatusUnauthorized {
		deleteAccessTokenFromCache(c.ClientID, c.ApiEndpoint)

		// Refreshing the access token as it is expired
		accessToken, err = c.accessTokenGetter()
		if err != nil {
			return response{nil, 0, nil, errors2.ConstructNestedError("error while getting access token", err)}
		}
		// Trying to send the same request again now that the access token has been refreshed
		resp = request(requestParameters{ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON})
	}
	return resp
}

// makeCanonicalUrl checks whether url starts with https:// and if not prepends it
func makeCanonicalUrl(url string) string {
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "http://") {
		return url
	} else {
		return fmt.Sprintf("https://%s", url)
	}
}

// checkErrorResponse, checks whether error response is returned instead of a desired response.
func checkErrorResponse(response []byte) error {
	// ErrorResponse definition of any response with some error
	type ErrorResponse struct {
		Error   string        `json:"error"`
		Code    int           `json:"code"`
		Message string        `json:"message"`
		Details []interface{} `json:"details"`
	}

	var errorResponse ErrorResponse

	if err := json.Unmarshal(response, &errorResponse); err == nil && errorResponse.Code != 0 {
		// return error only if error response was
		// unmarshalled correctly and error code is not zero
		return errors.New(errorResponse.Message)
	}
	return nil
}

// Collect arguments for request function
type requestParameters struct {
	ctx         context.Context
	accessToken string
	method      string
	url         string
	userAgent   string
	params      map[string]string
	bodyStr     string
	contentType string
}

type ContextKey string

func extractAdditionalHeaders(ctx context.Context) map[string]string {
	additionalHeaders, ok := ctx.Value(ContextKey("additionalHeaders")).(map[string]string)
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

// request sends a request using "POST" or "GET" method on a specified url
// additionally it passes the parameters and a bodyStr as a payload
// if accessToken is passed, it is used for authorization
// returns response and an error
func request(
	reqParams requestParameters) response {
	req, _ := http.NewRequestWithContext(reqParams.ctx, reqParams.method, makeCanonicalUrl(reqParams.url), strings.NewReader(reqParams.bodyStr))

	// adding sdk usage tracking
	req.Header.Set("User-Agent", reqParams.userAgent)

	// add protocol version header
	req.Header.Set(protocolVersionHeader, protocolVersion)

	if len(reqParams.accessToken) > 0 {
		var bearer = "Bearer " + reqParams.accessToken
		req.Header.Add("Authorization", bearer)
	}

	if len(reqParams.contentType) > 0 {
		req.Header.Set("Content-Type", reqParams.contentType)
	}

	// add additional headers from context
	for key, value := range extractAdditionalHeaders(reqParams.ctx) {
		req.Header.Set(key, value)
	}

	q := req.URL.Query()
	for key, value := range reqParams.params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		logging.Infolog.Println(err)
		return response{nil, 0, nil, errors2.ConstructNestedError("error during a request execution", err)}
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.Infolog.Println(err)
		return response{nil, 0, nil, errors2.ConstructNestedError("error during reading a request response", err)}
	}
	// Error might be in the response body, despite the status code 200
	errorResponse := struct {
		Errors []types.ErrorDetails `json:"errors"`
	}{}
	if err = json.Unmarshal(body, &errorResponse); err == nil {
		if errorResponse.Errors != nil {
			return response{nil, resp.StatusCode, nil, errors2.NewStructuredError(errorResponse.Errors)}
		}
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		if err = checkErrorResponse(body); err != nil {
			return response{nil, resp.StatusCode, nil, errors2.ConstructNestedError("request returned an error", err)}
		}
		if resp.StatusCode == 500 {
			// this is a database error
			return response{nil, resp.StatusCode, nil, fmt.Errorf("%s", string(body))}
		}
		return response{nil, resp.StatusCode, nil, fmt.Errorf("request returned non ok status code: %d, %s", resp.StatusCode, string(body))}
	}

	return response{body, resp.StatusCode, resp.Header, nil}
}

// jsonStrictUnmarshall unmarshalls json into object, and returns an error
// if some fields are missing, or extra fields are present
func jsonStrictUnmarshall(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}
