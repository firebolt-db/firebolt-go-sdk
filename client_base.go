package fireboltgosdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"slices"
	"strings"
)

const outputFormat = "JSON_Compact"
const protocolVersionHeader = "Firebolt-Protocol-Version"
const protocolVersion = "2.0"

const updateParametersHeader = "Firebolt-Update-Parameters"

var allowedUpdateParameters = []string{"database"}

type Client interface {
	GetEngineUrlAndDB(ctx context.Context, engineName string, accountId string) (string, string, error)
	Query(ctx context.Context, engineUrl, query string, parameters map[string]string) (*QueryResponse, error)
}

type BaseClient struct {
	ClientID          string
	ClientSecret      string
	AccountID         string
	ApiEndpoint       string
	UserAgent         string
	parameterGetter   func(map[string]string) (map[string]string, error)
	accessTokenGetter func() (string, error)
}

// Query sends a query to the engine URL and populates queryResponse, if query was successful
func (c *BaseClient) Query(ctx context.Context, engineUrl, query string, parameters map[string]string) (*QueryResponse, error) {
	infolog.Printf("Query engine '%s' with '%s'", engineUrl, query)

	if c.parameterGetter == nil {
		return nil, errors.New("parameterGetter is not set")
	}
	params, err := c.parameterGetter(parameters)
	if err != nil {
		return nil, err
	}

	response, headers, _, err := c.request(ctx, "POST", engineUrl, params, query)
	if err != nil {
		return nil, ConstructNestedError("error during query request", err)
	}

	if err = processResponseHeaders(headers, parameters); err != nil {
		return nil, ConstructNestedError("error during processing response headers", err)
	}

	var queryResponse QueryResponse
	if len(response) == 0 {
		// response could be empty, which doesn't mean it is an error
		return &queryResponse, nil
	}

	if err = json.Unmarshal(response, &queryResponse); err != nil {
		return nil, ConstructNestedError("wrong response", errors.New(string(response)))
	}

	infolog.Printf("Query was successful")
	return &queryResponse, nil
}

func processResponseHeaders(headers http.Header, parameters map[string]string) error {
	if updateParametersRaw, ok := headers[updateParametersHeader]; ok {
		updateParametersPairs := strings.Split(updateParametersRaw[0], ",")
		for _, parameter := range updateParametersPairs {
			kv := strings.Split(parameter, "=")
			if len(kv) != 2 {
				return fmt.Errorf("invalid parameter assignment %s", parameter)
			}
			if slices.Contains(allowedUpdateParameters, kv[0]) {
				parameters[kv[0]] = kv[1]
			} else {
				infolog.Printf("Warning: received unknown update parameter %s", kv[0])
			}
		}
	}
	return nil
}

// request fetches an access token from the cache or re-authenticate when the access token is not available in the cache
// and sends a request using that token
func (c *BaseClient) request(ctx context.Context, method string, url string, params map[string]string, bodyStr string) ([]byte, http.Header, int, error) {
	var err error

	if c.accessTokenGetter == nil {
		return nil, nil, 0, errors.New("accessTokenGetter is not set")
	}

	accessToken, err := c.accessTokenGetter()
	if err != nil {
		return nil, nil, 0, ConstructNestedError("error while getting access token", err)
	}
	var response []byte
	var responseCode int
	var headers http.Header
	response, err, responseCode, headers = request(ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON)
	if responseCode == http.StatusUnauthorized {
		deleteAccessTokenFromCache(c.ClientID, c.ApiEndpoint)

		// Refreshing the access token as it is expired
		accessToken, err = c.accessTokenGetter()
		if err != nil {
			return nil, nil, 0, ConstructNestedError("error while refreshing access token", err)
		}
		// Trying to send the same request again now that the access token has been refreshed
		response, err, responseCode, headers = request(ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON)
	}
	return response, headers, responseCode, err
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

// request sends a request using "POST" or "GET" method on a specified url
// additionally it passes the parameters and a bodyStr as a payload
// if accessToken is passed, it is used for authorization
// returns response and an error
func request(
	ctx context.Context,
	accessToken string,
	method string,
	url string,
	userAgent string,
	params map[string]string,
	bodyStr string,
	contentType string) ([]byte, error, int, http.Header) {
	req, _ := http.NewRequestWithContext(ctx, method, makeCanonicalUrl(url), strings.NewReader(bodyStr))

	// adding sdk usage tracking
	req.Header.Set("User-Agent", userAgent)

	// add protocol version header
	req.Header.Set(protocolVersionHeader, protocolVersion)

	if len(accessToken) > 0 {
		var bearer = "Bearer " + accessToken
		req.Header.Add("Authorization", bearer)
	}

	if len(contentType) > 0 {
		req.Header.Set("Content-Type", contentType)
	}

	q := req.URL.Query()
	for key, value := range params {
		q.Add(key, value)
	}
	req.URL.RawQuery = q.Encode()

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		infolog.Println(err)
		return nil, ConstructNestedError("error during a request execution", err), 0, nil
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		infolog.Println(err)
		return nil, ConstructNestedError("error during reading a request response", err), 0, nil
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		if err = checkErrorResponse(body); err != nil {
			return nil, ConstructNestedError("request returned an error", err), resp.StatusCode, nil
		}
		if resp.StatusCode == 500 {
			// this is a database error
			return nil, fmt.Errorf("%s", string(body)), resp.StatusCode, nil
		}
		return nil, fmt.Errorf("request returned non ok status code: %d, %s", resp.StatusCode, string(body)), resp.StatusCode, nil
	}

	return body, nil, resp.StatusCode, resp.Header
}

// jsonStrictUnmarshall unmarshalls json into object, and returns an error
// if some fields are missing, or extra fields are present
func jsonStrictUnmarshall(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}
