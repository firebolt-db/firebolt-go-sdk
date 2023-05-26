package fireboltgosdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

type Client struct {
	ClientId     string
	ClientSecret string
	ApiEndpoint  string
	UserAgent    string
	AccountId    string
}

const outputFormat = "JSON_Compact"

const engineInfoSQL = `
SELECT url, status, attached_to FROM information_schema.engines
WHERE engine_name='%s'
`

func (c *Client) GetEngineUrlStatusDBByName(ctx context.Context, engineName string, systemEngineUrl string) (string, string, string, error) {
	infolog.Printf("Get info for engine '%s'", engineName)
	engineSQL := fmt.Sprintf(engineInfoSQL, engineName)
	queryRes, err := c.Query(ctx, systemEngineUrl+QueryUrl, "", engineSQL, make(map[string]string))
	if err != nil {
		return "", "", "", ConstructNestedError("error executing engine info sql query", err)
	}

	if len(queryRes.Data) == 0 {
		return "", "", "", fmt.Errorf("engine with name %s doesn't exist", engineName)
	}

	engineUrl, status, dbName, err := parseEngineInfoResponse(queryRes.Data)
	if err != nil {
		return "", "", "", ConstructNestedError("error parsing server response for engine info SQL query", err)
	}
	return engineUrl, status, dbName, nil
}

func parseEngineInfoResponse(resp [][]interface{}) (string, string, string, error) {
	if len(resp) != 1 || len(resp[0]) != 3 {
		return "", "", "", fmt.Errorf("invalid response shape: %v", resp)
	}
	engineUrl, ok := resp[0][0].(string)
	if !ok {
		return "", "", "", fmt.Errorf("expected string for engine URL, got %v", resp[0][0])
	}
	status, ok := resp[0][1].(string)
	if !ok {
		return "", "", "", fmt.Errorf("expected string for engine status, got %v", resp[0][1])
	}
	dbName, ok := resp[0][2].(string)
	// NULL is also acceptable for database name
	if !ok && resp[0][2] != nil {
		return "", "", "", fmt.Errorf("expected string for engine status, got %v", resp[0][1])
	}
	return engineUrl, status, dbName, nil
}

func (c *Client) GetSystemEngineURL(ctx context.Context, accountName string) (string, error) {
	infolog.Printf("Get system engine URL for account '%s'", accountName)

	type SystemEngineURLResponse struct {
		EngineUrl string `json:"engineUrl"`
	}

	url := fmt.Sprintf(c.ApiEndpoint+GatewayHostByAccountName, accountName)

	response, err := c.request(ctx, "GET", url, make(map[string]string), "")
	if err != nil {
		return "", ConstructNestedError("error during system engine url http request", err)
	}

	var systemEngineURLResponse SystemEngineURLResponse
	if err = json.Unmarshal(response, &systemEngineURLResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling system engine URL response", errors.New(string(response)))
	}

	return systemEngineURLResponse.EngineUrl, nil
}

// Query sends a query to the engine URL and populates queryResponse, if query was successful
func (c *Client) Query(ctx context.Context, engineUrl, databaseName, query string, setStatements map[string]string) (*QueryResponse, error) {
	infolog.Printf("Query engine '%s' with '%s'", engineUrl, query)

	params := map[string]string{"output_format": outputFormat}
	if len(databaseName) > 0 {
		params["database"] = databaseName
	}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	params["account_id"] = c.AccountId
	response, err := c.request(ctx, "POST", engineUrl, params, query)
	if err != nil {
		return nil, ConstructNestedError("error during query request", err)
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

// request fetches an access token from the cache or re-authenticate when the access token is not available in the cache
// and sends a request using that token
func (c Client) request(ctx context.Context, method string, url string, params map[string]string, bodyStr string) ([]byte, error) {
	var err error
	accessToken, err := getAccessToken(c.ClientId, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
	if err != nil {
		return nil, ConstructNestedError("error while getting access token", err)
	}
	var response []byte
	var responseCode int
	response, err, responseCode = request(ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON)
	if responseCode == http.StatusUnauthorized {
		deleteAccessTokenFromCache(c.ClientId, c.ApiEndpoint)

		// Refreshing the access token as it is expired
		accessToken, err = getAccessToken(c.ClientId, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
		if err != nil {
			return nil, ConstructNestedError("error while refreshing access token", err)
		}
		// Trying to send the same request again now that the access token has been refreshed
		response, err, _ = request(ctx, accessToken, method, url, c.UserAgent, params, bodyStr, ContentTypeJSON)
	}
	return response, err
}

// request sends a request using "POST" or "GET" method on a specified url
// additionally it passes the parameters and a bodyStr as a payload
// if accessToken is passed, it is used for authorization
// returns response and an error
func request(ctx context.Context, accessToken string, method string, url string, userAgent string, params map[string]string, bodyStr string, contentType string) ([]byte, error, int) {
	req, _ := http.NewRequestWithContext(ctx, method, makeCanonicalUrl(url), strings.NewReader(bodyStr))

	// adding sdk usage tracking
	req.Header.Set("User-Agent", userAgent)

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
		return nil, ConstructNestedError("error during a request execution", err), 0
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		infolog.Println(err)
		return nil, ConstructNestedError("error during reading a request response", err), 0
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		if err = checkErrorResponse(body); err != nil {
			return nil, ConstructNestedError("request returned an error", err), resp.StatusCode
		}
		if resp.StatusCode == 500 {
			// this is a database error
			return nil, fmt.Errorf("%s", string(body)), resp.StatusCode
		}
		return nil, fmt.Errorf("request returned non ok status code: %d, %s", resp.StatusCode, string(body)), resp.StatusCode
	}

	return body, nil, resp.StatusCode
}

// jsonStrictUnmarshall unmarshalls json into object, and returns an error
// if some fields are missing, or extra fields are present
func jsonStrictUnmarshall(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}
