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
	AccessToken string
	ApiEndpoint string
	UserAgent   string
}

// GetAccountIdByName returns account ID based on account name
func (c *Client) GetAccountIdByName(ctx context.Context, accountName string) (string, error) {
	infolog.Printf("get account id by name: %s", accountName)
	type AccountIdByNameResponse struct {
		AccountId string `json:"account_id"`
	}

	params := map[string]string{"account_name": accountName}

	response, err := request(ctx, c.AccessToken, "GET", c.ApiEndpoint+AccountIdByNameURL, c.UserAgent, params, "")
	if err != nil {
		return "", ConstructNestedError("error during getting account id by name request", err)
	}

	var accountIdByNameResponse AccountIdByNameResponse
	if err = json.Unmarshal(response, &accountIdByNameResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling account id by name response", errors.New(string(response)))
	}
	return accountIdByNameResponse.AccountId, nil
}

// GetEngineIdByName returns engineId based on engineName and accountId
func (c *Client) GetEngineIdByName(ctx context.Context, engineName string, accountId string) (string, error) {
	infolog.Printf("get engine id by name '%s' and account id '%s'", engineName, accountId)

	type EngineIdByNameInnerResponse struct {
		AccountId string `json:"account_id"`
		EngineId  string `json:"engine_id"`
	}
	type EngineIdByNameResponse struct {
		EngineId EngineIdByNameInnerResponse `json:"engine_id"`
	}

	params := map[string]string{"engine_name": engineName}
	response, err := request(ctx, c.AccessToken, "GET", fmt.Sprintf(c.ApiEndpoint+EngineIdByNameURL, accountId), c.UserAgent, params, "")
	if err != nil {
		return "", ConstructNestedError("error during getting engine id by name request", err)
	}

	var engineIdByNameResponse EngineIdByNameResponse
	if err = json.Unmarshal(response, &engineIdByNameResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling engine id by name response", errors.New(string(response)))
	}
	return engineIdByNameResponse.EngineId.EngineId, nil
}

// GetEngineUrlById returns engine url based on engineId and accountId
func (c *Client) GetEngineUrlById(ctx context.Context, engineId string, accountId string) (string, error) {
	infolog.Printf("get engine url by id '%s' and account id '%s'", engineId, accountId)

	type EngineResponse struct {
		Endpoint string `json:"endpoint"`
	}
	type EngineByIdResponse struct {
		Engine EngineResponse `json:"engine"`
	}

	response, err := request(ctx, c.AccessToken, "GET", fmt.Sprintf(c.ApiEndpoint+EngineByIdURL, accountId, engineId), c.UserAgent, make(map[string]string), "")

	if err != nil {
		return "", ConstructNestedError("error during getting engine url by id request", err)
	}

	var engineByIdResponse EngineByIdResponse
	if err = json.Unmarshal(response, &engineByIdResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling engine url by id response", errors.New(string(response)))
	}
	return makeCanonicalUrl(engineByIdResponse.Engine.Endpoint), nil
}

// GetDefaultAccount returns an id of the default account
func (c *Client) GetDefaultAccountId(ctx context.Context) (string, error) {
	type AccountResponse struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}
	type DefaultAccountResponse struct {
		Account AccountResponse `json:"account"`
	}

	response, err := request(ctx, c.AccessToken, "GET", fmt.Sprintf(c.ApiEndpoint+DefaultAccountURL), c.UserAgent, make(map[string]string), "")
	if err != nil {
		return "", ConstructNestedError("error during getting default account id request", err)
	}

	var defaultAccountResponse DefaultAccountResponse
	if err = json.Unmarshal(response, &defaultAccountResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling default account response", errors.New(string(response)))
	}

	return defaultAccountResponse.Account.Id, nil
}

// GetEngineUrlByName return engine URL based on engineName and accountName
func (c *Client) GetEngineUrlByName(ctx context.Context, engineName string, accountId string) (string, error) {
	infolog.Printf("get engine url by name '%s' and account id '%s'", engineName, accountId)

	engineId, err := c.GetEngineIdByName(ctx, engineName, accountId)
	if err != nil {
		return "", ConstructNestedError("error during getting engine id by name", err)
	}

	engineUrl, err := c.GetEngineUrlById(ctx, engineId, accountId)
	if err != nil {
		return "", ConstructNestedError("error during getting engine url by id", err)
	}

	return engineUrl, nil
}

// GetEngineUrlByDatabase return URL of the default engine based on databaseName and accountName
func (c *Client) GetEngineUrlByDatabase(ctx context.Context, databaseName string, accountId string) (string, error) {
	infolog.Printf("get engine url by database name '%s' and account name '%s'", databaseName, accountId)

	type EngineUrlByDatabaseResponse struct {
		EngineUrl string `json:"engine_url"`
	}

	params := map[string]string{"database_name": databaseName}
	response, err := request(ctx, c.AccessToken, "GET", fmt.Sprintf(c.ApiEndpoint+EngineUrlByDatabaseNameURL, accountId), c.UserAgent, params, "")
	if err != nil {
		return "", ConstructNestedError("error during getting engine url by database request", err)
	}

	var engineUrlByDatabaseResponse EngineUrlByDatabaseResponse
	if err = json.Unmarshal(response, &engineUrlByDatabaseResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling engine url by database response", errors.New(string(response)))
	}
	return engineUrlByDatabaseResponse.EngineUrl, nil
}

// Query sends a query to the engine URL and populates queryResponse, if query was successful
func (c *Client) Query(ctx context.Context, engineUrl, databaseName, query string, setStatements map[string]string) (*QueryResponse, error) {
	infolog.Printf("Query engine '%s' with '%s'", engineUrl, query)

	params := map[string]string{"database": databaseName, "output_format": "JSONCompact"}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}

	response, err := request(ctx, c.AccessToken, "POST", engineUrl, c.UserAgent, params, query)
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
	if strings.HasPrefix(url, "https://") {
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
func request(ctx context.Context, accessToken string, method string, url string, userAgent string, params map[string]string, bodyStr string) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, method, makeCanonicalUrl(url), strings.NewReader(bodyStr))

	// adding sdk usage tracking
	req.Header.Set("User-Agent", userAgent)

	if len(accessToken) > 0 {
		var bearer = "Bearer " + accessToken
		req.Header.Add("Authorization", bearer)
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
		return nil, ConstructNestedError("error during a request execution", err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		infolog.Println(err)
		return nil, ConstructNestedError("error during reading a request response", err)
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode < 300) {
		if err = checkErrorResponse(body); err != nil {
			return nil, ConstructNestedError("request returned an error", err)
		}
		if resp.StatusCode == 500 {
			// this is a database error
			return nil, fmt.Errorf("%s", string(body))
		}
		return nil, fmt.Errorf("request returned non ok status code: %d", resp.StatusCode)
	}

	return body, nil
}

// jsonStrictUnmarshall unmarshalls json into object, and returns an error
// if some fields are missing, or extra fields are present
func jsonStrictUnmarshall(data []byte, v interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(v)
}
