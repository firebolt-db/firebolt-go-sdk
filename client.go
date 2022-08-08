package fireboltgosdk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strings"
)

type Client struct {
	AccessToken string
}

// GetAccountIdByName returns account ID based on account name
func (c *Client) GetAccountIdByName(accountName string) (string, error) {
	log.Printf("get account id by name: %s", accountName)
	type AccountIdByNameResponse struct {
		AccountId string `json:"account_id"`
	}

	params := make(map[string]string)
	params["account_name"] = accountName

	response, err := request(c.AccessToken, "GET", GetHostNameURL()+AccountIdByNameURL, params, "")
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
func (c *Client) GetEngineIdByName(engineName string, accountId string) (string, error) {
	log.Printf("get engine id by name '%s' and account id '%s'", engineName, accountId)

	type EngineIdByNameInnerResponse struct {
		AccountId string `json:"account_id"`
		EngineId  string `json:"engine_id"`
	}
	type EngineIdByNameResponse struct {
		EngineId EngineIdByNameInnerResponse `json:"engine_id"`
	}

	params := make(map[string]string)
	params["engine_name"] = engineName

	response, err := request(c.AccessToken, "GET", fmt.Sprintf(GetHostNameURL()+EngineIdByNameURL, accountId), params, "")
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
func (c *Client) GetEngineUrlById(engineId string, accountId string) (string, error) {
	log.Printf("get engine url by id '%s' and account id '%s'", engineId, accountId)

	type EngineResponse struct {
		Endpoint string `json:"endpoint"`
	}
	type EngineByIdResponse struct {
		Engine EngineResponse `json:"engine"`
	}

	params := make(map[string]string)
	response, err := request(c.AccessToken, "GET", fmt.Sprintf(GetHostNameURL()+EngineByIdURL, accountId, engineId), params, "")
	if err != nil {
		return "", ConstructNestedError("error during getting engine url by id request", err)
	}

	var engineByIdResponse EngineByIdResponse
	if err = json.Unmarshal(response, &engineByIdResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling engine url by id response", errors.New(string(response)))
	}
	return fmt.Sprintf("https://%s", engineByIdResponse.Engine.Endpoint), nil
}

// GetDefaultAccount returns an id of the default account
func (c *Client) GetDefaultAccountId() (string, error) {
	type AccountResponse struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}
	type DefaultAccountResponse struct {
		Account AccountResponse `json:"account"`
	}

	params := make(map[string]string)
	response, err := request(c.AccessToken, "GET", fmt.Sprintf(GetHostNameURL()+DefaultAccountURL), params, "")

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
func (c *Client) GetEngineUrlByName(engineName string, accountId string) (string, error) {
	log.Printf("get engine url by name '%s' and account id '%s'", engineName, accountId)

	engineId, err := c.GetEngineIdByName(engineName, accountId)
	if err != nil {
		return "", ConstructNestedError("error during getting engine id by name", err)
	}

	engineUrl, err := c.GetEngineUrlById(engineId, accountId)
	if err != nil {
		return "", ConstructNestedError("error during getting engine url by id", err)
	}

	return engineUrl, nil
}

// GetEngineUrlByDatabase return URL of the default engine based on databaseName and accountName
func (c *Client) GetEngineUrlByDatabase(databaseName string, accountId string) (string, error) {
	log.Printf("get engine url by database name '%s' and account name '%s'", databaseName, accountId)

	type EngineUrlByDatabaseResponse struct {
		EngineUrl string `json:"engine_url"`
	}

	params := make(map[string]string)
	params["database_name"] = databaseName
	response, err := request(c.AccessToken, "GET", fmt.Sprintf(GetHostNameURL()+EngineUrlByDatabaseNameURL, accountId), params, "")
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
func (c *Client) Query(engineUrl, databaseName, query string, setStatements *map[string]string, queryResponse *QueryResponse) error {
	log.Printf("Query engine '%s' with '%s'", engineUrl, query)

	params := make(map[string]string)
	params["database"] = databaseName
	params["output_format"] = "FB_JSONCompactLimited"
	if setStatements != nil {
		for setKey, setValue := range *setStatements {
			params[setKey] = setValue
		}
	}

	response, err := request(c.AccessToken, "POST", engineUrl, params, query)
	if err != nil {
		return ConstructNestedError("error during query request", err)
	}

	if err = json.Unmarshal(response, &queryResponse); err != nil {
		return ConstructNestedError("wrong response", errors.New(string(response)))
	}

	log.Printf("Query was successful")
	return nil
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
func request(accessToken string, method string, url string, params map[string]string, bodyStr string) ([]byte, error) {
	req, _ := http.NewRequest(method, makeCanonicalUrl(url), strings.NewReader(bodyStr))

	// adding sdk usage tracking
	req.Header.Set("User-Agent", fmt.Sprintf("GoSDK/%s (Go %s; %s)", sdkVersion, runtime.Version(), runtime.GOOS))

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
		log.Println(err)
		return nil, ConstructNestedError("error during a request execution", err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, ConstructNestedError("error during reading a request response", err)
	}

	if err = checkErrorResponse(body); err != nil {
		return nil, ConstructNestedError("request returned an error", err)
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
