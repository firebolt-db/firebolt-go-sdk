package fireboltgosdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type Client struct {
	AccessToken string
}

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
		// return error only if error response was unmarshalled correctly and error code is not zero
		return errors.New(errorResponse.Message)
	}
	return nil
}

func makeCanonicalUrl(url string) string {
	if strings.HasPrefix(url, "https://") {
		return url
	} else {
		return fmt.Sprintf("https://%s", url)
	}
}

func (c *Client) Request(method string, url string, params map[string]string, bodyStr string) ([]byte, error) {
	req, _ := http.NewRequest(method, makeCanonicalUrl(url), strings.NewReader(bodyStr))

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	if len(c.AccessToken) > 0 {
		var bearer = "Bearer " + c.AccessToken
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
		return nil, fmt.Errorf("error during a request execution: %v", err)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, fmt.Errorf("error during reading a request response: %v", err)
	}

	if err = checkErrorResponse(body); err != nil {
		return nil, fmt.Errorf("request returned an error: %v", err)
	}

	return body, nil
}

func (c *Client) GetAccountIdByName(accountName string) (string, error) {
	type AccountIdByNameResponse struct {
		AccountId string `json:"account_id"`
	}

	params := make(map[string]string)
	params["account_name"] = accountName

	response, err := c.Request("GET", HostNameURL+AccountIdByNameURL, params, "")
	if err != nil {
		return "", fmt.Errorf("error duting getting account id by name request: %v", err)
	}

	var accountIdByNameResponse AccountIdByNameResponse
	if err = json.Unmarshal(response, &accountIdByNameResponse); err != nil {
		return "", fmt.Errorf("error duting unmarshalling account id by name response: %v", err)
	}
	return accountIdByNameResponse.AccountId, nil
}

func (c *Client) GetEngineIdByName(engineName string, accountId string) (string, error) {
	type EngineIdByNameInnerResponse struct {
		AccountId string `json:"account_id"`
		EngineId  string `json:"engine_id"`
	}
	type EngineIdByNameResponse struct {
		EngineId EngineIdByNameInnerResponse `json:"engine_id"`
	}

	params := make(map[string]string)
	params["engine_name"] = engineName

	response, err := c.Request("GET", fmt.Sprintf(HostNameURL+EngineIdByNameURL, accountId), params, "")
	if err != nil {
		return "", fmt.Errorf("error duting getting engine id by name request: %v", err)
	}

	var engineIdByNameResponse EngineIdByNameResponse
	if err = json.Unmarshal(response, &engineIdByNameResponse); err != nil {
		return "", fmt.Errorf("error duting unmarshalling engine id by name response: %v", err)
	}
	return engineIdByNameResponse.EngineId.EngineId, nil
}

func (c *Client) GetEngineUrlById(engineId string, accountId string) (string, error) {
	type EngineResponse struct {
		Endpoint string `json:"endpoint"`
	}
	type EngineByIdResponse struct {
		Engine EngineResponse `json:"engine"`
	}

	params := make(map[string]string)
	response, err := c.Request("GET", fmt.Sprintf(HostNameURL+EngineByIdURL, accountId, engineId), params, "")
	if err != nil {
		return "", fmt.Errorf("error duting getting engine url by id request: %v", err)
	}

	var engineByIdResponse EngineByIdResponse
	if err = json.Unmarshal(response, &engineByIdResponse); err != nil {
		return "", fmt.Errorf("error duting unmarshalling engine url by id response: %v", err)
	}
	return fmt.Sprintf("https://%s", engineByIdResponse.Engine.Endpoint), nil
}

func (c *Client) GetEngineUrlByName(engineName string, accountName string) (string, error) {
	accountId, err := c.GetAccountIdByName(accountName)
	if err != nil {
		return "", fmt.Errorf("error duting getting account id by name: %v", err)
	}

	engineId, err := c.GetEngineIdByName(engineName, accountId)
	if err != nil {
		return "", fmt.Errorf("error duting getting engine id by name: %v", err)
	}

	engineUrl, err := c.GetEngineUrlById(engineId, accountId)
	if err != nil {
		return "", fmt.Errorf("error duting getting engine url by id: %v", err)
	}

	return engineUrl, nil
}

func (c *Client) GetEngineUrlByDatabase(databaseName string, accountName string) (string, error) {
	accountId, err := c.GetAccountIdByName(accountName)
	if err != nil {
		return "", err
	}

	type EngineUrlByDatabaseResponse struct {
		EngineUrl string `json:"engine_url"`
	}

	params := make(map[string]string)
	params["database_name"] = databaseName
	response, err := c.Request("GET", fmt.Sprintf(HostNameURL+EngineUrlByDatabaseNameURL, accountId), params, "")
	if err != nil {
		return "", fmt.Errorf("error duting getting engine url by database request: %v", err)
	}

	var engineUrlByDatabaseResponse EngineUrlByDatabaseResponse
	if err = json.Unmarshal(response, &engineUrlByDatabaseResponse); err != nil {
		return "", fmt.Errorf("error duting unmarshalling engine url by database response: %v", err)
	}
	return engineUrlByDatabaseResponse.EngineUrl, nil
}
