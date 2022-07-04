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
	err := json.Unmarshal(response, &errorResponse)
	if err == nil && errorResponse.Code != 0 {
		return errors.New(errorResponse.Message)
	}
	return err
}

func (c *Client) Request(method string, url string, params map[string]string, bodyStr string) ([]byte, error) {
	req, _ := http.NewRequest(method, url, strings.NewReader(bodyStr))

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
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if err = checkErrorResponse(body); err != nil {
		return body, err
	}

	return body, nil
}

func (c *Client) GetAccountIdByName(accountName string) (string, error) {
	type AccountIdByNameResponse struct {
		AccountId string `json:"account_id"`
	}

	params := make(map[string]string)
	params["account_name"] = accountName

	response, err := c.Request("GET", AccountIdByNameURL, params, "")
	if err != nil {
		return "", err
	}

	var accountIdByNameResponse AccountIdByNameResponse
	if err = json.Unmarshal(response, &accountIdByNameResponse); err != nil {
		return "", err
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

	response, err := c.Request("GET", fmt.Sprintf(EngineIdByNameURL, accountId), params, "")
	if err != nil {
		return "", err
	}

	var engineIdByNameResponse EngineIdByNameResponse
	if err = json.Unmarshal(response, &engineIdByNameResponse); err != nil {
		return "", err
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
	response, err := c.Request("GET", fmt.Sprintf(EngineByIdURL, accountId, engineId), params, "")
	if err != nil {
		return "", err
	}

	var engineByIdResponse EngineByIdResponse
	if err = json.Unmarshal(response, &engineByIdResponse); err != nil {
		return "", err
	}
	return engineByIdResponse.Engine.Endpoint, nil
}

func (c *Client) GetEngineUrlByName(engineName string, accountName string) (string, error) {
	accountId, err := c.GetAccountIdByName(accountName)
	if err != nil {
		return "", err
	}

	engineId, err := c.GetEngineIdByName(engineName, accountId)
	if err != nil {
		return "", err
	}

	engineUrl, err := c.GetEngineUrlById(engineId, accountId)
	if err != nil {
		return "", err
	}

	return engineUrl, nil
}
