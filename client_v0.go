package fireboltgosdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	errors2 "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
)

type ClientImplV0 struct {
	AccountID string
	BaseClient
}

func MakeClientV0(settings *fireboltSettings, apiEndpoint string) (*ClientImplV0, error) {
	client := &ClientImplV0{
		BaseClient: BaseClient{
			ClientID:     settings.clientID,
			ClientSecret: settings.clientSecret,
			ApiEndpoint:  apiEndpoint,
			UserAgent:    ConstructUserAgentString(),
		},
	}
	client.parameterGetter = client.getQueryParams
	client.accessTokenGetter = client.getAccessToken

	var err error
	client.AccountID, err = client.getAccountID(context.Background(), settings.accountName)
	if err != nil {
		return nil, errors2.ConstructNestedError("error during getting account id", err)
	}
	return client, nil
}

// getAccountIDByName returns account ID based on account name
func (c *ClientImplV0) getAccountIDByName(ctx context.Context, accountName string) (string, error) {
	logging.Infolog.Printf("get account id by name: %s", accountName)

	type AccountIdByNameResponse struct {
		AccountId string `json:"account_id"`
	}

	params := map[string]string{"account_name": accountName}

	resp := c.request(ctx, "GET", c.ApiEndpoint+AccountIdByNameURL, params, "")
	if resp.err != nil {
		return "", errors2.ConstructNestedError("error during getting account id by name request", resp.err)
	}

	var accountIdByNameResponse AccountIdByNameResponse
	if err := json.Unmarshal(resp.data, &accountIdByNameResponse); err != nil {
		return "", errors2.ConstructNestedError("error during unmarshalling account id by name response", errors.New(string(resp.data)))
	}
	return accountIdByNameResponse.AccountId, nil
}

// getDefaultAccountID returns an id of the default account
func (c *ClientImplV0) getDefaultAccountID(ctx context.Context) (string, error) {
	type AccountResponse struct {
		Id   string `json:"id"`
		Name string `json:"name"`
	}
	type DefaultAccountResponse struct {
		Account AccountResponse `json:"account"`
	}

	resp := c.request(ctx, "GET", c.ApiEndpoint+DefaultAccountURL, make(map[string]string), "")
	if resp.err != nil {
		return "", errors2.ConstructNestedError("error during getting default account id request", resp.err)
	}

	var defaultAccountResponse DefaultAccountResponse
	if err := json.Unmarshal(resp.data, &defaultAccountResponse); err != nil {
		return "", errors2.ConstructNestedError("error during unmarshalling default account response", errors.New(string(resp.data)))
	}

	return defaultAccountResponse.Account.Id, nil
}

func (c *ClientImplV0) getAccountID(ctx context.Context, accountName string) (string, error) {
	var accountId string
	var err error
	if accountName == "" {
		logging.Infolog.Println("account name not specified, trying to get a default account id")
		accountId, err = c.getDefaultAccountID(ctx)
	} else {
		accountId, err = c.getAccountIDByName(ctx, accountName)
	}
	if err != nil {
		return "", errors2.ConstructNestedError("error during getting account id", err)
	}
	return accountId, nil
}

// getEngineIdByName returns engineId based on engineName and accountId
func (c *ClientImplV0) getEngineIdByName(ctx context.Context, engineName string, accountId string) (string, error) {
	logging.Infolog.Printf("get engine id by name '%s' and account id '%s'", engineName, accountId)

	type EngineIdByNameInnerResponse struct {
		AccountId string `json:"account_id"`
		EngineId  string `json:"engine_id"`
	}
	type EngineIdByNameResponse struct {
		EngineId EngineIdByNameInnerResponse `json:"engine_id"`
	}

	params := map[string]string{"engine_name": engineName}
	resp := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+EngineIdByNameURL, accountId), params, "")
	if resp.err != nil {
		return "", errors2.ConstructNestedError("error during getting engine id by name request", resp.err)
	}

	var engineIdByNameResponse EngineIdByNameResponse
	if err := json.Unmarshal(resp.data, &engineIdByNameResponse); err != nil {
		return "", errors2.ConstructNestedError("error during unmarshalling engine id by name response", errors.New(string(resp.data)))
	}
	return engineIdByNameResponse.EngineId.EngineId, nil
}

// getEngineUrlById returns engine url based on engineId and accountId
func (c *ClientImplV0) getEngineUrlById(ctx context.Context, engineId string, accountId string) (string, error) {
	logging.Infolog.Printf("get engine url by id '%s' and account id '%s'", engineId, accountId)

	type EngineResponse struct {
		Endpoint string `json:"endpoint"`
	}
	type EngineByIdResponse struct {
		Engine EngineResponse `json:"engine"`
	}

	resp := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+EngineByIdURL, accountId, engineId), make(map[string]string), "")

	if resp.err != nil {
		return "", errors2.ConstructNestedError("error during getting engine url by id request", resp.err)
	}

	var engineByIdResponse EngineByIdResponse
	if err := json.Unmarshal(resp.data, &engineByIdResponse); err != nil {
		return "", errors2.ConstructNestedError("error during unmarshalling engine url by id response", errors.New(string(resp.data)))
	}
	return makeCanonicalUrl(engineByIdResponse.Engine.Endpoint), nil
}

// getEngineUrlByName return engine URL based on engineName and accountName
func (c *ClientImplV0) getEngineUrlByName(ctx context.Context, engineName string, accountId string) (string, error) {
	logging.Infolog.Printf("get engine url by name '%s' and account id '%s'", engineName, accountId)

	engineId, err := c.getEngineIdByName(ctx, engineName, accountId)
	if err != nil {
		return "", errors2.ConstructNestedError("error during getting engine id by name", err)
	}

	engineUrl, err := c.getEngineUrlById(ctx, engineId, accountId)
	if err != nil {
		return "", errors2.ConstructNestedError("error during getting engine url by id", err)
	}

	return engineUrl, nil
}

// getEngineUrlByDatabase return URL of the default engine based on databaseName and accountName
func (c *ClientImplV0) getEngineUrlByDatabase(ctx context.Context, databaseName string, accountId string) (string, error) {
	logging.Infolog.Printf("get engine url by database name '%s' and account name '%s'", databaseName, accountId)

	type EngineUrlByDatabaseResponse struct {
		EngineUrl string `json:"engine_url"`
	}

	params := map[string]string{"database_name": databaseName}
	resp := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+EngineUrlByDatabaseNameURL, accountId), params, "")
	if resp.err != nil {
		return "", errors2.ConstructNestedError("error during getting engine url by database request", resp.err)
	}

	var engineUrlByDatabaseResponse EngineUrlByDatabaseResponse
	if err := json.Unmarshal(resp.data, &engineUrlByDatabaseResponse); err != nil {
		return "", errors2.ConstructNestedError("error during unmarshalling engine url by database response", errors.New(string(resp.data)))
	}
	return engineUrlByDatabaseResponse.EngineUrl, nil
}

// GetConnectionParameters returns engine URL and engine name based on engineName and accountId
func (c *ClientImplV0) GetConnectionParameters(ctx context.Context, engineName, databaseName string) (string, map[string]string, error) {
	// getting engineUrl either by using engineName if available,
	// if not using default engine for the database
	var engineUrl string
	var err error
	params := map[string]string{"database": databaseName}
	if engineName != "" {
		if strings.Contains(engineName, ".") {
			engineUrl, err = makeCanonicalUrl(engineName), nil
		} else {
			engineUrl, err = c.getEngineUrlByName(ctx, engineName, c.AccountID)
		}
	} else {
		logging.Infolog.Println("engine name not set, trying to get a default engine")
		engineUrl, err = c.getEngineUrlByDatabase(ctx, databaseName, c.AccountID)
	}
	if err != nil {
		return "", params, errors2.ConstructNestedError("error during getting engine url", err)
	}
	return engineUrl, params, nil

}

func (c *ClientImplV0) getQueryParams(setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": outputFormat}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	return params, nil
}

func (c *ClientImplV0) getAccessToken() (string, error) {
	return getAccessTokenUsernamePassword(c.ClientID, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
}
