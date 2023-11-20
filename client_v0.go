package fireboltgosdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type ClientImplV0 struct {
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

	var err error
	client.AccountID, err = client.getAccountID(context.Background(), settings.accountName)
	if err != nil {
		return nil, ConstructNestedError("error during getting account id", err)
	}
	return client, nil
}

// getAccountIDByName returns account ID based on account name
func (c *ClientImplV0) getAccountIDByName(ctx context.Context, accountName string) (string, error) {
	infolog.Printf("get account id by name: %s", accountName)

	type AccountIdByNameResponse struct {
		AccountId string `json:"account_id"`
	}

	params := map[string]string{"account_name": accountName}

	response, err := c.request(ctx, "GET", c.ApiEndpoint+AccountIdByNameURL, params, "")
	if err != nil {
		return "", ConstructNestedError("error during getting account id by name request", err)
	}

	var accountIdByNameResponse AccountIdByNameResponse
	if err = json.Unmarshal(response, &accountIdByNameResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling account id by name response", errors.New(string(response)))
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

	response, err := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+DefaultAccountURL), make(map[string]string), "")
	if err != nil {
		return "", ConstructNestedError("error during getting default account id request", err)
	}

	var defaultAccountResponse DefaultAccountResponse
	if err = json.Unmarshal(response, &defaultAccountResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling default account response", errors.New(string(response)))
	}

	return defaultAccountResponse.Account.Id, nil
}

func (c *ClientImplV0) getAccountID(ctx context.Context, accountName string) (string, error) {
	var accountId string
	var err error
	if accountName == "" {
		infolog.Println("account name not specified, trying to get a default account id")
		accountId, err = c.getDefaultAccountID(context.TODO())
	} else {
		accountId, err = c.getAccountIDByName(context.TODO(), accountName)
	}
	if err != nil {
		return "", ConstructNestedError("error during getting account id", err)
	}
	return accountId, nil
}

// GetEngineIdByName returns engineId based on engineName and accountId
func (c *ClientImplV0) GetEngineIdByName(ctx context.Context, engineName string, accountId string) (string, error) {
	infolog.Printf("get engine id by name '%s' and account id '%s'", engineName, accountId)

	type EngineIdByNameInnerResponse struct {
		AccountId string `json:"account_id"`
		EngineId  string `json:"engine_id"`
	}
	type EngineIdByNameResponse struct {
		EngineId EngineIdByNameInnerResponse `json:"engine_id"`
	}

	params := map[string]string{"engine_name": engineName}
	response, err := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+EngineIdByNameURL, accountId), params, "")
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
func (c *ClientImplV0) GetEngineUrlById(ctx context.Context, engineId string, accountId string) (string, error) {
	infolog.Printf("get engine url by id '%s' and account id '%s'", engineId, accountId)

	type EngineResponse struct {
		Endpoint string `json:"endpoint"`
	}
	type EngineByIdResponse struct {
		Engine EngineResponse `json:"engine"`
	}

	response, err := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+EngineByIdURL, accountId, engineId), make(map[string]string), "")

	if err != nil {
		return "", ConstructNestedError("error during getting engine url by id request", err)
	}

	var engineByIdResponse EngineByIdResponse
	if err = json.Unmarshal(response, &engineByIdResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling engine url by id response", errors.New(string(response)))
	}
	return makeCanonicalUrl(engineByIdResponse.Engine.Endpoint), nil
}

// GetEngineUrlByName return engine URL based on engineName and accountName
func (c *ClientImplV0) GetEngineUrlByName(ctx context.Context, engineName string, accountId string) (string, error) {
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
func (c *ClientImplV0) GetEngineUrlByDatabase(ctx context.Context, databaseName string, accountId string) (string, error) {
	infolog.Printf("get engine url by database name '%s' and account name '%s'", databaseName, accountId)

	type EngineUrlByDatabaseResponse struct {
		EngineUrl string `json:"engine_url"`
	}

	params := map[string]string{"database_name": databaseName}
	response, err := c.request(ctx, "GET", fmt.Sprintf(c.ApiEndpoint+EngineUrlByDatabaseNameURL, accountId), params, "")
	if err != nil {
		return "", ConstructNestedError("error during getting engine url by database request", err)
	}

	var engineUrlByDatabaseResponse EngineUrlByDatabaseResponse
	if err = json.Unmarshal(response, &engineUrlByDatabaseResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling engine url by database response", errors.New(string(response)))
	}
	return engineUrlByDatabaseResponse.EngineUrl, nil
}

// GetEngineUrlAndDB returns engine URL and engine name based on engineName and accountId
func (c *ClientImplV0) GetEngineUrlAndDB(ctx context.Context, engineName, databaseName string) (string, string, error) {
	// getting engineUrl either by using engineName if available,
	// if not using default engine for the database
	var engineUrl string
	var err error
	if engineName != "" {
		if strings.Contains(engineName, ".") {
			engineUrl, err = makeCanonicalUrl(engineName), nil
		} else {
			engineUrl, err = c.GetEngineUrlByName(ctx, engineName, c.AccountID)
		}
	} else {
		infolog.Println("engine name not set, trying to get a default engine")
		engineUrl, err = c.GetEngineUrlByDatabase(ctx, databaseName, c.AccountID)
	}
	if err != nil {
		return "", "", ConstructNestedError("error during getting engine url", err)
	}
	return engineUrl, databaseName, nil

}

func (c *ClientImplV0) getQueryParams(databaseName string, setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"database": databaseName, "output_format": outputFormat}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	return params, nil
}

func (c *ClientImplV0) getAccessToken() (string, error) {
	return getAccessTokenUsernamePassword(c.ClientID, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
}
