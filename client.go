package fireboltgosdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

type ClientImpl struct {
	ConnectedToSystemEngine bool
	SystemEngineURL         string
	BaseClient
}

const engineStatusRunning = "Running"
const engineInfoSQL = `
SELECT url, status, attached_to FROM information_schema.engines
WHERE engine_name='%s'
`
const accountError = `account '%s' does not exist in this organization or is not authorized.
Please verify the account name and make sure your service account has the
correct RBAC permissions and is linked to a user`

func MakeClient(settings *fireboltSettings, apiEndpoint string) (*ClientImpl, error) {
	client := &ClientImpl{
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
		return nil, ConstructNestedError("error during getting account id", err)
	}
	client.SystemEngineURL, err = client.getSystemEngineURL(context.Background(), settings.accountName)
	if err != nil {
		return nil, ConstructNestedError("error during getting system engine url", err)
	}
	return client, nil
}

func (c *ClientImpl) getEngineUrlStatusDBByName(ctx context.Context, engineName string, systemEngineUrl string) (string, string, string, error) {
	infolog.Printf("Get info for engine '%s'", engineName)
	engineSQL := fmt.Sprintf(engineInfoSQL, engineName)
	queryRes, err := c.Query(ctx, systemEngineUrl, engineSQL, make(map[string]string), func(key, value string) {
		// No need to support set statements for engine info query
	})
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

func (c *ClientImpl) getSystemEngineURL(ctx context.Context, accountName string) (string, error) {
	infolog.Printf("Get system engine URL for account '%s'", accountName)

	type SystemEngineURLResponse struct {
		EngineUrl string `json:"engineUrl"`
	}

	url := fmt.Sprintf(c.ApiEndpoint+EngineUrlByAccountName, accountName)

	resp := c.request(ctx, "GET", url, make(map[string]string), "")
	if resp.statusCode == 404 {
		return "", fmt.Errorf(accountError, accountName)
	}
	if resp.err != nil {
		return "", ConstructNestedError("error during system engine url http request", resp.err)
	}

	var systemEngineURLResponse SystemEngineURLResponse
	if err := json.Unmarshal(resp.data, &systemEngineURLResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling system engine URL response", errors.New(string(resp.data)))
	}

	return systemEngineURLResponse.EngineUrl, nil
}

func (c *ClientImpl) getAccountID(ctx context.Context, accountName string) (string, error) {
	infolog.Printf("Getting account ID for '%s'", accountName)

	type AccountIdURLResponse struct {
		Id     string `json:"id"`
		Region string `json:"region"`
	}

	url := fmt.Sprintf(c.ApiEndpoint+AccountIdByAccountName, accountName)

	resp := c.request(ctx, "GET", url, make(map[string]string), "")
	if resp.statusCode == 404 {
		return "", fmt.Errorf(accountError, accountName)
	}
	if resp.err != nil {
		return "", ConstructNestedError("error during account id resolution http request", resp.err)
	}

	var accountIdURLResponse AccountIdURLResponse
	if err := json.Unmarshal(resp.data, &accountIdURLResponse); err != nil {
		return "", ConstructNestedError("error during unmarshalling account id resolution URL response", errors.New(string(resp.data)))
	}

	infolog.Printf("Resolved account %s to id %s", accountName, accountIdURLResponse.Id)

	return accountIdURLResponse.Id, nil
}

func (c *ClientImpl) getQueryParams(setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": outputFormat}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	// Account id is only used when querying system engine
	if c.ConnectedToSystemEngine {
		if len(c.AccountID) == 0 {
			return nil, fmt.Errorf("Trying to run a query against system engine without account id defined")
		}
		params["account_id"] = c.AccountID
	}
	return params, nil
}

func (c *ClientImpl) getAccessToken() (string, error) {
	return getAccessTokenServiceAccount(c.ClientID, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
}

// GetEngineUrlAndDB returns engine URL and engine name based on engineName and accountId
func (c *ClientImpl) GetEngineUrlAndDB(ctx context.Context, engineName, databaseName string) (string, string, error) {
	// Assume we are connected to a system engine in the beginning
	c.ConnectedToSystemEngine = true
	// If engine name is empty, assume system engine
	if len(engineName) == 0 {
		return c.SystemEngineURL, databaseName, nil
	}

	engineUrl, status, dbName, err := c.getEngineUrlStatusDBByName(ctx, engineName, c.SystemEngineURL)
	if err != nil {
		return "", "", ConstructNestedError("error during getting engine info", err)
	}
	if status != engineStatusRunning {
		return "", "", fmt.Errorf("engine %s is not running", engineName)
	}
	if len(dbName) == 0 {
		return "", "", fmt.Errorf("engine %s not attached to any DB or you don't have permission to access its database", engineName)
	}
	if len(databaseName) != 0 && databaseName != dbName {
		return "", "", fmt.Errorf("engine %s is not attached to database %s", engineName, databaseName)
	}
	c.ConnectedToSystemEngine = false

	return engineUrl, dbName, nil
}
