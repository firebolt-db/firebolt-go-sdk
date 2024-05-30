package fireboltgosdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/astaxie/beego/cache"
)

type ClientImpl struct {
	ConnectedToSystemEngine bool
	AccountName             string
	AccountVersion          int
	AccountCache            cache.Cache
	URLCache                cache.Cache
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
		AccountName: settings.accountName,
	}
	client.parameterGetter = client.getQueryParams
	client.accessTokenGetter = client.getAccessToken

	var err error
	if client.AccountCache, err = cache.NewCache("memory", `{}`); err != nil {
		infolog.Println(fmt.Errorf("could not create account cache: %v", err))
	}
	if client.URLCache, err = cache.NewCache("memory", `{}`); err != nil {
		infolog.Println(fmt.Errorf("could not create url cache: %v", err))
	}
	client.AccountID, client.AccountVersion, err = client.getAccountInfo(context.Background(), settings.accountName)
	if err != nil {
		return nil, ConstructNestedError("error during getting account id", err)
	}
	return client, nil
}

func (c *ClientImpl) getEngineUrlStatusDBByName(ctx context.Context, engineName string, systemEngineUrl string) (string, string, string, error) {
	infolog.Printf("Get info for engine '%s'", engineName)
	engineSQL := fmt.Sprintf(engineInfoSQL, engineName)
	queryRes, err := c.Query(ctx, systemEngineUrl, engineSQL, make(map[string]string), connectionControl{})
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

func constructParameters(databaseName string, queryParams map[string][]string) map[string]string {
	parameters := make(map[string]string)
	if len(databaseName) != 0 {
		parameters["database"] = databaseName
	}
	for key, value := range queryParams {
		parameters[key] = value[0]
	}
	return parameters
}

func (c *ClientImpl) getSystemEngineURLAndParameters(ctx context.Context, accountName string, databaseName string) (string, map[string]string, error) {
	infolog.Printf("Get system engine URL for account '%s'", accountName)

	type SystemEngineURLResponse struct {
		EngineUrl string `json:"engineUrl"`
	}

	url := fmt.Sprintf(c.ApiEndpoint+EngineUrlByAccountName, accountName)
	// Check if the URL is in the cache
	if c.URLCache != nil {
		val := c.URLCache.Get(url)
		if val != nil {
			if systemEngineURLResponse, ok := val.(SystemEngineURLResponse); ok {
				infolog.Printf("Resolved account %s to system engine URL %s from cache", accountName, systemEngineURLResponse.EngineUrl)
				engineUrl, queryParams, err := splitEngineEndpoint(systemEngineURLResponse.EngineUrl)
				if err != nil {
					return "", nil, ConstructNestedError("error during splitting system engine URL", err)
				}
				parameters := constructParameters(databaseName, queryParams)
				return engineUrl, parameters, nil
			}
		}
	}

	resp := c.request(ctx, "GET", url, make(map[string]string), "")
	if resp.statusCode == 404 {
		return "", nil, fmt.Errorf(accountError, accountName)
	}
	if resp.err != nil {
		return "", nil, ConstructNestedError("error during system engine url http request", resp.err)
	}

	var systemEngineURLResponse SystemEngineURLResponse
	if err := json.Unmarshal(resp.data, &systemEngineURLResponse); err != nil {
		return "", nil, ConstructNestedError("error during unmarshalling system engine URL response", errors.New(string(resp.data)))
	}
	if c.URLCache != nil {
		c.URLCache.Put(url, systemEngineURLResponse, 0) //nolint:errcheck
	}
	engineUrl, queryParams, err := splitEngineEndpoint(systemEngineURLResponse.EngineUrl)
	if err != nil {
		return "", nil, ConstructNestedError("error during splitting system engine URL", err)
	}

	parameters := constructParameters(databaseName, queryParams)

	return engineUrl, parameters, nil
}

func (c *ClientImpl) getAccountInfo(ctx context.Context, accountName string) (string, int, error) {

	type AccountIdURLResponse struct {
		Id           string `json:"id"`
		Region       string `json:"region"`
		InfraVersion int    `json:"infraVersion"`
	}

	url := fmt.Sprintf(c.ApiEndpoint+AccountInfoByAccountName, accountName)

	if c.AccountCache != nil {
		val := c.AccountCache.Get(url)
		if val != nil {
			if accountInfo, ok := val.(AccountIdURLResponse); ok {
				infolog.Printf("Resolved account %s to id %s from cache", accountName, accountInfo.Id)
				return accountInfo.Id, accountInfo.InfraVersion, nil
			}
		}
	}
	infolog.Printf("Getting account ID for '%s'", accountName)

	resp := c.request(ctx, "GET", url, make(map[string]string), "")
	if resp.statusCode == 404 {
		return "", 0, fmt.Errorf(accountError, accountName)
	}
	if resp.err != nil {
		return "", 0, ConstructNestedError("error during account id resolution http request", resp.err)
	}

	var accountIdURLResponse AccountIdURLResponse
	// InfraVersion should default to 1 if not present
	accountIdURLResponse.InfraVersion = 1
	if err := json.Unmarshal(resp.data, &accountIdURLResponse); err != nil {
		return "", 0, ConstructNestedError("error during unmarshalling account id resolution URL response", errors.New(string(resp.data)))
	}
	if c.AccountCache != nil {
		c.AccountCache.Put(url, accountIdURLResponse, 0) //nolint:errcheck
	}

	infolog.Printf("Resolved account %s to id %s", accountName, accountIdURLResponse.Id)

	return accountIdURLResponse.Id, accountIdURLResponse.InfraVersion, nil
}

func (c *ClientImpl) getQueryParams(setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": outputFormat}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	// Account id is only used when querying system engine for infra v1
	if c.ConnectedToSystemEngine && c.AccountVersion == 1 {
		if len(c.AccountID) == 0 {
			return nil, fmt.Errorf("Trying to run a query against system engine without account id defined")
		}
		if _, ok := params["account_id"]; !ok {
			params["account_id"] = c.AccountID
		}
	}
	return params, nil
}

func (c *ClientImpl) getAccessToken() (string, error) {
	return getAccessTokenServiceAccount(c.ClientID, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
}

func (c *ClientImpl) getConnectionParametersV2(
	ctx context.Context,
	engineName,
	databaseName,
	systemEngineURL string,
	systemEngineParameters map[string]string,
) (string, map[string]string, error) {
	engineURL := systemEngineURL
	parameters := systemEngineParameters
	control := connectionControl{
		updateParameters: func(key, value string) {
			parameters[key] = value
		},
		setEngineURL: func(s string) {
			engineURL = s
		},
		resetParameters: func() {},
	}
	if databaseName != "" {
		sql := fmt.Sprintf("USE DATABASE \"%s\"", databaseName)
		if _, err := c.Query(ctx, engineURL, sql, parameters, control); err != nil {
			return "", nil, err
		}
	}
	if engineName != "" {
		sql := fmt.Sprintf("USE ENGINE \"%s\"", engineName)
		if _, err := c.Query(ctx, engineURL, sql, parameters, control); err != nil {
			return "", nil, err
		}
	}
	return engineURL, parameters, nil
}

func (c *ClientImpl) getConnectionParametersV1(
	ctx context.Context,
	engineName,
	databaseName,
	systemEngineURL string,
) (string, map[string]string, error) {
	// If engine name is empty, assume system engine
	if len(engineName) == 0 {
		return systemEngineURL, map[string]string{"database": databaseName}, nil
	}

	engineUrl, status, dbName, err := c.getEngineUrlStatusDBByName(ctx, engineName, systemEngineURL)
	params := map[string]string{"database": dbName}
	if err != nil {
		return "", params, ConstructNestedError("error during getting engine info", err)
	}
	// Case-insensitive comparison
	if !strings.EqualFold(status, engineStatusRunning) {
		return "", params, fmt.Errorf("engine %s is not running", engineName)
	}
	if len(dbName) == 0 {
		return "", params, fmt.Errorf("engine %s not attached to any DB or you don't have permission to access its database", engineName)
	}
	if len(databaseName) != 0 && databaseName != dbName {
		return "", params, fmt.Errorf("engine %s is not attached to database %s", engineName, databaseName)
	}
	c.ConnectedToSystemEngine = false

	return engineUrl, params, nil
}

// GetConnectionParameters returns engine URL and parameters based on engineName and databaseName
func (c *ClientImpl) GetConnectionParameters(ctx context.Context, engineName, databaseName string) (string, map[string]string, error) {
	// Assume we are connected to a system engine in the beginning

	systemEngineURL, systemEngineParameters, err := c.getSystemEngineURLAndParameters(context.Background(), c.AccountName, databaseName)
	if err != nil {
		return "", nil, ConstructNestedError("error during getting system engine url", err)
	}

	c.ConnectedToSystemEngine = true
	if c.AccountVersion == 2 {
		return c.getConnectionParametersV2(ctx, engineName, databaseName, systemEngineURL, systemEngineParameters)
	}
	return c.getConnectionParametersV1(ctx, engineName, databaseName, systemEngineURL)
}
