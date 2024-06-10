package fireboltgosdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/astaxie/beego/cache"
)

// Static caches on pacakge level
var AccountCache cache.Cache
var URLCache cache.Cache

type ClientImpl struct {
	ConnectedToSystemEngine bool
	AccountName             string
	AccountVersion          int
	BaseClient
}

const accountError = `account '%s' does not exist in this organization or is not authorized.
Please verify the account name and make sure your service account has the
correct RBAC permissions and is linked to a user`

func initialiseCaches() error {
	var err error
	if AccountCache == nil {
		if AccountCache, err = cache.NewCache("memory", `{}`); err != nil {
			return fmt.Errorf("could not create account cache: %v", err)
		}
	}
	if URLCache == nil {
		if URLCache, err = cache.NewCache("memory", `{}`); err != nil {
			return fmt.Errorf("could not create url cache: %v", err)
		}
	}
	return nil
}

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

	if err := initialiseCaches(); err != nil {
		infolog.Printf("Error during cache initialisation: %v", err)
	}

	var err error
	client.AccountID, client.AccountVersion, err = client.getAccountInfo(context.Background(), settings.accountName)
	if err != nil {
		return nil, ConstructNestedError("error during getting account id", err)
	}
	return client, nil
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
	if URLCache != nil {
		val := URLCache.Get(url)
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
	if URLCache != nil {
		URLCache.Put(url, systemEngineURLResponse, 0) //nolint:errcheck
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

	if AccountCache != nil {
		val := AccountCache.Get(url)
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
	if AccountCache != nil {
		AccountCache.Put(url, accountIdURLResponse, 0) //nolint:errcheck
	}

	infolog.Printf("Resolved account %s to id %s", accountName, accountIdURLResponse.Id)

	return accountIdURLResponse.Id, accountIdURLResponse.InfraVersion, nil
}

func (c *ClientImpl) getQueryParams(setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": outputFormat}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	return params, nil
}

func (c *ClientImpl) getAccessToken() (string, error) {
	return getAccessTokenServiceAccount(c.ClientID, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
}

func (c *ClientImpl) getConnectionParameters(
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

// GetConnectionParameters returns engine URL and parameters based on engineName and databaseName
func (c *ClientImpl) GetConnectionParameters(ctx context.Context, engineName, databaseName string) (string, map[string]string, error) {
	// Assume we are connected to a system engine in the beginning

	systemEngineURL, systemEngineParameters, err := c.getSystemEngineURLAndParameters(context.Background(), c.AccountName, databaseName)
	if err != nil {
		return "", nil, ConstructNestedError("error during getting system engine url", err)
	}

	c.ConnectedToSystemEngine = true
	return c.getConnectionParameters(ctx, engineName, databaseName, systemEngineURL, systemEngineParameters)
}
