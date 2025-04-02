package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"

	"github.com/firebolt-db/firebolt-go-sdk/types"

	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"

	"github.com/astaxie/beego/cache"
)

// Static caches on pacakge level
var AccountCache cache.Cache
var URLCache cache.Cache

type ClientImpl struct {
	ConnectedToSystemEngine bool
	AccountName             string
	BaseClient
}

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

func MakeClient(settings *types.FireboltSettings, apiEndpoint string) (*ClientImpl, error) {
	client := &ClientImpl{
		BaseClient: BaseClient{
			ClientID:     settings.ClientID,
			ClientSecret: settings.ClientSecret,
			ApiEndpoint:  apiEndpoint,
			UserAgent:    ConstructUserAgentString(),
		},
		AccountName: settings.AccountName,
	}
	client.ParameterGetter = client.GetQueryParams
	client.AccessTokenGetter = client.getAccessToken

	if err := initialiseCaches(); err != nil {
		logging.Infolog.Printf("Error during cache initialisation: %v", err)
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
	logging.Infolog.Printf("Get system engine URL for account '%s'", accountName)

	type SystemEngineURLResponse struct {
		EngineUrl string `json:"engineUrl"`
	}

	url := fmt.Sprintf(c.ApiEndpoint+EngineUrlByAccountName, accountName)
	// Check if the URL is in the cache
	if URLCache != nil {
		val := URLCache.Get(url)
		if val != nil {
			if systemEngineURLResponse, ok := val.(SystemEngineURLResponse); ok {
				logging.Infolog.Printf("Resolved account %s to system engine URL %s from cache", accountName, systemEngineURLResponse.EngineUrl)
				engineUrl, queryParams, err := splitEngineEndpoint(systemEngineURLResponse.EngineUrl)
				if err != nil {
					return "", nil, errorUtils.ConstructNestedError("error during splitting system engine URL", err)
				}
				parameters := constructParameters(databaseName, queryParams)
				return engineUrl, parameters, nil
			}
		}
	}

	resp := c.requestWithAuthRetry(ctx, "GET", url, make(map[string]string), "")
	if resp.statusCode == 404 {
		return "", nil, errorUtils.InvalidAccountError
	}
	if resp.err != nil {
		return "", nil, errorUtils.ConstructNestedError("error during system engine url http request", resp.err)
	}

	content, err := resp.Content()
	if err != nil {
		return "", nil, errorUtils.ConstructNestedError("error during reading response content", err)
	}

	var systemEngineURLResponse SystemEngineURLResponse
	if err := json.Unmarshal(content, &systemEngineURLResponse); err != nil {
		return "", nil, errorUtils.ConstructNestedError("error during unmarshalling system engine URL response", errors.New(string(content)))
	}
	if URLCache != nil {
		URLCache.Put(url, systemEngineURLResponse, 0) //nolint:errcheck
	}
	engineUrl, queryParams, err := splitEngineEndpoint(systemEngineURLResponse.EngineUrl)
	if err != nil {
		return "", nil, errorUtils.ConstructNestedError("error during splitting system engine URL", err)
	}

	parameters := constructParameters(databaseName, queryParams)

	return engineUrl, parameters, nil
}

func (c *ClientImpl) getOutputFormat(ctx context.Context) string {
	if contextUtils.IsStreaming(ctx) {
		return jsonLinesOutputFormat
	}
	return jsonOutputFormat
}

func (c *ClientImpl) GetQueryParams(ctx context.Context, setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": c.getOutputFormat(ctx)}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	return params, nil
}

func (c *ClientImpl) getAccessToken() (string, error) {
	return getAccessTokenServiceAccount(c.ClientID, c.ClientSecret, c.ApiEndpoint, c.UserAgent)
}

// GetConnectionParameters returns engine URL and parameters based on engineName and databaseName
func (c *ClientImpl) GetConnectionParameters(ctx context.Context, engineName, databaseName string) (string, map[string]string, error) {
	// Assume we are connected to a system engine in the beginning

	engineURL, parameters, err := c.getSystemEngineURLAndParameters(context.Background(), c.AccountName, databaseName)
	if err != nil {
		return "", nil, errorUtils.ConstructNestedError("error during getting system engine url", err)
	}
	c.ConnectedToSystemEngine = true

	control := ConnectionControl{
		UpdateParameters: func(key, value string) {
			parameters[key] = value
		},
		SetEngineURL: func(s string) {
			engineURL = s
		},
		ResetParameters: func() { /* empty because we only want to collect new parameters without resetting anything*/ },
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
