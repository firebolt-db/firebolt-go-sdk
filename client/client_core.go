package client

import (
	"context"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type ClientImplCore struct {
	AccountName string
	BaseClient
}

func MakeClientCore(settings *types.FireboltSettings) (*ClientImplCore, error) {
	client := &ClientImplCore{
		BaseClient: BaseClient{
			ApiEndpoint: settings.Url,
			UserAgent:   ConstructUserAgentString(),
			HttpClient:  NewHttpClient(),
		},
		AccountName: settings.AccountName,
	}
	client.ParameterGetter = client.GetQueryParams
	client.AccessTokenGetter = client.getAccessToken

	return client, nil
}

func (c *ClientImplCore) getOutputFormat(ctx context.Context) string {
	if contextUtils.IsStreaming(ctx) {
		return jsonLinesOutputFormat
	}
	return jsonOutputFormat
}

func (c *ClientImplCore) GetQueryParams(ctx context.Context, setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": c.getOutputFormat(ctx)}
	if contextUtils.IsAsync(ctx) {
		return nil, errorUtils.AsyncNotSupportedError
	}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	return params, nil
}

func (c *ClientImplCore) getAccessToken() (string, error) {
	return "", nil // No access token needed for core client
}

// GetConnectionParameters returns engine URL and parameters based on engineName and databaseName
func (c *ClientImplCore) GetConnectionParameters(_ context.Context, _, databaseName string) (string, map[string]string, error) {
	params := make(map[string]string)
	if databaseName != "" {
		params["database"] = databaseName
	}
	return c.ApiEndpoint, params, nil
}
