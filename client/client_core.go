package client

import (
	"context"
	"net/url"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/logging"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

type ClientImplCore struct {
	AccountName string
	BaseClient
}

func MakeClientCore(settings *types.FireboltSettings) (*ClientImplCore, error) {
	httpClient := NewHttpClientWithTransport(settings.Transport)
	var resolver *RoundRobinResolver

	if settings.ClientSideLB {
		var err error
		resolver, err = NewRoundRobinResolver(settings.Url, nil)
		if err != nil {
			return nil, err
		}
		if settings.DNSTTL > 0 {
			resolver.TTL = settings.DNSTTL
		}
		// Use a TLS-aware client when the scheme is HTTPS, so certificate
		// verification still works after the resolver rewrites the URL host
		// to a raw IP address.
		canonical := MakeCanonicalUrl(settings.Url)
		if parsed, err := url.Parse(canonical); err == nil && parsed.Scheme == "https" {
			httpClient = NewHttpClientForLBWithTransport(settings.Transport, parsed.Hostname())
		}
		logging.Infolog.Printf("client-side load balancing enabled for %s (DNS TTL: %s)", settings.Url, resolver.TTL)
	}

	client := &ClientImplCore{
		BaseClient: BaseClient{
			ApiEndpoint: settings.Url,
			UserAgent:   ConstructUserAgentString(),
			HttpClient:  httpClient,
			URLResolver: resolver,
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
