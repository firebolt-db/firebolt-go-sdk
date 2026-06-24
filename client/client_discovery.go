package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/firebolt-db/firebolt-go-sdk/types"
)

const discoveryPath = "/.well-known/firebolt"

type ClientImplDiscovery struct {
	BaseClient
	ConnectionParameters map[string]string
}

type discoveryResponse struct {
	EngineURL       string            `json:"engine_url"`
	EngineUrl       string            `json:"engineUrl"`
	Endpoint        string            `json:"endpoint"`
	QueryEndpoint   string            `json:"query_endpoint"`
	QueryEndpointJS string            `json:"queryEndpoint"`
	SQLURL          string            `json:"sql_url"`
	SQLUrl          string            `json:"sqlUrl"`
	SQLHTTPURL      string            `json:"sql_http_url"`
	Parameters      map[string]string `json:"parameters"`
	QueryParameters map[string]string `json:"query_parameters"`
}

func MakeClientDiscovery(settings *types.FireboltSettings) (*ClientImplDiscovery, error) {
	httpClient := NewHttpClientWithTransportAndTLS(settings.Transport, settings.SSLMode == "none")
	client := &ClientImplDiscovery{
		BaseClient: BaseClient{
			ApiEndpoint: settings.DiscoveryEndpoint,
			UserAgent:   ConstructUserAgentString(),
			HttpClient:  httpClient,
		},
		ConnectionParameters: copyStringMap(settings.ConnectionParameters),
	}
	client.ParameterGetter = client.GetQueryParams
	client.AccessTokenGetter = client.getAccessToken

	return client, nil
}

func (c *ClientImplDiscovery) getOutputFormat(ctx context.Context) string {
	if contextUtils.IsStreaming(ctx) {
		return jsonLinesOutputFormat
	}
	return jsonOutputFormat
}

func (c *ClientImplDiscovery) GetQueryParams(ctx context.Context, setStatements map[string]string) (map[string]string, error) {
	params := map[string]string{"output_format": c.getOutputFormat(ctx)}
	if contextUtils.IsAsync(ctx) {
		return nil, errorUtils.AsyncNotSupportedError
	}
	for setKey, setValue := range setStatements {
		params[setKey] = setValue
	}
	return params, nil
}

func (c *ClientImplDiscovery) getAccessToken() (string, error) {
	return "", nil
}

func (c *ClientImplDiscovery) GetConnectionParameters(ctx context.Context, engineName, databaseName string) (string, map[string]string, error) {
	discovery, err := c.getDiscovery(ctx)
	if err != nil {
		return "", nil, err
	}

	engineURL, discoveredParams, err := c.resolveDiscoveredEndpoint(discovery.endpoint())
	if err != nil {
		return "", nil, err
	}

	parameters := copyStringMap(discoveredParams)
	for k, v := range discovery.Parameters {
		parameters[k] = v
	}
	for k, v := range discovery.QueryParameters {
		parameters[k] = v
	}
	for k, v := range c.ConnectionParameters {
		parameters[k] = v
	}
	if databaseName != "" {
		parameters["database"] = databaseName
	}
	if engineName != "" {
		parameters["engine"] = engineName
	}

	return engineURL, parameters, nil
}

func (c *ClientImplDiscovery) getDiscovery(ctx context.Context) (*discoveryResponse, error) {
	discoveryURL := strings.TrimRight(c.ApiEndpoint, "/") + discoveryPath
	resp := c.requestWithAuthRetry(ctx, http.MethodGet, discoveryURL, nil, "")
	if resp.err != nil {
		return nil, errorUtils.ConstructNestedError("error during discovery request", resp.err)
	}
	content, err := resp.Content()
	if err != nil {
		return nil, errorUtils.ConstructNestedError("error during reading discovery response", err)
	}

	var discovery discoveryResponse
	if len(content) == 0 {
		return nil, errors.New("empty discovery response")
	}
	if err := json.Unmarshal(content, &discovery); err != nil {
		return nil, errorUtils.ConstructNestedError("error during unmarshalling discovery response", err)
	}
	return &discovery, nil
}

func (r *discoveryResponse) endpoint() string {
	for _, endpoint := range []string{
		r.EngineURL,
		r.EngineUrl,
		r.Endpoint,
		r.QueryEndpoint,
		r.QueryEndpointJS,
		r.SQLURL,
		r.SQLUrl,
		r.SQLHTTPURL,
	} {
		if endpoint != "" {
			return endpoint
		}
	}
	return ""
}

func (c *ClientImplDiscovery) resolveDiscoveredEndpoint(discovered string) (string, map[string]string, error) {
	if discovered == "" {
		return "", nil, errors.New("discovery response does not contain an endpoint")
	}

	base, err := url.Parse(MakeCanonicalUrl(c.ApiEndpoint))
	if err != nil {
		return "", nil, err
	}

	endpointURL, err := parseEndpointRelativeToBase(discovered, base)
	if err != nil {
		return "", nil, err
	}

	parameters := make(map[string]string)
	for key, values := range endpointURL.Query() {
		if len(values) > 0 {
			parameters[key] = values[0]
		}
	}
	endpointURL.RawQuery = ""
	return endpointURL.String(), parameters, nil
}

func parseEndpointRelativeToBase(endpoint string, base *url.URL) (*url.URL, error) {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return url.Parse(endpoint)
	}
	if strings.HasPrefix(endpoint, "/") {
		return base.ResolveReference(&url.URL{Path: endpoint}), nil
	}
	return url.Parse(fmt.Sprintf("%s://%s", base.Scheme, endpoint))
}

func copyStringMap(original map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range original {
		result[k] = v
	}
	return result
}
