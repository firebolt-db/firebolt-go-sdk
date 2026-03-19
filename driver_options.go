package fireboltgosdk

import (
	"context"
	"errors"
	"net/http"

	"github.com/firebolt-db/firebolt-go-sdk/client"
)

type driverOption func(d *FireboltDriver)
type driverOptionWithError func(d *FireboltDriver) error

func NoError(option driverOption) driverOptionWithError {
	return func(d *FireboltDriver) error {
		option(d)
		return nil
	}
}

// WithEngineUrl defines engine url for the driver
func WithEngineUrl(engineUrl string) driverOption {
	return func(d *FireboltDriver) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		d.engineUrl = engineUrl
	}
}

// WithDatabaseName defines database name for the driver
func WithDatabaseName(databaseName string) driverOption {
	return func(d *FireboltDriver) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.cachedParams == nil {
			d.cachedParams = map[string]string{}
		}
		d.cachedParams["database"] = databaseName
	}
}

// WithAccountID defines account ID for the driver
func WithAccountID(accountID string) driverOption {
	return func(d *FireboltDriver) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.cachedParams == nil {
			d.cachedParams = map[string]string{}
		}
		if accountID != "" {
			d.cachedParams["account_id"] = accountID
		}
	}
}

func withClientOption(setter func(baseClient *client.BaseClient)) driverOption {
	return func(d *FireboltDriver) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.client != nil {
			if clientImpl, ok := d.client.(*client.ClientImpl); ok {
				setter(&clientImpl.BaseClient)
			}
			// ignore V0 client since it's not supported
		} else {
			cl := &client.ClientImpl{
				ConnectedToSystemEngine: true,
				BaseClient: client.BaseClient{
					ApiEndpoint: client.GetHostNameURL(),
				},
			}
			cl.ParameterGetter = cl.GetQueryParams
			setter(&cl.BaseClient)
			d.client = cl
		}
	}
}

// WithToken defines token for the driver
func WithToken(token string) driverOption {
	return withClientOption(func(baseClient *client.BaseClient) {
		baseClient.AccessTokenGetter = func() (string, error) {
			return token, nil
		}
	})
}

// WithUserAgent defines user agent for the driver
func WithUserAgent(userAgent string) driverOption {
	return withClientOption(func(baseClient *client.BaseClient) {
		baseClient.UserAgent = userAgent
	})
}

// WithClientParams defines client parameters for the driver
func WithClientParams(accountID string, token string, userAgent string) driverOption {
	return func(d *FireboltDriver) {
		WithAccountID(accountID)(d)
		WithToken(token)(d)
		WithUserAgent(userAgent)(d)
	}
}

// WithAccountName defines account name for the driver
func WithAccountName(accountName string) driverOptionWithError {
	return func(d *FireboltDriver) error {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.client != nil {
			if clientImpl, ok := d.client.(*client.ClientImpl); ok {
				clientImpl.AccountName = accountName
			}
			// ignore V0 client since it's not supported
		} else {
			cl := &client.ClientImpl{
				ConnectedToSystemEngine: true,
				BaseClient: client.BaseClient{
					ApiEndpoint: client.GetHostNameURL(),
				},
				AccountName: accountName,
			}
			cl.ParameterGetter = cl.GetQueryParams
			d.client = cl
		}
		return nil
	}
}

// WithDatabaseAndEngineName defines database name and engine name for the driver
func WithDatabaseAndEngineName(databaseName, engineName string) driverOptionWithError {
	return func(d *FireboltDriver) error {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.client == nil {
			return errors.New("client must be initialized before setting database and engine name")
		}
		var err error
		d.engineUrl, d.cachedParams, err = d.client.GetConnectionParameters(context.TODO(), engineName, databaseName)
		if err != nil {
			return err
		}
		return nil
	}
}

// WithTransport sets a custom http.RoundTripper for all HTTP requests made
// by the SDK (queries, batch uploads, authentication). Use
// client.DefaultTransport() as a starting point and override specific fields,
// or wrap it with middleware (e.g. otelhttp for tracing):
//
//	transport := client.DefaultTransport()
//	transport.DialContext = (&net.Dialer{Timeout: 60*time.Second}).DialContext
//	connector, _ := firebolt.OpenConnectorWithDSN(dsn, firebolt.WithTransport(transport))
//	db := sql.OpenDB(connector)
func WithTransport(transport http.RoundTripper) driverOption {
	return func(d *FireboltDriver) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		d.transport = transport
	}
}

// WithDefaultQueryParams defines default query parameters that will be seeded into the connection
// These parameters will be included in all HTTP requests and can be overridden by SET statements
func WithDefaultQueryParams(params map[string]string) driverOption {
	return func(d *FireboltDriver) {
		d.mutex.Lock()
		defer d.mutex.Unlock()
		if d.cachedParams == nil {
			d.cachedParams = make(map[string]string)
		}
		// Seed defaults only if they don't already exist
		for k, v := range params {
			if _, exists := d.cachedParams[k]; !exists {
				d.cachedParams[k] = v
			}
		}
	}
}

// OpenConnectorWithDSN parses a DSN string and applies the given driver
// options (e.g. WithTransport), returning a connector suitable for
// sql.OpenDB. This is the recommended way to customize transport settings
// that cannot be expressed in a DSN string:
//
//	transport := client.DefaultTransport()
//	transport.IdleConnTimeout = 2 * time.Minute
//	connector, err := firebolt.OpenConnectorWithDSN(dsn, firebolt.WithTransport(transport))
//	db := sql.OpenDB(connector)
func OpenConnectorWithDSN(dsn string, opts ...driverOption) (*FireboltConnector, error) {
	d := &FireboltDriver{}
	for _, opt := range opts {
		opt(d)
	}
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.(*FireboltConnector), nil
}

// FireboltConnectorWithOptions builds a custom connector
func FireboltConnectorWithOptions(opts ...driverOption) *FireboltConnector {
	d := &FireboltDriver{}

	for _, opt := range opts {
		opt(d)
	}

	return &FireboltConnector{
		d.engineUrl,
		d.client,
		d.cachedParams,
		d,
	}
}

// FireboltConnectorWithOptionsWithErrors builds a custom connector with error handling
func FireboltConnectorWithOptionsWithErrors(opts ...driverOptionWithError) (*FireboltConnector, error) {
	d := &FireboltDriver{}

	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}

	return &FireboltConnector{
		d.engineUrl,
		d.client,
		d.cachedParams,
		d,
	}, nil
}
