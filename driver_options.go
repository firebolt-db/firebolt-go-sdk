package fireboltgosdk

import (
	"context"
	"errors"

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
		d.engineUrl = engineUrl
	}
}

// WithDatabaseName defines database name for the driver
func WithDatabaseName(databaseName string) driverOption {
	return func(d *FireboltDriver) {
		if d.cachedParams == nil {
			d.cachedParams = map[string]string{}
		}
		d.cachedParams["database"] = databaseName
	}
}

// WithAccountID defines account ID for the driver
func WithAccountID(accountID string) driverOption {
	return func(d *FireboltDriver) {
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
		if d.client != nil {
			if clientImpl, ok := d.client.(*client.ClientImpl); ok {
				setter(&clientImpl.BaseClient)
			} else if clientImplV0, ok := d.client.(*client.ClientImplV0); ok {
				setter(&clientImplV0.BaseClient)
			}
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
		if d.client != nil {
			if clientImpl, ok := d.client.(*client.ClientImpl); ok {
				clientImpl.AccountName = accountName
			} else if clientImplV0, ok := d.client.(*client.ClientImplV0); ok {
				var err error
				clientImplV0.AccountID, err = clientImplV0.GetAccountID(context.TODO(), accountName)
				if err != nil {
					return err
				}
			}
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
		if d.client == nil {
			return errors.New("client must be initialized before setting database and engine name")
		}
		oldCachedParameters := d.cachedParams
		var err error
		d.engineUrl, d.cachedParams, err = d.client.GetConnectionParameters(context.TODO(), engineName, databaseName)
		if err != nil {
			return err
		}
		for key, value := range oldCachedParameters {
			d.cachedParams[key] = value
		}
		return nil
	}
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
