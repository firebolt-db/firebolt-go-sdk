package fireboltgosdk

import "github.com/firebolt-db/firebolt-go-sdk/client"

type driverOption func(d *FireboltDriver)

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
				BaseClient:              client.BaseClient{},
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
