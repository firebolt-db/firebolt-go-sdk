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

// WithToken defines token for the driver
func WithToken(token string) driverOption {
	return func(d *FireboltDriver) {
		if d.client != nil {
			if clientImpl, ok := d.client.(*client.ClientImpl); ok {
				clientImpl.AccessTokenGetter = func() (string, error) {
					return token, nil
				}
			} else if clientImplV0, ok := d.client.(*client.ClientImplV0); ok {
				clientImplV0.AccessTokenGetter = func() (string, error) {
					return token, nil
				}
			}
		} else {
			cl := &client.ClientImpl{
				ConnectedToSystemEngine: true,
				BaseClient: client.BaseClient{
					AccessTokenGetter: func() (string, error) {
						return token, nil
					},
				},
			}
			cl.ParameterGetter = cl.GetQueryParams
			d.client = cl
		}
	}
}

// WithUserAgent defines user agent for the driver
func WithUserAgent(userAgent string) driverOption {
	return func(d *FireboltDriver) {
		if d.client != nil {
			if clientImpl, ok := d.client.(*client.ClientImpl); ok {
				clientImpl.UserAgent = userAgent
			} else if clientImplV0, ok := d.client.(*client.ClientImplV0); ok {
				clientImplV0.UserAgent = userAgent
			}
		} else {
			cl := &client.ClientImpl{
				ConnectedToSystemEngine: true,
				BaseClient: client.BaseClient{
					UserAgent: userAgent,
				},
			}
			cl.ParameterGetter = cl.GetQueryParams
			d.client = cl
		}
	}
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
