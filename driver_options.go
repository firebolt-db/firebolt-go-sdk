package fireboltgosdk

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

// WithClientParams defines client parameters for the driver
func WithClientParams(accountID string, token string, userAgent string) driverOption {
	return func(d *FireboltDriver) {
		if d.cachedParams == nil {
			d.cachedParams = map[string]string{}
		}
		// Put account_id in cachedParams for it to work both with engines v1 and v2
		d.cachedParams["account_id"] = accountID

		cl := &ClientImpl{
			ConnectedToSystemEngine: true,
		}

		cl.UserAgent = userAgent

		cl.parameterGetter = cl.getQueryParams
		cl.accessTokenGetter = func() (string, error) {
			return token, nil
		}

		d.client = cl
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
