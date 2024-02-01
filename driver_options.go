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
		d.databaseName = databaseName
	}
}

// WithClientParams defines client parameters for the driver
func WithClientParams(accountID string, token string, userAgent string) driverOption {
	return func(d *FireboltDriver) {
		cl := &ClientImpl{
			ConnectedToSystemEngine: true,
		}

		cl.AccountID = accountID
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
		d.databaseName,
		d.client,
		map[string]string{},
		d,
	}
}
