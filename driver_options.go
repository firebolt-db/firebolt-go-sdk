package fireboltgosdk

import (
	"context"
	"errors"

	"github.com/firebolt-db/firebolt-go-sdk/client"
)

// ConnectorConfig holds the configuration for creating a FireboltConnector
type ConnectorConfig struct {
	engineUrl    string
	client       client.Client
	cachedParams map[string]string
}

type driverOption func(config *ConnectorConfig)
type driverOptionWithError func(config *ConnectorConfig) error

func NoError(option driverOption) driverOptionWithError {
	return func(config *ConnectorConfig) error {
		option(config)
		return nil
	}
}

// WithEngineUrl defines engine url for the driver
func WithEngineUrl(engineUrl string) driverOption {
	return func(config *ConnectorConfig) {
		config.engineUrl = engineUrl
	}
}

// WithDatabaseName defines database name for the driver
func WithDatabaseName(databaseName string) driverOption {
	return func(config *ConnectorConfig) {
		if config.cachedParams == nil {
			config.cachedParams = map[string]string{}
		}
		config.cachedParams["database"] = databaseName
	}
}

// WithAccountID defines account ID for the driver
func WithAccountID(accountID string) driverOption {
	return func(config *ConnectorConfig) {
		if config.cachedParams == nil {
			config.cachedParams = map[string]string{}
		}
		if accountID != "" {
			config.cachedParams["account_id"] = accountID
		}
	}
}

func withClientOption(setter func(baseClient *client.BaseClient)) driverOption {
	return func(config *ConnectorConfig) {
		if config.client != nil {
			if clientImpl, ok := config.client.(*client.ClientImpl); ok {
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
			config.client = cl
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
	return func(config *ConnectorConfig) {
		WithAccountID(accountID)(config)
		WithToken(token)(config)
		WithUserAgent(userAgent)(config)
	}
}

// WithAccountName defines account name for the driver
func WithAccountName(accountName string) driverOptionWithError {
	return func(config *ConnectorConfig) error {
		if config.client != nil {
			if clientImpl, ok := config.client.(*client.ClientImpl); ok {
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
			config.client = cl
		}
		return nil
	}
}

// WithDatabaseAndEngineName defines database name and engine name for the driver
func WithDatabaseAndEngineName(databaseName, engineName string) driverOptionWithError {
	return func(config *ConnectorConfig) error {
		if config.client == nil {
			return errors.New("client must be initialized before setting database and engine name")
		}
		var err error
		config.engineUrl, config.cachedParams, err = config.client.GetConnectionParameters(context.TODO(), engineName, databaseName)
		if err != nil {
			return err
		}
		return nil
	}
}

// FireboltConnectorWithOptions builds a custom connector
func FireboltConnectorWithOptions(opts ...driverOption) *FireboltConnector {
	config := &ConnectorConfig{}

	for _, opt := range opts {
		opt(config)
	}

	return &FireboltConnector{
		config.engineUrl,
		config.client,
		config.cachedParams,
		&FireboltDriver{},
	}
}

// FireboltConnectorWithOptionsWithErrors builds a custom connector with error handling
func FireboltConnectorWithOptionsWithErrors(opts ...driverOptionWithError) (*FireboltConnector, error) {
	config := &ConnectorConfig{}

	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, err
		}
	}

	return &FireboltConnector{
		config.engineUrl,
		config.client,
		config.cachedParams,
		&FireboltDriver{},
	}, nil
}
