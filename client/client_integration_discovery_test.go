//go:build integration_discovery
// +build integration_discovery

package client

import (
	"context"

	"github.com/firebolt-db/firebolt-go-sdk/types"
)

var (
	engineUrlMock string
	databaseMock  string
	clientMock    *ClientImplDiscovery
)

func init() {
	databaseMock = "integration_test_db"
	client, err := ClientFactory(&types.FireboltSettings{
		Database:             databaseMock,
		DiscoveryEndpoint:    "http://localhost:3473",
		SSLMode:              "none",
		NewVersion:           true,
		ConnectionParameters: map[string]string{"database": databaseMock},
	}, GetHostNameURL())
	if err != nil {
		panic(err)
	}

	clientMock = client.(*ClientImplDiscovery)
	engineUrl, _, err := clientMock.GetConnectionParameters(context.Background(), "", databaseMock)
	if err != nil {
		panic(err)
	}
	engineUrlMock = engineUrl
}
