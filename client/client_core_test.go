package client

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	contextUtils "github.com/firebolt-db/firebolt-go-sdk/context"
	errorUtils "github.com/firebolt-db/firebolt-go-sdk/errors"
)

func clientFactoryCore(url string) Client {
	var client = &ClientImplCore{
		BaseClient: BaseClient{ApiEndpoint: url},
	}
	client.AccessTokenGetter = client.getAccessToken
	client.ParameterGetter = client.GetQueryParams
	err := initialiseCaches()
	if err != nil {
		log.Printf("Error while initializing caches: %s", err)
	}
	return client
}

func TestAsyncQueryCore(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// We don't expect the client to get to this point
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	prepareEnvVariablesForTest(t, server)
	client := clientFactoryCore(server.URL)

	ctx := contextUtils.WithAsync(context.Background())

	_, err := client.Query(ctx, server.URL, selectOne, map[string]string{}, ConnectionControl{})
	if err == nil {
		t.Error("Expected an error when executing async query without an async handler")
	}
	if !errors.Is(err, errorUtils.AsyncNotSupportedError) {
		t.Errorf("Expected AsyncNotSupportedError, got: %v", err)
	}
}
