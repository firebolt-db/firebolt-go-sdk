package client

import (
	"context"
)

// MockClient rudimentary mocks Client and tracks the parameters passed to Query
type MockClient struct {
	ParametersCalled []map[string]string
	errorToRaise     error
}

func MakeMockClient() *MockClient {
	return &MockClient{errorToRaise: nil}
}

func MakeMockClientWithError(errorToRaise error) *MockClient {
	return &MockClient{errorToRaise: errorToRaise}
}

func (m *MockClient) Query(ctx context.Context, engineUrl, query string, parameters map[string]string, control ConnectionControl) (*Response, error) {
	m.ParametersCalled = append(m.ParametersCalled, parameters)
	return nil, m.errorToRaise
}

func (m *MockClient) GetConnectionParameters(ctx context.Context, engineName string, databaseName string) (string, map[string]string, error) {
	// Implement to satisfy Client interface
	return "", nil, nil
}
