package fireboltgosdk

import (
	"testing"
)

func TestDriverOptions(t *testing.T) {
	engineUrl := "https://engine.url"
	databaseName := "database_name"
	accountID := "account-id"
	token := "1234567890"
	userAgent := "UA Test"

	conn := FireboltConnectorWithOptions(
		WithEngineUrl(engineUrl),
		WithDatabaseName(databaseName),
		WithClientParams(accountID, token, userAgent),
	)

	assert(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	assert(conn.databaseName, databaseName, t, "databaseName is invalid")

	cl, ok := conn.client.(*ClientImpl)
	assert(ok, true, t, "client is not *ClientImpl")

	assert(cl.AccountID, accountID, t, "accountID is invalid")
	assert(cl.UserAgent, userAgent, t, "userAgent is invalid")

	tok, err := cl.accessTokenGetter()
	if err != nil {
		t.Errorf("token getter returned an error: %v", err)
	}

	assert(tok, token, t, "token getter returned wrong token")
}
