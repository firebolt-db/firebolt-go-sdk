package fireboltgosdk

import (
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
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

	utils.AssertEqual(conn.engineUrl, engineUrl, t, "engineUrl is invalid")
	utils.AssertEqual(conn.cachedParameters["database"], databaseName, t, "databaseName is invalid")

	cl, ok := conn.client.(*ClientImpl)
	utils.AssertEqual(ok, true, t, "client is not *ClientImpl")

	connectionAccountID := conn.cachedParameters["account_id"]
	utils.AssertEqual(connectionAccountID, accountID, t, "accountID is invalid")
	utils.AssertEqual(cl.UserAgent, userAgent, t, "userAgent is invalid")

	tok, err := cl.accessTokenGetter()
	if err != nil {
		t.Errorf("token getter returned an error: %v", err)
	}

	utils.AssertEqual(tok, token, t, "token getter returned wrong token")
}
