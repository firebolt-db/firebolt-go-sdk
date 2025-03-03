package client

const (
	ServiceAccountLoginURLSuffix = "/oauth/token"
	EngineUrlByAccountName       = "/web/v3/account/%s/engineUrl"
	//API v0
	UsernamePasswordURLSuffix  = "/auth/v1/login"
	DefaultAccountURL          = "/iam/v2/account"
	AccountIdByNameURL         = "/iam/v2/accounts:getIdByName"
	EngineIdByNameURL          = "/core/v1/accounts/%s/engines:getIdByName"
	EngineByIdURL              = "/core/v1/accounts/%s/engines/%s"
	EngineUrlByDatabaseNameURL = "/core/v1/accounts/%s/engines:getURLByDatabaseName"
)
