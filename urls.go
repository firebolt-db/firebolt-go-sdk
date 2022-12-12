package fireboltgosdk

const (
	UsernamePasswordURLSuffix    = "/auth/v1/login"
	ServiceAccountLoginURLSuffix = "/auth/v1/token"
	DefaultAccountURL            = "/iam/v2/account"
	AccountIdByNameURL           = "/iam/v2/accounts:getIdByName"
	EngineIdByNameURL            = "/core/v1/accounts/%s/engines:getIdByName"
	EngineByIdURL                = "/core/v1/accounts/%s/engines/%s"
	EngineUrlByDatabaseNameURL   = "/core/v1/accounts/%s/engines:getURLByDatabaseName"
)
