package errors

const accountErrorMsg = `provided account name does not exist in this organization or is not authorized.
Please verify the account name and make sure your service account has the
correct RBAC permissions and is linked to a user`

var (
	AuthenticationError = ConstructNestedError("authentication error", nil)
	AuthorizationError  = ConstructNestedError("authorization error", nil)
	QueryExecutionError = ConstructNestedError("query execution error", nil)
	DSNParseError       = ConstructNestedError("error parsing DSN", nil)
	InvalidAccountError = ConstructNestedError(accountErrorMsg, nil)
)
