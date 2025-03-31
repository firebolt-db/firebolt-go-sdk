package errors

var (
	AuthenticationError         = ConstructNestedError("authentication error", nil)
	QueryExecutionError         = ConstructNestedError("query execution error", nil)
	SystemEngineResolutionError = ConstructNestedError("error getting system engine URL", nil)
	DSNParseError               = ConstructNestedError("error parsing DSN", nil)
)
