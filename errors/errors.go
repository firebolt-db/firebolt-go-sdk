package errors

var (
	AuthenticationError = ConstructNestedError("authentication error", nil)
)
