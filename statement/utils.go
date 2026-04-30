package statement

import "database/sql/driver"

func valueToNamedValue(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		// Ordinal is 1-based per driver.NamedValue
		namedValues = append(namedValues, driver.NamedValue{Ordinal: i + 1, Value: arg})
	}
	return namedValues
}
