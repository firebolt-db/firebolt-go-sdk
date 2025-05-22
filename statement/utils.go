package statement

import "database/sql/driver"

func valueToNamedValue(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, 0, len(args))
	for i, arg := range args {
		namedValues = append(namedValues, driver.NamedValue{Ordinal: i, Value: arg})
	}
	return namedValues
}
