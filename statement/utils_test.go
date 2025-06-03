package statement

import (
	"database/sql/driver"
	"testing"

	"github.com/firebolt-db/firebolt-go-sdk/utils"
)

func TestValueToNamedValue(t *testing.T) {
	utils.AssertEqual(len(valueToNamedValue([]driver.Value{})), 0, t, "valueToNamedValue of empty array is wrong")

	namedValues := valueToNamedValue([]driver.Value{2, "string"})
	utils.AssertEqual(len(namedValues), 2, t, "len of namedValues is wrong")
	utils.AssertEqual(namedValues[0].Value, 2, t, "namedValues value is wrong")
	utils.AssertEqual(namedValues[1].Value, "string", t, "namedValues value is wrong")
}
