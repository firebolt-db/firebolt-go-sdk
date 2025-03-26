package rows

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// NullBytes represents a []byte that may be null.
// NullBytes implements the Scanner interface so
// it can be used as a scan destination.
type NullBytes struct {
	Bytes []byte
	Valid bool // Valid is true if Bytes is not NULL
}

// Scan implements the Scanner interface.
func (n *NullBytes) Scan(value interface{}) error {
	if value == nil {
		n.Bytes = nil
		n.Valid = false
		return nil
	}

	switch v := value.(type) {
	case []byte:
		n.Bytes = make([]byte, len(v))
		copy(n.Bytes, v)
		n.Valid = true
		return nil
	case string:
		trimmedString := strings.TrimPrefix(v, "\\x")
		decoded, err := hex.DecodeString(trimmedString)
		if err != nil {
			return fmt.Errorf("unable to parse hex value: %v", v)
		}
		n.Bytes = decoded
		n.Valid = true
		return nil
	}

	return fmt.Errorf("cannot scan type %T into NullableBytes", value)
}
