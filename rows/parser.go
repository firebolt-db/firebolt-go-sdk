package rows

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/errors"
	"github.com/shopspring/decimal"
)

const (
	intType    = "int"
	longType   = "long"
	floatType  = "float"
	doubleType = "double"

	textType = "text"

	dateType   = "date"
	pgDateType = "pgdate"

	timestampType    = "timestamp"
	timestampNtzType = "timestampntz"
	timestampTzType  = "timestamptz"

	booleanType = "boolean"
	byteaType   = "bytea"

	geographyType = "geography"

	// Alternative names for the same types
	integerType         = "integer"
	bigIntType          = "bigint"
	realType            = "real"
	doublePrecisionType = "double precision"

	// Prefixes and suffixes for complex types
	nullableSuffix = " null"
	arrayPrefix    = "array("
	decimalPrefix  = "Decimal("
	numericPrefix  = "numeric("
	structPrefix   = "struct("
	suffix         = ")"
)

// isFloatingPointPrimitiveType checks if the columnType is a floating point type
func isFloatingPointPrimitiveType(columnType string) bool {
	switch columnType {
	case floatType, realType, doubleType, doublePrecisionType:
		return true
	}
	return false
}

// checkTypeValue checks that val type could be changed to columnType
func checkTypeValue(columnType string, val interface{}) error {
	switch columnType {
	case intType, integerType, longType, bigIntType, floatType, realType, doubleType, doublePrecisionType:
		if _, ok := val.(float64); !ok {
			if isFloatingPointPrimitiveType(columnType) {
				for _, v := range []string{"inf", "-inf", "nan", "-nan"} {
					if val == v {
						return nil
					}
				}
			}
			// Allow string values for long columns
			if _, is_str := val.(string); !((columnType == longType || columnType == bigIntType) && is_str) {
				return fmt.Errorf("expected to convert a value to long, but couldn't: %v", val)
			}
		}
		return nil
	case textType, dateType, pgDateType, timestampType, timestampNtzType, timestampTzType, byteaType, geographyType:
		if _, ok := val.(string); !ok {
			return fmt.Errorf("expected to convert a value to string, but couldn't: %v", val)
		}
		return nil
	case booleanType:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("expected to convert a value to bool, but couldn't: %v", val)
		}
		return nil

	}
	return fmt.Errorf("unknown column type: %s", columnType)
}

func extractStructColumn(columnType string) (string, string, error) {
	columnType = strings.TrimSpace(columnType)
	if idx := strings.IndexRune(columnType[1:], '`'); strings.HasPrefix(columnType, "`") && idx != -1 {
		// We use idx+2 since we found this index in the substring starting from the second character
		return strings.Trim(columnType[1:idx+2], " `"), strings.TrimSpace(columnType[idx+2:]), nil
	}
	field := strings.SplitN(strings.TrimSpace(columnType), " ", 2)
	if len(field) < 2 {
		return "", "", fmt.Errorf("invalid struct field: %s", columnType)
	}
	return strings.TrimSpace(field[0]), strings.TrimSpace(field[1]), nil
}

func extractStructColumns(columnTypes string) (map[string]string, error) {
	balance := 0
	current := strings.Builder{}
	columns := make(map[string]string)
	for _, char := range columnTypes {
		if char == '(' {
			balance++
		} else if char == ')' {
			balance--
		}
		if balance == 0 && char == ',' {
			fieldName, fieldType, err := extractStructColumn(current.String())
			if err != nil {
				return nil, err
			}
			columns[fieldName] = fieldType
			current.Reset()
		} else {
			current.WriteRune(char)
		}
	}
	if balance != 0 {
		return nil, fmt.Errorf("invalid struct type: %s", columnTypes)
	}
	fieldName, fieldType, err := extractStructColumn(current.String())
	if err != nil {
		return nil, err
	}
	columns[fieldName] = fieldType
	return columns, nil
}

func parseStruct(structInnerFields string, val interface{}) (map[string]driver.Value, error) {
	fields, err := extractStructColumns(structInnerFields)
	if err != nil {
		return nil, errors.ConstructNestedError("error during parsing struct type", err)
	}
	structValue, ok := val.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected value for struct type: %v", val)
	}
	res := make(map[string]driver.Value)
	if len(fields) != len(structValue) {
		return nil, fmt.Errorf("expected %d fields, but got %d", len(fields), len(structValue))
	}
	for fieldName, fieldType := range fields {
		if fieldValue, ok := structValue[fieldName]; ok {
			res[fieldName], err = parseValue(fieldType, fieldValue)
			if err != nil {
				return nil, errors.ConstructNestedError("error during parsing struct field", err)
			}
		} else {
			return nil, fmt.Errorf("field %s is missing in struct value %v", fieldName, structValue)
		}
	}
	return res, nil
}

func ParseTimestampTz(value string) (driver.Value, error) {
	formats := [...]string{"2006-01-02 15:04:05.000000-07", "2006-01-02 15:04:05.000000-07:00", "2006-01-02 15:04:05.000000-07:00:00",
		"2006-01-02 15:04:05-07", "2006-01-02 15:04:05-07:00", "2006-01-02 15:04:05-07:00:00"}
	var res time.Time
	var err error
	for _, format := range formats {
		res, err = time.Parse(format, value)
		if err == nil {
			break
		}
	}
	return res, err
}

// parseDateTimeValue parses different date types
func parseDateTimeValue(columnType string, value string) (driver.Value, error) {
	switch columnType {
	case dateType, pgDateType:
		return time.Parse("2006-01-02", value)
	case timestampType:
		// Go doesn't use yyyy-mm-dd layout. Instead, it uses the value: Mon Jan 2 15:04:05 MST 2006
		return time.Parse("2006-01-02 15:04:05", value)
	case timestampNtzType:
		return time.Parse("2006-01-02 15:04:05.000000", value)
	case timestampTzType:
		return ParseTimestampTz(value)
	}
	return nil, fmt.Errorf("type not known: %s", columnType)
}

func parseFloatValue(val interface{}) (float64, error) {
	if _, notNum := val.(string); notNum {
		switch val.(string) {
		case "inf":
			return math.Inf(1), nil
		case "-inf":
			return math.Inf(-1), nil
		case "nan":
			return math.NaN(), nil
		case "-nan":
			return math.NaN(), nil
		default:
			return 0, fmt.Errorf("unknown float value: %s", val)
		}
	}
	return val.(float64), nil
}

// parseSingleValue parses all columns types except arrays
func parseSingleValue(columnType string, val interface{}) (driver.Value, error) {
	if err := checkTypeValue(columnType, val); err != nil {
		return nil, errors.ConstructNestedError("error during value parsing", err)
	}

	switch columnType {
	case intType, integerType:
		return int32(val.(float64)), nil
	case longType, bigIntType:
		// long values as passed as strings by system engine
		if unpacked, ok := val.(float64); ok {
			return int64(unpacked), nil
		}
		return strconv.ParseInt(val.(string) /*base*/, 10 /*bitSize*/, 64)
	case floatType, realType:
		v, err := parseFloatValue(val)
		return float32(v), err
	case doubleType, doublePrecisionType:
		return parseFloatValue(val)
	case textType, geographyType:
		return val.(string), nil
	case dateType, pgDateType, timestampType, timestampNtzType, timestampTzType:
		return parseDateTimeValue(columnType, val.(string))
	case booleanType:
		return val.(bool), nil
	case byteaType:
		trimmedString := strings.TrimPrefix(val.(string), "\\x")
		decoded, err := hex.DecodeString(trimmedString)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse to hex value: %v", val)
		}
		return decoded, nil
	}

	return nil, fmt.Errorf("type not known: %s", columnType)
}

func parseDecimalValue(val interface{}) (driver.Value, error) {
	var decimalValue decimal.Decimal
	var err error
	switch val := val.(type) {
	case string:
		decimalValue, err = decimal.NewFromString(val)
	case float32:
		decimalValue = decimal.NewFromFloat(float64(val))
	case float64:
		decimalValue = decimal.NewFromFloat(val)
	default:
		return nil, fmt.Errorf("unable to parse decimal value: %v", val)
	}

	if err != nil {
		return nil, fmt.Errorf("unable to parse decimal value: %v", val)
	} else {
		return decimalValue, nil
	}
}

// parseValue treating the val according to the column type and casts it to one of the go native types:
// uint8, uint32, uint64, int32, int64, float32, float64, string, Time or []driver.Value for arrays
func parseValue(columnType string, val interface{}) (driver.Value, error) {
	// No need to parse type if the value is nil
	if val == nil {
		return nil, nil
	}

	if strings.HasPrefix(columnType, arrayPrefix) && strings.HasSuffix(columnType, suffix) {
		s := reflect.ValueOf(val)
		res := make([]driver.Value, s.Len())

		for i := 0; i < s.Len(); i++ {
			res[i], _ = parseValue(columnType[len(arrayPrefix):len(columnType)-len(suffix)], s.Index(i).Interface())
		}
		return res, nil
	} else if (strings.HasPrefix(columnType, decimalPrefix) || strings.HasPrefix(columnType, numericPrefix)) && strings.HasSuffix(columnType, suffix) {
		return parseDecimalValue(val)
	} else if strings.HasPrefix(columnType, structPrefix) && strings.HasSuffix(columnType, suffix) {
		return parseStruct(columnType[len(structPrefix):len(columnType)-len(suffix)], val)
	} else if strings.HasSuffix(columnType, nullableSuffix) {
		return parseValue(columnType[0:len(columnType)-len(nullableSuffix)], val)
	}

	return parseSingleValue(columnType, val)
}

type fireboltType struct {
	goType     reflect.Type
	dbName     string
	isNullable bool
	length     int64
	precision  int64
	scale      int64
}

func makeFireboltType(goType reflect.Type, dbName string, length int64) fireboltType {
	return fireboltType{goType: goType, dbName: dbName, isNullable: false, length: length, precision: -1, scale: -1}
}

func parseDecimalPrecisionScale(precisionScale string) (int64, int64, error) {
	parts := strings.Split(precisionScale, ",")
	if len(parts) != 2 {
		return -1, -1, fmt.Errorf("invalid decimal precision/scale: %s", precisionScale)
	}
	precision, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return -1, -1, fmt.Errorf("invalid decimal precision: %s", parts[0])
	}
	scale, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return -1, -1, fmt.Errorf("invalid decimal scale: %s", parts[1])
	}
	return precision, scale, nil
}

// parsePrimitiveType parses the primitive type of the column into the reflect.Type object
func parsePrimitiveType(columnType string) (fireboltType, error) {
	var err error
	var primitiveType reflect.Type
	var length int64 = -1
	switch columnType {
	case intType, integerType:
		primitiveType = reflect.TypeOf(int32(0))
	case longType, bigIntType:
		primitiveType = reflect.TypeOf(int64(0))
	case floatType, realType:
		primitiveType = reflect.TypeOf(float32(0))
	case doubleType, doublePrecisionType:
		primitiveType = reflect.TypeOf(float64(0))
	case textType:
		primitiveType = reflect.TypeOf("")
		length = math.MaxInt64
	case geographyType:
		primitiveType = reflect.TypeOf("")
	case dateType, pgDateType, timestampType, timestampNtzType, timestampTzType:
		primitiveType = reflect.TypeOf(time.Time{})
	case byteaType:
		primitiveType = reflect.TypeOf([]byte{})
		length = math.MaxInt64
	case booleanType:
		primitiveType = reflect.TypeOf(false)
	default:
		err = fmt.Errorf("unknown column type: %s", columnType)
		primitiveType = reflect.TypeOf(nil)
	}
	return makeFireboltType(primitiveType, columnType, length), err
}

// parseType parses the type of the column into the reflect.Type object
func parseType(columnType string) (fireboltType, error) {

	if strings.HasPrefix(columnType, arrayPrefix) && strings.HasSuffix(columnType, suffix) {
		innerType, err := parseType(columnType[len(arrayPrefix) : len(columnType)-len(suffix)])
		if err != nil {
			return makeFireboltType(reflect.TypeOf(nil), columnType, -1), err
		}
		return makeFireboltType(reflect.SliceOf(innerType.goType), columnType, math.MaxInt64), nil
	} else if (strings.HasPrefix(columnType, decimalPrefix) || strings.HasPrefix(columnType, numericPrefix)) && strings.HasSuffix(columnType, suffix) {
		res := makeFireboltType(reflect.TypeOf(decimal.Decimal{}), columnType, -1)
		var err error
		var precisionScale string
		if strings.HasPrefix(columnType, decimalPrefix) {
			precisionScale = columnType[len(decimalPrefix) : len(columnType)-len(suffix)]
		} else {
			precisionScale = columnType[len(numericPrefix) : len(columnType)-len(suffix)]
		}
		res.precision, res.scale, err = parseDecimalPrecisionScale(precisionScale)
		return res, err

	} else if strings.HasPrefix(columnType, structPrefix) && strings.HasSuffix(columnType, suffix) {
		return makeFireboltType(reflect.TypeOf(map[string]interface{}{}), columnType, -1), nil
	} else if strings.HasSuffix(columnType, nullableSuffix) {
		res, err := parseType(columnType[0 : len(columnType)-len(nullableSuffix)])
		res.isNullable = true
		return res, err

	}
	return parsePrimitiveType(columnType)
}
