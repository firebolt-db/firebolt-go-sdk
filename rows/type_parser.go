package rows

import (
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type fireboltType struct {
	goType     reflect.Type
	dbType     string
	isNullable bool
	length     int64
	precision  int64
	scale      int64
}

func makeFireboltType(goType reflect.Type, dbName string, length int64, isNullable bool) fireboltType {
	return fireboltType{goType: goType, dbType: dbName, isNullable: isNullable, length: length, precision: -1, scale: -1}
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
	return makeFireboltType(primitiveType, columnType, length, false), err
}

// parseNullablePrimitiveType parses the primitive type of the nullable column into the reflect.Type object
func parseNullablePrimitiveType(columnType string) (fireboltType, error) {
	var err error
	var primitiveType reflect.Type
	var length int64 = -1
	switch columnType {
	case intType, integerType:
		primitiveType = reflect.TypeOf(sql.NullInt32{})
	case longType, bigIntType:
		primitiveType = reflect.TypeOf(sql.NullInt64{})
	case floatType, realType, doubleType, doublePrecisionType:
		primitiveType = reflect.TypeOf(sql.NullFloat64{})
	case textType:
		primitiveType = reflect.TypeOf(sql.NullString{})
		length = math.MaxInt64
	case geographyType:
		primitiveType = reflect.TypeOf(sql.NullString{})
	case dateType, pgDateType, timestampType, timestampNtzType, timestampTzType:
		primitiveType = reflect.TypeOf(sql.NullTime{})
	case byteaType:
		primitiveType = reflect.TypeOf(NullBytes{})
		length = math.MaxInt64
	case booleanType:
		primitiveType = reflect.TypeOf(sql.NullBool{})
	default:
		err = fmt.Errorf("unknown column type: %s", columnType)
		primitiveType = reflect.TypeOf(nil)
	}
	return makeFireboltType(primitiveType, columnType, length, true), err
}

func parseTypeWithNullability(columnType string, isNullable bool) (fireboltType, error) {
	if strings.HasSuffix(columnType, nullableSuffix) {
		return parseTypeWithNullability(columnType[0:len(columnType)-len(nullableSuffix)], true)
	}
	if strings.HasPrefix(columnType, arrayPrefix) && strings.HasSuffix(columnType, complexTypeSuffix) {
		var arrayType reflect.Type
		if isNullable {
			arrayType = reflect.TypeOf(FireboltNullArray{})
		} else {
			arrayType = reflect.TypeOf(FireboltArray{})
		}
		return makeFireboltType(arrayType, columnType, math.MaxInt64, isNullable), nil
	} else if (strings.HasPrefix(columnType, decimalPrefix) || strings.HasPrefix(columnType, numericPrefix)) && strings.HasSuffix(columnType, complexTypeSuffix) {
		var decimalType reflect.Type
		if isNullable {
			decimalType = reflect.TypeOf(FireboltNullDecimal{})
		} else {
			decimalType = reflect.TypeOf(FireboltDecimal{})
		}
		res := makeFireboltType(decimalType, columnType, -1, isNullable)
		var err error
		var precisionScale string
		if strings.HasPrefix(columnType, decimalPrefix) {
			precisionScale = columnType[len(decimalPrefix) : len(columnType)-len(complexTypeSuffix)]
		} else {
			precisionScale = columnType[len(numericPrefix) : len(columnType)-len(complexTypeSuffix)]
		}
		res.precision, res.scale, err = parseDecimalPrecisionScale(precisionScale)
		return res, err

	} else if strings.HasPrefix(columnType, structPrefix) && strings.HasSuffix(columnType, complexTypeSuffix) {
		var structType reflect.Type
		if isNullable {
			structType = reflect.TypeOf(FireboltNullStruct{})
		} else {
			structType = reflect.TypeOf(FireboltNullStruct{})
		}
		return makeFireboltType(structType, columnType, -1, isNullable), nil
	}
	if isNullable {
		return parseNullablePrimitiveType(columnType)
	}
	return parsePrimitiveType(columnType)
}

// parseType parses the type of the column into the reflect.Type object
func parseType(columnType string) (fireboltType, error) {
	return parseTypeWithNullability(columnType, false)
}
