package rows

import (
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/firebolt-db/firebolt-go-sdk/types"
	"github.com/shopspring/decimal"
)

type columnReaderTestCase struct {
	column             types.Column
	expectedName       string
	expectedType       reflect.Type
	expectedDBTypeName string
	expectedNullable   bool
	expectedLength     int64
	expectedPrecision  int64
	expectedScale      int64
}

var testCases = []columnReaderTestCase{
	{
		column:             types.Column{Name: "col_int", Type: "int"},
		expectedName:       "col_int",
		expectedType:       reflect.TypeOf(int32(0)),
		expectedDBTypeName: "int",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_int_null", Type: "int null"},
		expectedName:       "col_int_null",
		expectedType:       reflect.TypeOf(int32(0)),
		expectedDBTypeName: "int",
		expectedNullable:   true,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_integer", Type: "integer"},
		expectedName:       "col_integer",
		expectedType:       reflect.TypeOf(int32(0)),
		expectedDBTypeName: "integer",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_long", Type: "long"},
		expectedName:       "col_long",
		expectedType:       reflect.TypeOf(int64(0)),
		expectedDBTypeName: "long",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_bigint", Type: "bigint"},
		expectedName:       "col_bigint",
		expectedType:       reflect.TypeOf(int64(0)),
		expectedDBTypeName: "bigint",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_float", Type: "float"},
		expectedName:       "col_float",
		expectedType:       reflect.TypeOf(float32(0)),
		expectedDBTypeName: "float",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_double", Type: "double"},
		expectedName:       "col_double",
		expectedType:       reflect.TypeOf(float64(0)),
		expectedDBTypeName: "double",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_double_precision", Type: "double precision"},
		expectedName:       "col_double_precision",
		expectedType:       reflect.TypeOf(float64(0)),
		expectedDBTypeName: "double precision",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_text", Type: "text"},
		expectedName:       "col_text",
		expectedType:       reflect.TypeOf(""),
		expectedDBTypeName: "text",
		expectedNullable:   false,
		expectedLength:     math.MaxInt,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_date", Type: "date"},
		expectedName:       "col_date",
		expectedType:       reflect.TypeOf(time.Time{}),
		expectedDBTypeName: "date",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_timestamp", Type: "timestamp"},
		expectedName:       "col_timestamp",
		expectedType:       reflect.TypeOf(time.Time{}),
		expectedDBTypeName: "timestamp",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_timestamptz", Type: "timestamptz"},
		expectedName:       "col_timestamptz",
		expectedType:       reflect.TypeOf(time.Time{}),
		expectedDBTypeName: "timestamptz",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_boolean", Type: "boolean"},
		expectedName:       "col_boolean",
		expectedType:       reflect.TypeOf(true),
		expectedDBTypeName: "boolean",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_array", Type: "array(int)"},
		expectedName:       "col_array",
		expectedType:       reflect.TypeOf([]int32{}),
		expectedDBTypeName: "array(int)",
		expectedNullable:   false,
		expectedLength:     math.MaxInt,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_decimal", Type: "Decimal(14, 22)"},
		expectedName:       "col_decimal",
		expectedType:       reflect.TypeOf(decimal.Decimal{}),
		expectedDBTypeName: "Decimal(14, 22)",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  14,
		expectedScale:      22,
	},
	{
		column:             types.Column{Name: "col_numeric", Type: "numeric(17, 8) null"},
		expectedName:       "col_numeric",
		expectedType:       reflect.TypeOf(decimal.Decimal{}),
		expectedDBTypeName: "numeric(17, 8)",
		expectedNullable:   true,
		expectedLength:     -1,
		expectedPrecision:  17,
		expectedScale:      8,
	},
	{
		column:             types.Column{Name: "col_bytea", Type: "bytea"},
		expectedName:       "col_bytea",
		expectedType:       reflect.TypeOf([]byte{}),
		expectedDBTypeName: "bytea",
		expectedNullable:   false,
		expectedLength:     math.MaxInt,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
	{
		column:             types.Column{Name: "col_geography", Type: "geography"},
		expectedName:       "col_geography",
		expectedType:       reflect.TypeOf(""),
		expectedDBTypeName: "geography",
		expectedNullable:   false,
		expectedLength:     -1,
		expectedPrecision:  -1,
		expectedScale:      -1,
	},
}

func testColumns() []types.Column {
	columns := make([]types.Column, len(testCases))
	for i, tc := range testCases {
		columns[i] = tc.column
	}
	return columns
}

func TestColumnReader_Columns(t *testing.T) {
	c := &ColumnReader{}
	must(c.setColumns(testColumns()))

	for i, tc := range testCases {
		if got := c.Columns()[i]; got != tc.expectedName {
			t.Errorf("Expected column name %s, got %s for column %s", tc.expectedName, got, tc.column.Name)
		}
	}
}

func TestColumnReader_ColumnTypeScanType(t *testing.T) {
	c := &ColumnReader{}
	must(c.setColumns(testColumns()))

	for i, tc := range testCases {
		if got := c.ColumnTypeScanType(i); got != tc.expectedType {
			t.Errorf("Expected type %v, got %v for column %s", tc.expectedType, got, tc.column.Name)
		}
	}
}

func TestColumnReader_ColumnTypeDatabaseTypeName(t *testing.T) {
	c := &ColumnReader{}
	must(c.setColumns(testColumns()))

	for i, tc := range testCases {
		if got := c.ColumnTypeDatabaseTypeName(i); got != tc.expectedDBTypeName {
			t.Errorf("Expected database type %s, got %s for column %s", tc.expectedDBTypeName, got, tc.column.Name)
		}
	}
}

func TestColumnReader_ColumnTypeNullable(t *testing.T) {
	c := &ColumnReader{}
	must(c.setColumns(testColumns()))

	for i, tc := range testCases {
		if got, ok := c.ColumnTypeNullable(i); got != tc.expectedNullable || !ok {
			t.Errorf("Expected nullable %t, got %t for column %s", tc.expectedNullable, got, tc.column.Name)
		}
	}
}

func TestColumnReader_ColumnTypeLength(t *testing.T) {
	c := &ColumnReader{}
	must(c.setColumns(testColumns()))

	for i, tc := range testCases {
		expectedLength := tc.expectedLength
		expectedOk := true
		if tc.expectedLength == -1 {
			expectedLength = 0
			expectedOk = false
		}
		if got, ok := c.ColumnTypeLength(i); got != expectedLength || ok != expectedOk {
			t.Errorf("Expected length %d, ok %v, got length %d, ok %v for column %s", expectedLength, expectedOk, got, ok, tc.column.Name)
		}
	}
}

func TestColumnReader_ColumnTypePrecisionScale(t *testing.T) {
	c := &ColumnReader{}
	must(c.setColumns(testColumns()))

	for i, tc := range testCases {
		expectedPrecision := tc.expectedPrecision
		expectedScale := tc.expectedScale
		expectedOk := true
		if tc.expectedPrecision == -1 || tc.expectedScale == -1 {
			expectedPrecision = 0
			expectedScale = 0
			expectedOk = false
		}
		if gotPrecision, gotScale, ok := c.ColumnTypePrecisionScale(i); gotPrecision != expectedPrecision || gotScale != expectedScale || ok != expectedOk {
			t.Errorf(
				"Expected precision %d, scale %d, ok %v, got precision %d, scale %d, ok %v for column %s",
				expectedPrecision, expectedScale, expectedOk, gotPrecision, gotScale, ok, tc.column.Name,
			)
		}
	}
}
