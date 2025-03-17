package rows

import (
	"errors"
	"math/big"

	"github.com/shopspring/decimal"
)

type FireboltDecimal struct {
	decimal.Decimal
}

// Decompose extracts the decimal value into form, sign, coefficient, and exponent.
func (fd *FireboltDecimal) Decompose(buf []byte) (byte, bool, []byte, int32) {
	// From golang's spec: (finite=0, infinite=1, NaN=2)
	form := byte(0)
	negative := fd.Sign() < 0
	absCoefficient := fd.Coefficient()
	coefficientBytesLen := (absCoefficient.BitLen() + 7) / 8 // Required bytes to represent the coefficient
	var coefficientBuff []byte
	if cap(buf) < coefficientBytesLen {
		coefficientBuff = make([]byte, coefficientBytesLen)
	} else {
		coefficientBuff = buf[:coefficientBytesLen]
	}
	absCoefficient.FillBytes(coefficientBuff)
	exponent := fd.Exponent()

	return form, negative, coefficientBuff, exponent
}

// Compose constructs an ExtendedDecimal from the given form, sign, coefficient, and exponent.
func (fd *FireboltDecimal) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	if form != 0 {
		return errors.New("unsupported form: only finite values are supported")
	}
	intCoefficient := new(big.Int).SetBytes(coefficient)
	if negative {
		intCoefficient.Neg(intCoefficient)
	}
	fd.Decimal = decimal.NewFromBigInt(intCoefficient, exponent)
	return nil
}

type FireboltNullDecimal struct {
	decimal.NullDecimal
}

// Decompose extracts the decimal value into form, sign, coefficient, and exponent.
func (fnd *FireboltNullDecimal) Decompose(buf []byte) (byte, bool, []byte, int32) {
	if !fnd.Valid {
		return 2, false, nil, 0
	}
	return (&FireboltDecimal{Decimal: fnd.Decimal}).Decompose(buf)
}

// Compose constructs an ExtendedDecimal from the given form, sign, coefficient, and exponent.
func (fnd *FireboltNullDecimal) Compose(form byte, negative bool, coefficient []byte, exponent int32) error {
	if form == 2 {
		fnd.Valid = false
		return nil
	}
	fd := &FireboltDecimal{}
	err := fd.Compose(form, negative, coefficient, exponent)
	if err != nil {
		return err
	}
	fnd.Valid = true
	fnd.Decimal = fd.Decimal
	return nil
}

// GetDecimal returns the decimal value if it is valid, otherwise returns nil.
func (fnd FireboltNullDecimal) GetDecimal() *decimal.Decimal {
	if fnd.Valid {
		return &fnd.Decimal
	}
	return nil
}
