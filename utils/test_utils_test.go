package utils

import "testing"

func TestParseWKBPoint(t *testing.T) {
	cases := []struct {
		name    string
		value   interface{}
		x, y    float64
		wantErr bool
	}{
		{name: "EWKB with SRID", value: "0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F", x: 0.9999999999999998, y: 1},
		{name: "EWKB from newer engine", value: "0101000020E6100000FDFFFFFFFFFFEF3F000000000000F03F", x: 0.9999999999999997, y: 1},
		{name: "plain WKB", value: "0101000000000000000000F03F000000000000F03F", x: 1, y: 1},
		{name: "big endian WKB", value: "00000000013FF00000000000004000000000000000", x: 1, y: 2},
		{name: "not a string", value: 5, wantErr: true},
		{name: "not hex", value: "zz", wantErr: true},
		{name: "linestring", value: "0102000000", wantErr: true},
		{name: "truncated point", value: "0101000000000000000000F03F", wantErr: true},
		{name: "invalid byte order", value: "0201000000000000000000F03F000000000000F03F", wantErr: true},
		{name: "3D EWKB point", value: "01010000A0E6100000000000000000F03F000000000000F03F0000000000001440", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			x, y, errMsg := parseWKBPoint(c.value)
			if c.wantErr {
				if errMsg == "" {
					t.Errorf("expected an error, got point (%v, %v)", x, y)
				}
				return
			}
			if errMsg != "" {
				t.Fatalf("unexpected error: %s", errMsg)
			}
			if x != c.x || y != c.y {
				t.Errorf("expected (%v, %v), got (%v, %v)", c.x, c.y, x, y)
			}
		})
	}
}

func TestAssertGeographyPointToleratesEngineRounding(t *testing.T) {
	AssertGeographyPoint("0101000020E6100000FEFFFFFFFFFFEF3F000000000000F03F", 1, 1, t, "")
	AssertGeographyPoint("0101000020E6100000FDFFFFFFFFFFEF3F000000000000F03F", 1, 1, t, "")
}
