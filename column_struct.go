package fireboltgosdk

import (
	"errors"
	"fmt"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// structArrayColumn buffers values for an ARRAY(STRUCT(...)) column.
//
// In Parquet this is one repeated group containing one leaf per struct field, so
// the column spans several leaves rather than one. Rather than compute
// repetition and definition levels for the group directly, each field is backed
// by an ordinary arrayColumn: every field array is appended in lockstep, so they
// share an element count per row, which is exactly the shape a repeated group
// encodes. That reuses the array level logic already in use for ARRAY(T) instead
// of writing a second implementation of it.
//
// Struct fields must be scalars. Firebolt accepts deeper nesting
// (ARRAY(STRUCT(... ARRAY(T) ...))), but a nested repeated field inside a
// repeated group needs a second repetition level, and that is a materially
// harder encoding to get right. Semi-structured payloads should use a JSON
// field inside the struct instead.
type structArrayColumn struct {
	colName  string
	fields   []string
	fieldSet map[string]struct{}
	elems    []*arrayColumn
}

var errNullStructArray = errors.New("cannot store a null struct array: " +
	"the encoding cannot distinguish it from an empty array, pass an empty slice for []")

// newStructArrayColumn builds a column for array(struct(name type, ...)).
func newStructArrayColumn(colName string, fields []structField) (*structArrayColumn, error) {
	if len(fields) == 0 {
		return nil, fmt.Errorf("struct must have at least one field")
	}

	c := &structArrayColumn{
		colName: colName,
		fields:  make([]string, len(fields)),
		elems:   make([]*arrayColumn, len(fields)),
	}

	seen := make(map[string]struct{}, len(fields))
	for i, f := range fields {
		if _, dup := seen[f.name]; dup {
			return nil, fmt.Errorf("duplicate struct field %q", f.name)
		}
		seen[f.name] = struct{}{}

		elem, err := newColumnFromType("", f.typ)
		if err != nil {
			return nil, fmt.Errorf("struct field %q: %w", f.name, err)
		}
		// Nullability is orthogonal to shape, so unwrap before checking: a
		// nullable nested array is still a nested array.
		switch inner := unwrapNullable(elem).(type) {
		case *arrayColumn:
			return nil, fmt.Errorf("struct field %q: nested arrays inside a struct are not supported", f.name)
		case *structArrayColumn:
			return nil, fmt.Errorf("struct field %q: nested structs are not supported", f.name)
		default:
			_ = inner
		}

		c.fields[i] = f.name
		c.elems[i] = &arrayColumn{colName: f.name, elem: elem}
	}
	c.fieldSet = seen

	return c, nil
}

// unwrapNullable returns the column a nullable wrapper contains, or the column
// itself when it is not nullable.
func unwrapNullable(c column) column {
	if nc, ok := c.(*nullableColumn); ok {
		return nc.inner
	}
	return c
}

// structArrayFields recognises array(struct(...)) and returns its fields.
//
// Nullability is tolerated in the type because that is how the engine reports
// it: `array(struct(ts timestamp null, ...) null) null`. Nullable fields are
// encoded explicitly; an unrepresentable SQL NULL for the whole array is
// rejected by appendRow.
func structArrayFields(fireboltType string) ([]structField, bool, error) {
	t := trimNull(fireboltType)
	if !strings.HasPrefix(t, "array(") || !strings.HasSuffix(t, ")") {
		return nil, false, nil
	}
	elem := trimNull(t[len("array(") : len(t)-1])
	if !strings.HasPrefix(elem, "struct(") || !strings.HasSuffix(elem, ")") {
		return nil, false, nil
	}
	fields, err := parseStructFields(elem[len("struct(") : len(elem)-1])
	if err != nil {
		return nil, true, err
	}
	return fields, true, nil
}

// trimNull removes a trailing nullability marker and surrounding space.
func trimNull(s string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(s), " null"))
}

// structField is one field of a struct type.
type structField struct {
	name string
	typ  string
}

func (c *structArrayColumn) name() string { return c.colName }

func (c *structArrayColumn) rows() int { return c.elems[0].rows() }

// appendRow buffers one row's worth of struct elements.
//
// The accepted form is a slice of maps, one map per element, keyed by field
// name. Every declared field must be present, and unknown fields are rejected.
//
// Requiring every key is deliberate: a missing key usually means a misspelling,
// and silently treating it as null would hide the caller's mistake. Nullable
// fields accept an explicit nil; non-nullable fields reject it rather than
// inventing a zero value that reads as real.
func (c *structArrayColumn) appendRow(v interface{}) error { // NOSONAR - validation and atomic field buffering belong to one operation.
	elements, err := toStructElements(v)
	if err != nil {
		return err
	}
	for ei, elem := range elements {
		for field := range elem {
			if _, ok := c.fieldSet[field]; !ok {
				return fmt.Errorf("struct field %q is unknown in element %d", field, ei)
			}
		}
	}

	// Appending is all-or-nothing. Fields are separate Parquet leaves, so a
	// half-applied row leaves them at different lengths: rows() follows the
	// first field only, block.validate can still pass when this is the only
	// column, and the writer then panics indexing a shorter offsets slice.
	before := make([]int, len(c.elems))
	for i, e := range c.elems {
		before[i] = e.rows()
	}
	rollback := func() {
		for i, e := range c.elems {
			e.truncate(before[i])
		}
	}

	// Build one slice per field, then hand each to its array column so the
	// existing per-row offset bookkeeping stays the single source of truth.
	for fi, field := range c.fields {
		// Firebolt requires struct fields to be nullable -- it rejects
		// STRUCT(x TEXT NOT NULL) outright -- so this is the normal case, not
		// an edge one.
		_, nullable := c.elems[fi].elem.(*nullableColumn)

		values := make([]interface{}, len(elements))
		for ei, elem := range elements {
			v, ok := elem[field]
			if !ok {
				rollback()
				return fmt.Errorf("struct field %q is missing in element %d; "+
					"every field must be present, use a nil value for null", field, ei)
			}
			if v == nil && !nullable {
				rollback()
				return fmt.Errorf("struct field %q is nil in element %d, "+
					"but the field is not nullable", field, ei)
			}
			values[ei] = v
		}
		if err := c.elems[fi].appendRow(values); err != nil {
			rollback()
			return fmt.Errorf("struct field %q: %w", field, err)
		}
	}
	return nil
}

func (c *structArrayColumn) appendColumn(v interface{}) error {
	return appendColumnFallback(c, v)
}

// appendZero appends an empty array, which is how a repeated group represents
// "no elements".
func (c *structArrayColumn) appendZero() {
	for _, e := range c.elems {
		e.appendZero()
	}
}

// truncate drops rows after the first n from every field.
func (c *structArrayColumn) truncate(n int) {
	for _, e := range c.elems {
		e.truncate(n)
	}
}

func (c *structArrayColumn) reset() {
	for _, e := range c.elems {
		e.reset()
	}
}

// parquetNode returns the repeated group. Field nodes are the element nodes of
// the backing arrays, not the arrays themselves — the repetition lives on the
// group.
func (c *structArrayColumn) parquetNode() parquet.Node {
	group := make(parquet.Group, len(c.fields))
	for i, field := range c.fields {
		group[field] = c.elems[i].elem.parquetNode()
	}
	return parquet.Repeated(group)
}

// parquetValues is unused for struct columns: a single leaf index cannot
// describe a column that spans several leaves. The block reads leaves()
// instead.
func (c *structArrayColumn) parquetValues(int) []parquet.Value { return nil }

// leaves returns one entry per struct field, in field order.
func (c *structArrayColumn) leaves() []columnLeaf {
	out := make([]columnLeaf, len(c.fields))
	for i, field := range c.fields {
		elem := c.elems[i]
		// A nullable field contributes its own definition level on top of the
		// group's, which the backing array column does not account for. A
		// required field does not, and bumping it would exceed the leaf's
		// maximum definition level.
		var bump int
		if _, nullable := elem.elem.(*nullableColumn); nullable {
			bump = 1
		}
		out[i] = columnLeaf{
			path: []string{c.colName, field},
			values: func(colIdx int) []parquet.Value {
				return liftIntoGroup(elem.parquetValues(colIdx), elem.offsets, bump)
			},
			// Every leaf is repeated, and all fields share an element count per
			// row, so any field's offsets describe the whole group.
			offsets: func() []uint64 { return elem.offsets },
		}
	}
	return out
}

// liftIntoGroup raises the definition level of element values by bump, because
// the backing arrayColumn computes levels for a bare repeated leaf while these
// leaves sit inside a repeated group.
//
// For a leaf in `repeated group g { optional f }` the levels are:
//
//	empty g               → def 0
//	element, f is null    → def 1
//	element, f is present → def 2
//
// The array column emits 0/0/1, so element values need one more level while the
// marker a row contributes when it has no elements is left alone. A required
// field needs no adjustment: its leaf maximum is 1, which is what the array
// column already produces.
//
// Getting this wrong writes data that reads back as NULL while the element count
// still looks correct, which is why it is asserted against both a Parquet reader
// and a live engine.
func liftIntoGroup(values []parquet.Value, offsets []uint64, bump int) []parquet.Value {
	if bump == 0 {
		return values
	}
	out := make([]parquet.Value, 0, len(values))
	pos := 0
	var prev uint64
	for _, end := range offsets {
		if end == prev {
			// No elements: one value carrying the empty-group marker.
			out = append(out, values[pos])
			pos++
			continue
		}
		for range int(end - prev) {
			v := values[pos]
			// Only a present value moves up. A null element stays where the
			// array column put it, which is the level that means "element
			// present, field null" once inside the group. Bumping it too would
			// make it indistinguishable from a present value.
			d := v.DefinitionLevel()
			if !v.IsNull() {
				d += bump
			}
			out = append(out, v.Level(v.RepetitionLevel(), d, v.Column()))
			pos++
		}
		prev = end
	}
	return out
}

// toStructElements normalizes the accepted input shapes into a slice of maps.
func toStructElements(v interface{}) ([]map[string]interface{}, error) {
	switch vals := v.(type) {
	case nil:
		return nil, errNullStructArray
	case []map[string]interface{}:
		return vals, nil
	case []interface{}:
		out := make([]map[string]interface{}, len(vals))
		for i, e := range vals {
			m, ok := e.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("element %d is %T, want map[string]interface{}", i, e)
			}
			out[i] = m
		}
		return out, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to a struct array; "+
			"pass []map[string]interface{} with one map per element", v)
	}
}

// parseStructFields parses the field list of a struct type string, e.g.
// `struct(ts timestampntz, name text)`.
//
// Splitting is depth-aware so a field type containing commas — array(text), or
// a nested struct — does not split in the wrong place. Nested types are
// rejected later, by newStructArrayColumn, so that the error names the field.
func parseStructFields(inner string) ([]structField, error) {
	parts, err := splitTopLevel(inner)
	if err != nil {
		return nil, err
	}

	fields := make([]structField, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty struct field in %q", inner)
		}
		name, typ, err := parseStructField(part)
		if err != nil {
			return nil, err
		}
		fields = append(fields, structField{
			name: name,
			typ:  typ,
		})
	}
	return fields, nil
}

// parseStructField splits one `name type` pair. Firebolt quotes field names
// containing spaces or punctuation with backticks in type metadata.
func parseStructField(part string) (string, string, error) {
	if strings.HasPrefix(part, "`") {
		end := strings.IndexRune(part[1:], '`')
		if end < 0 {
			return "", "", fmt.Errorf("unterminated quoted struct field in %q", part)
		}
		end++ // Index in part rather than part[1:].
		name := part[1:end]
		typ := strings.TrimSpace(part[end+1:])
		if name == "" || typ == "" {
			return "", "", fmt.Errorf("struct field %q must be 'name type'", part)
		}
		return name, typ, nil
	}

	sp := strings.IndexFunc(part, func(r rune) bool { return r == ' ' || r == '\t' })
	if sp <= 0 {
		return "", "", fmt.Errorf("struct field %q must be 'name type'", part)
	}
	name := strings.TrimSpace(part[:sp])
	typ := strings.TrimSpace(part[sp+1:])
	if typ == "" {
		return "", "", fmt.Errorf("struct field %q must be 'name type'", part)
	}
	return name, typ, nil
}

// splitTopLevel splits on commas that are not inside parentheses.
func splitTopLevel(s string) ([]string, error) { // NOSONAR - quote and nesting states are one parser state machine.
	var (
		parts  []string
		depth  int
		start  int
		quoted bool
	)
	for i, r := range s {
		switch r {
		case '`':
			quoted = !quoted
		case '(':
			if !quoted {
				depth++
			}
		case ')':
			if !quoted {
				depth--
				if depth < 0 {
					return nil, fmt.Errorf("unbalanced parentheses in %q", s)
				}
			}
		case ',':
			if depth == 0 && !quoted {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if quoted {
		return nil, fmt.Errorf("unterminated quoted identifier in %q", s)
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced parentheses in %q", s)
	}
	return append(parts, s[start:]), nil
}
