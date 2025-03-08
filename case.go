package n1qlizer

import (
	"bytes"
)

// CaseBuilder builds SQL CASE expressions.
type CaseBuilder interface {
	N1qlizer
	When(condition interface{}, value interface{}) CaseBuilder
	Else(value interface{}) CaseBuilder
}

// searchedCaseBuilder builds SQL CASE expressions without an initial value
// e.g. "CASE WHEN a = b THEN 'foo' WHEN a = c THEN 'bar' ELSE 'baz' END"
type searchedCaseBuilder struct {
	whenParts []whenPart
	elsePart  interface{}
}

// NewCaseBuilder creates a new CaseBuilder for building CASE expressions
func NewCaseBuilder() CaseBuilder {
	return &searchedCaseBuilder{}
}

// NewCaseBuilderWithValue creates a new CaseBuilder for building CASE expressions
// with an initial value. e.g. "CASE a WHEN 'foo' THEN 1 WHEN 'bar' THEN 2 ELSE 3 END"
func NewCaseBuilderWithValue(value interface{}) CaseBuilder {
	return &simpleCaseBuilder{value: value}
}

type whenPart struct {
	condition interface{}
	value     interface{}
}

// When adds a WHEN ... THEN ... clause to the CASE expression.
func (b *searchedCaseBuilder) When(condition interface{}, value interface{}) CaseBuilder {
	b.whenParts = append(b.whenParts, whenPart{
		condition: condition,
		value:     value,
	})
	return b
}

// Else adds an ELSE clause to the CASE expression.
func (b *searchedCaseBuilder) Else(value interface{}) CaseBuilder {
	b.elsePart = value
	return b
}

// ToN1ql implements the N1qlizer interface.
func (b *searchedCaseBuilder) ToN1ql() (sql string, args []interface{}, err error) {
	buf := &bytes.Buffer{}

	buf.WriteString("CASE")

	for _, w := range b.whenParts {
		buf.WriteString(" WHEN ")
		switch c := w.condition.(type) {
		case N1qlizer:
			csql, cargs, err := c.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(csql)
			args = append(args, cargs...)
		case string:
			// Treat strings as SQL with placeholders
			buf.WriteString(c)
			if v, ok := w.value.(string); ok {
				args = append(args, v)
				buf.WriteString(" THEN ?")
				continue
			}
		default:
			buf.WriteString("?")
			args = append(args, w.condition)
		}

		buf.WriteString(" THEN ")
		switch v := w.value.(type) {
		case N1qlizer:
			vsql, vargs, err := v.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(vsql)
			args = append(args, vargs...)
		case string:
			// String values should be treated as placeholders
			buf.WriteString("?")
			args = append(args, v)
		default:
			buf.WriteString("?")
			args = append(args, w.value)
		}
	}

	if b.elsePart != nil {
		buf.WriteString(" ELSE ")
		switch e := b.elsePart.(type) {
		case N1qlizer:
			esql, eargs, err := e.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(esql)
			args = append(args, eargs...)
		case string:
			// String values should be treated as placeholders
			buf.WriteString("?")
			args = append(args, e)
		default:
			buf.WriteString("?")
			args = append(args, b.elsePart)
		}
	}

	buf.WriteString(" END")
	return buf.String(), args, nil
}

// simpleCaseBuilder builds SQL CASE expressions with an initial value
// e.g. "CASE a WHEN 'foo' THEN 1 WHEN 'bar' THEN 2 ELSE 3 END"
type simpleCaseBuilder struct {
	value     interface{}
	whenParts []whenPart
	elsePart  interface{}
}

// When adds a WHEN ... THEN ... clause to the CASE expression.
func (b *simpleCaseBuilder) When(condition interface{}, value interface{}) CaseBuilder {
	b.whenParts = append(b.whenParts, whenPart{
		condition: condition,
		value:     value,
	})
	return b
}

// Else adds an ELSE clause to the CASE expression.
func (b *simpleCaseBuilder) Else(value interface{}) CaseBuilder {
	b.elsePart = value
	return b
}

// ToN1ql implements the N1qlizer interface.
func (b *simpleCaseBuilder) ToN1ql() (sql string, args []interface{}, err error) {
	buf := &bytes.Buffer{}

	buf.WriteString("CASE ")
	switch v := b.value.(type) {
	case N1qlizer:
		vsql, vargs, err := v.ToN1ql()
		if err != nil {
			return "", nil, err
		}
		buf.WriteString(vsql)
		args = append(args, vargs...)
	case string:
		// For the case value, we'll keep it as a column name
		buf.WriteString(v)
	default:
		buf.WriteString("?")
		args = append(args, b.value)
	}

	for _, w := range b.whenParts {
		buf.WriteString(" WHEN ")
		switch c := w.condition.(type) {
		case N1qlizer:
			csql, cargs, err := c.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(csql)
			args = append(args, cargs...)
		case string:
			// For WHEN conditions in simple CASE, treat as values
			buf.WriteString("?")
			args = append(args, c)
		default:
			buf.WriteString("?")
			args = append(args, w.condition)
		}

		buf.WriteString(" THEN ")
		switch v := w.value.(type) {
		case N1qlizer:
			vsql, vargs, err := v.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(vsql)
			args = append(args, vargs...)
		case string:
			// String values should be treated as placeholders
			buf.WriteString("?")
			args = append(args, v)
		default:
			buf.WriteString("?")
			args = append(args, w.value)
		}
	}

	if b.elsePart != nil {
		buf.WriteString(" ELSE ")
		switch e := b.elsePart.(type) {
		case N1qlizer:
			esql, eargs, err := e.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			buf.WriteString(esql)
			args = append(args, eargs...)
		case string:
			// String values should be treated as placeholders
			buf.WriteString("?")
			args = append(args, e)
		default:
			buf.WriteString("?")
			args = append(args, b.elsePart)
		}
	}

	buf.WriteString(" END")
	return buf.String(), args, nil
}
