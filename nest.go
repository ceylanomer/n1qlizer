package n1qlizer

import (
	"fmt"
)

// NestClause represents a NEST clause in a N1QL query
type NestClause struct {
	bucket    string
	alias     string
	onKeys    string
	condition N1qlizer
}

// ToN1ql implements the N1qlizer interface
func (n NestClause) ToN1ql() (string, []interface{}, error) {
	var result string
	var args []interface{}

	result = fmt.Sprintf("NEST %s", n.bucket)

	if n.alias != "" {
		result += fmt.Sprintf(" AS %s", n.alias)
	}

	if n.onKeys != "" {
		result += fmt.Sprintf(" ON KEYS %s", n.onKeys)
	}

	if n.condition != nil {
		sql, condArgs, err := n.condition.ToN1ql()
		if err != nil {
			return "", nil, err
		}

		if sql != "" {
			result += fmt.Sprintf(" ON %s", sql)
			args = append(args, condArgs...)
		}
	}

	return result, args, nil
}

// Nest creates a new NEST clause for joining with nested sub-documents
func Nest(bucket string) NestClause {
	return NestClause{bucket: bucket}
}

// As sets the alias for the nested bucket
func (n NestClause) As(alias string) NestClause {
	n.alias = alias
	return n
}

// OnKeys sets the ON KEYS expression for the NEST clause
func (n NestClause) OnKeys(keys string) NestClause {
	n.onKeys = keys
	return n
}

// On sets the ON condition for the NEST clause
func (n NestClause) On(condition interface{}, args ...interface{}) NestClause {
	switch c := condition.(type) {
	case string:
		n.condition = Expr(c, args...)
	case N1qlizer:
		n.condition = c
	default:
		// Handle the error appropriately for your use case
		n.condition = Expr(fmt.Sprintf("%v", condition), args...)
	}
	return n
}

// UnnestClause represents an UNNEST clause in a N1QL query
type UnnestClause struct {
	path      string
	alias     string
	condition N1qlizer
}

// ToN1ql implements the N1qlizer interface
func (u UnnestClause) ToN1ql() (string, []interface{}, error) {
	var result string
	var args []interface{}

	result = fmt.Sprintf("UNNEST %s", u.path)

	if u.alias != "" {
		result += fmt.Sprintf(" AS %s", u.alias)
	}

	if u.condition != nil {
		sql, condArgs, err := u.condition.ToN1ql()
		if err != nil {
			return "", nil, err
		}

		if sql != "" {
			result += fmt.Sprintf(" ON %s", sql)
			args = append(args, condArgs...)
		}
	}

	return result, args, nil
}

// Unnest creates a new UNNEST clause for flattening array fields
func Unnest(path string) UnnestClause {
	return UnnestClause{path: path}
}

// As sets the alias for the unnested array
func (u UnnestClause) As(alias string) UnnestClause {
	u.alias = alias
	return u
}

// On sets the ON condition for the UNNEST clause
func (u UnnestClause) On(condition interface{}, args ...interface{}) UnnestClause {
	switch c := condition.(type) {
	case string:
		u.condition = Expr(c, args...)
	case N1qlizer:
		u.condition = c
	default:
		// Handle the error appropriately for your use case
		u.condition = Expr(fmt.Sprintf("%v", condition), args...)
	}
	return u
}

// LeftNestClause represents a LEFT NEST clause in a N1QL query
type LeftNestClause struct {
	nestClause NestClause
}

// ToN1ql implements the N1qlizer interface
func (ln LeftNestClause) ToN1ql() (string, []interface{}, error) {
	sql, args, err := ln.nestClause.ToN1ql()
	if err != nil {
		return "", nil, err
	}

	return "LEFT " + sql, args, nil
}

// LeftNest creates a new LEFT NEST clause for outer joining with nested sub-documents
func LeftNest(bucket string) LeftNestClause {
	return LeftNestClause{nestClause: NestClause{bucket: bucket}}
}

// As sets the alias for the nested bucket
func (ln LeftNestClause) As(alias string) LeftNestClause {
	ln.nestClause = ln.nestClause.As(alias)
	return ln
}

// OnKeys sets the ON KEYS expression for the LEFT NEST clause
func (ln LeftNestClause) OnKeys(keys string) LeftNestClause {
	ln.nestClause = ln.nestClause.OnKeys(keys)
	return ln
}

// On sets the ON condition for the LEFT NEST clause
func (ln LeftNestClause) On(condition interface{}, args ...interface{}) LeftNestClause {
	ln.nestClause = ln.nestClause.On(condition, args...)
	return ln
}

// LeftUnnestClause represents a LEFT UNNEST clause in a N1QL query
type LeftUnnestClause struct {
	unnestClause UnnestClause
}

// ToN1ql implements the N1qlizer interface
func (lu LeftUnnestClause) ToN1ql() (string, []interface{}, error) {
	sql, args, err := lu.unnestClause.ToN1ql()
	if err != nil {
		return "", nil, err
	}

	return "LEFT " + sql, args, nil
}

// LeftUnnest creates a new LEFT UNNEST clause for outer flattening of array fields
func LeftUnnest(path string) LeftUnnestClause {
	return LeftUnnestClause{unnestClause: UnnestClause{path: path}}
}

// As sets the alias for the unnested array
func (lu LeftUnnestClause) As(alias string) LeftUnnestClause {
	lu.unnestClause = lu.unnestClause.As(alias)
	return lu
}

// On sets the ON condition for the LEFT UNNEST clause
func (lu LeftUnnestClause) On(condition interface{}, args ...interface{}) LeftUnnestClause {
	lu.unnestClause = lu.unnestClause.On(condition, args...)
	return lu
}

// SelectBuilder methods to support NEST and UNNEST

// Nest adds a NEST clause to the query
func (b SelectBuilder) Nest(bucket string) SelectBuilder {
	return b.NestClause(Nest(bucket))
}

// NestClause adds a NEST clause to the query
func (b SelectBuilder) NestClause(nest NestClause) SelectBuilder {
	return Append(b, "Joins", nest).(SelectBuilder)
}

// LeftNest adds a LEFT NEST clause to the query
func (b SelectBuilder) LeftNest(bucket string) SelectBuilder {
	return b.LeftNestClause(LeftNest(bucket))
}

// LeftNestClause adds a LEFT NEST clause to the query
func (b SelectBuilder) LeftNestClause(nest LeftNestClause) SelectBuilder {
	return Append(b, "Joins", nest).(SelectBuilder)
}

// Unnest adds an UNNEST clause to the query
func (b SelectBuilder) Unnest(path string) SelectBuilder {
	return b.UnnestClause(Unnest(path))
}

// UnnestClause adds an UNNEST clause to the query
func (b SelectBuilder) UnnestClause(unnest UnnestClause) SelectBuilder {
	return Append(b, "Joins", unnest).(SelectBuilder)
}

// LeftUnnest adds a LEFT UNNEST clause to the query
func (b SelectBuilder) LeftUnnest(path string) SelectBuilder {
	return b.LeftUnnestClause(LeftUnnest(path))
}

// LeftUnnestClause adds a LEFT UNNEST clause to the query
func (b SelectBuilder) LeftUnnestClause(unnest LeftUnnestClause) SelectBuilder {
	return Append(b, "Joins", unnest).(SelectBuilder)
}
