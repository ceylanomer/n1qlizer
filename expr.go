package n1qlizer

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

type expr struct {
	sql  string
	args []any
}

// Expr builds an expression from a SQL fragment and arguments.
// The first argument should be a string, which may contain ? placeholders.
func Expr(sql any, args ...any) N1qlizer {
	sqlStr, ok := sql.(string)
	if !ok {
		// For expressions like Eq, Lt, etc. which are N1qlizer instances
		if n1qlizer, ok := sql.(N1qlizer); ok {
			return n1qlizer
		}

		// Handle non-string input, convert to string
		return expr{sql: fmt.Sprintf("%v", sql), args: args}
	}
	return expr{sql: sqlStr, args: args}
}

func (e expr) ToN1ql() (string, []any, error) {
	// Check if we have enough arguments for placeholders
	placeholderCount := strings.Count(e.sql, "?")
	if placeholderCount > len(e.args) {
		return "", nil, fmt.Errorf("expr: not enough arguments for placeholders")
	}

	// Check if the expr arguments contain N1qlizer instances
	simple := true
	for _, arg := range e.args {
		if _, ok := arg.(N1qlizer); ok {
			simple = false
			break
		}
	}

	// If no N1qlizer arguments, just return the SQL and args as-is
	if simple {
		return e.sql, e.args, nil
	}

	// Handle N1qlizer arguments by substituting their SQL and args
	buf := &strings.Builder{}
	newArgs := make([]any, 0, len(e.args))

	argPos := 0
	for i := 0; i < len(e.sql); i++ {
		if e.sql[i] != '?' {
			buf.WriteByte(e.sql[i])
			continue
		}

		if argPos >= len(e.args) {
			return "", nil, fmt.Errorf("expr: not enough arguments for placeholders")
		}

		arg := e.args[argPos]
		argPos++

		if n1qlizer, ok := arg.(N1qlizer); ok {
			nestedSQL, nestedArgs, err := n1qlizer.ToN1ql()
			if err != nil {
				return "", nil, err
			}

			buf.WriteString(nestedSQL)
			newArgs = append(newArgs, nestedArgs...)
		} else {
			buf.WriteString("?")
			newArgs = append(newArgs, arg)
		}
	}

	return buf.String(), newArgs, nil
}

// newPart creates a new Sqlizer from a simple string
func newPart(sql string) N1qlizer {
	return expr{sql: sql}
}

// aliasExpr helps build expressions involving aliases, like "table AS alias".
type aliasExpr struct {
	expr  N1qlizer
	alias string
}

// Alias allows a N1qlizer to alias itself with the "AS" keyword.
func Alias(expr N1qlizer, alias string) N1qlizer {
	return aliasExpr{expr: expr, alias: alias}
}

func (e aliasExpr) ToN1ql() (string, []any, error) {
	sql, args, err := e.expr.ToN1ql()
	if err != nil {
		return "", nil, err
	}

	return fmt.Sprintf("(%s) AS %s", sql, e.alias), args, nil
}

// Eq is an equality expression ("=").
type Eq map[string]any

func (eq Eq) ToN1ql() (sql string, args []any, err error) {
	if len(eq) == 0 {
		// Empty Eq needs to be handled separately
		return "", nil, nil
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(eq))
	for key := range eq {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	exprs := make([]string, 0, len(eq))
	for _, key := range keys {
		expr, eargs, err := equalityToN1ql(key, eq[key])
		if err != nil {
			return "", nil, err
		}

		exprs = append(exprs, expr)
		args = append(args, eargs...)
	}

	sql = strings.Join(exprs, " AND ")
	return
}

// NotEq is an inequality expression ("<>").
type NotEq map[string]any

func (neq NotEq) ToN1ql() (sql string, args []any, err error) {
	if len(neq) == 0 {
		// Empty NotEq needs to be handled separately
		return "", nil, nil
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(neq))
	for key := range neq {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	exprs := make([]string, 0, len(neq))
	for _, key := range keys {
		expr, eargs, err := inequalityToN1ql(key, neq[key])
		if err != nil {
			return "", nil, err
		}

		exprs = append(exprs, expr)
		args = append(args, eargs...)
	}

	sql = strings.Join(exprs, " AND ")
	return
}

// Lt is a less-than expression ("<").
type Lt map[string]any

func (lt Lt) ToN1ql() (sql string, args []any, err error) {
	return comparisonExpr(lt, "<")
}

// Lte is a less-than-or-equal expression ("<=").
type Lte map[string]any

func (lte Lte) ToN1ql() (sql string, args []any, err error) {
	return comparisonExpr(lte, "<=")
}

// Gt is a greater-than expression (">").
type Gt map[string]any

func (gt Gt) ToN1ql() (sql string, args []any, err error) {
	return comparisonExpr(gt, ">")
}

// Gte is a greater-than-or-equal expression (">=").
type Gte map[string]any

func (gte Gte) ToN1ql() (sql string, args []any, err error) {
	return comparisonExpr(gte, ">=")
}

// comparisonExpr is a helper function for creating comparison expressions.
func comparisonExpr(m map[string]any, op string) (sql string, args []any, err error) {
	if len(m) == 0 {
		return "", nil, nil
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	exprs := make([]string, 0, len(m))
	for _, key := range keys {
		expr, eargs, err := comparisonToN1ql(key, m[key], op)
		if err != nil {
			return "", nil, err
		}

		exprs = append(exprs, expr)
		args = append(args, eargs...)
	}

	sql = strings.Join(exprs, " AND ")
	return
}

// equalityToN1ql generates SQL and args for an equality condition.
func equalityToN1ql(key string, val any) (sql string, args []any, err error) {
	switch v := val.(type) {
	case nil:
		return fmt.Sprintf("%s IS NULL", key), args, nil
	case []any:
		if len(v) == 0 {
			return "1=0", args, nil
		}
		placeholders := make([]string, len(v))
		for i := range v {
			placeholders[i] = "?"
			args = append(args, v[i])
		}
		return fmt.Sprintf("%s IN (%s)", key, strings.Join(placeholders, ",")), args, nil
	case N1qlizer:
		vsql, vargs, err := v.ToN1ql()
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s = %s", key, vsql), vargs, nil
	default:
		return fmt.Sprintf("%s = ?", key), []any{val}, nil
	}
}

// inequalityToN1ql generates SQL and args for an inequality condition.
func inequalityToN1ql(key string, val any) (sql string, args []any, err error) {
	switch v := val.(type) {
	case nil:
		return fmt.Sprintf("%s IS NOT NULL", key), args, nil
	case []any:
		if len(v) == 0 {
			return "1=1", args, nil
		}
		placeholders := make([]string, len(v))
		for i := range v {
			placeholders[i] = "?"
			args = append(args, v[i])
		}
		return fmt.Sprintf("%s NOT IN (%s)", key, strings.Join(placeholders, ",")), args, nil
	case N1qlizer:
		vsql, vargs, err := v.ToN1ql()
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s <> %s", key, vsql), vargs, nil
	default:
		return fmt.Sprintf("%s <> ?", key), []any{val}, nil
	}
}

// comparisonToN1ql generates SQL and args for a comparative condition using an operator.
func comparisonToN1ql(key string, val any, op string) (sql string, args []any, err error) {
	if val == nil {
		return "", nil, fmt.Errorf("cannot use %s operator with NULL value", op)
	}

	switch v := val.(type) {
	case N1qlizer:
		vsql, vargs, err := v.ToN1ql()
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s %s", key, op, vsql), vargs, nil
	default:
		return fmt.Sprintf("%s %s ?", key, op), []any{val}, nil
	}
}

// And combines multiple expressions with the "AND" operator.
type And []N1qlizer

func (and And) ToN1ql() (string, []any, error) {
	return andOrToN1ql(and, "AND")
}

// Or combines multiple expressions with the "OR" operator.
type Or []N1qlizer

func (or Or) ToN1ql() (string, []any, error) {
	return andOrToN1ql(or, "OR")
}

// andOrToN1ql is a helper function for generating AND/OR expressions.
func andOrToN1ql(ex []N1qlizer, sep string) (sql string, args []any, err error) {
	if len(ex) == 0 {
		return "", nil, nil
	}

	if len(ex) == 1 {
		return ex[0].ToN1ql()
	}

	parts := make([]string, 0, len(ex))
	for _, e := range ex {
		s, a, err := e.ToN1ql()
		if err != nil {
			return "", nil, err
		}
		if s != "" {
			parts = append(parts, s)
			args = append(args, a...)
		}
	}

	if len(parts) == 0 {
		return "", args, nil
	}

	return fmt.Sprintf("(%s)", strings.Join(parts, fmt.Sprintf(" %s ", sep))), args, nil
}

// writePlaceholders generates placeholder syntax for the given count, separated by commas.
func writePlaceholders(w io.Writer, count int) error {
	for i := 0; i < count; i++ {
		if i > 0 {
			_, err := io.WriteString(w, ",")
			if err != nil {
				return err
			}
		}
		_, err := io.WriteString(w, "?")
		if err != nil {
			return err
		}
	}
	return nil
}
