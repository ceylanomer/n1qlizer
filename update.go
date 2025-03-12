package n1qlizer

import (
	"bytes"
	"fmt"
	"sort"
)

// updateData stores the state of an UPDATE query as it is built
type updateData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           QueryRunner
	Prefixes          []N1qlizer
	Table             string
	SetClauses        map[string]any
	WhereParts        []N1qlizer
	UseKeys           string
	Limit             string
	Offset            string
	Suffixes          []N1qlizer
}

func (d *updateData) ToN1ql() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *updateData) toN1qlRaw() (sqlStr string, args []any, err error) {
	if len(d.Table) == 0 {
		err = fmt.Errorf("update statements must specify a table")
		return
	}
	if len(d.SetClauses) == 0 {
		err = fmt.Errorf("update statements must have at least one Set clause")
		return
	}

	sql := &bytes.Buffer{}

	if len(d.Prefixes) > 0 {
		args, err = buildClauses(d.Prefixes, sql, " ", args)
		if err != nil {
			return
		}

		sql.WriteString(" ")
	}

	sql.WriteString("UPDATE ")
	sql.WriteString(d.Table)

	if d.UseKeys != "" {
		sql.WriteString(" USE KEYS ")
		sql.WriteString(d.UseKeys)
	}

	sql.WriteString(" SET ")

	// Sort the set clauses to ensure consistent output ordering
	setSql := make([]string, 0, len(d.SetClauses))
	for col := range d.SetClauses {
		setSql = append(setSql, col)
	}
	sort.Strings(setSql)

	for i, col := range setSql {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(col)
		sql.WriteString(" = ")

		value := d.SetClauses[col]
		if n1ql, ok := value.(N1qlizer); ok {
			vsql, vargs, err := n1ql.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			sql.WriteString(vsql)
			args = append(args, vargs...)
		} else {
			sql.WriteString("?")
			args = append(args, value)
		}
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = buildClauses(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.Limit) > 0 {
		sql.WriteString(" LIMIT ")
		sql.WriteString(d.Limit)
	}

	if len(d.Offset) > 0 {
		sql.WriteString(" OFFSET ")
		sql.WriteString(d.Offset)
	}

	if len(d.Suffixes) > 0 {
		sql.WriteString(" ")
		args, err = buildClauses(d.Suffixes, sql, " ", args)
		if err != nil {
			return
		}
	}

	sqlStr = sql.String()
	return
}

// UpdateBuilder builds UPDATE statements.
type UpdateBuilder Builder

func init() {
	Register(UpdateBuilder{}, updateData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) UpdateBuilder {
	return Set[UpdateBuilder, PlaceholderFormat](b, "PlaceholderFormat", f)
}

// RunWith sets a Runner (like a Couchbase DB connection) to be used with e.g. Execute.
func (b UpdateBuilder) RunWith(runner QueryRunner) UpdateBuilder {
	return Set[UpdateBuilder, QueryRunner](b, "RunWith", runner)
}

// Execute builds and executes the query.
func (b UpdateBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(updateData)
	if data.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecuteWith(data.RunWith, b)
}

// ToN1ql builds the query into a N1QL string and bound args.
func (b UpdateBuilder) ToN1ql() (string, []any, error) {
	data := GetStruct(b).(updateData)
	return data.ToN1ql()
}

// MustN1ql builds the query into a N1QL string and bound args.
//
// MustN1ql panics if there are any errors.
func (b UpdateBuilder) MustN1ql() (string, []any) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b UpdateBuilder) Prefix(sql string, args ...any) UpdateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the beginning of the query
func (b UpdateBuilder) PrefixExpr(expr N1qlizer) UpdateBuilder {
	return Append[UpdateBuilder, N1qlizer](b, "Prefixes", expr)
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return Set[UpdateBuilder, string](b, "Table", table)
}

// UseKeys sets the USE KEYS clause of the query.
func (b UpdateBuilder) UseKeys(keys string) UpdateBuilder {
	return Set[UpdateBuilder, string](b, "UseKeys", keys)
}

// Set adds SET clauses to the query.
func (b UpdateBuilder) Set(column string, value any) UpdateBuilder {
	data := GetStruct(b).(updateData)
	if data.SetClauses == nil {
		data.SetClauses = make(map[string]any)
	}
	data.SetClauses[column] = value
	return Set[UpdateBuilder, map[string]any](b, "SetClauses", data.SetClauses)
}

// SetMap is a convenience method which calls .Set for each key/value pair in clauses.
func (b UpdateBuilder) SetMap(clauses map[string]any) UpdateBuilder {
	data := GetStruct(b).(updateData)
	if data.SetClauses == nil {
		data.SetClauses = make(map[string]any)
	}
	for k, v := range clauses {
		data.SetClauses[k] = v
	}
	return Set[UpdateBuilder, map[string]any](b, "SetClauses", data.SetClauses)
}

// Where adds WHERE expressions to the query.
func (b UpdateBuilder) Where(pred any, args ...any) UpdateBuilder {
	return Append[UpdateBuilder, N1qlizer](b, "WhereParts", Expr(pred, args...))
}

// Limit sets a LIMIT clause on the query.
func (b UpdateBuilder) Limit(limit uint64) UpdateBuilder {
	return Set[UpdateBuilder, string](b, "Limit", fmt.Sprintf("%d", limit))
}

// Offset sets a OFFSET clause on the query.
func (b UpdateBuilder) Offset(offset uint64) UpdateBuilder {
	return Set[UpdateBuilder, string](b, "Offset", fmt.Sprintf("%d", offset))
}

// Suffix adds an expression to the end of the query
func (b UpdateBuilder) Suffix(sql string, args ...any) UpdateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b UpdateBuilder) SuffixExpr(expr N1qlizer) UpdateBuilder {
	return Append[UpdateBuilder, N1qlizer](b, "Suffixes", expr)
}
