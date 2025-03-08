package n1qlizer

import (
	"bytes"
	"fmt"
)

// deleteData stores the state of a DELETE query as it is built
type deleteData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           QueryRunner
	Prefixes          []N1qlizer
	From              string
	WhereParts        []N1qlizer
	UseKeys           string
	Limit             string
	Offset            string
	Suffixes          []N1qlizer
}

func (d *deleteData) ToN1ql() (sqlStr string, args []interface{}, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *deleteData) toN1qlRaw() (sqlStr string, args []interface{}, err error) {
	if len(d.From) == 0 {
		err = fmt.Errorf("delete statements must specify a table")
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

	sql.WriteString("DELETE FROM ")
	sql.WriteString(d.From)

	if d.UseKeys != "" {
		sql.WriteString(" USE KEYS ")
		sql.WriteString(d.UseKeys)
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

// DeleteBuilder builds SQL DELETE statements.
type DeleteBuilder Builder

func init() {
	Register(DeleteBuilder{}, deleteData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Dollar) for the query.
func (b DeleteBuilder) PlaceholderFormat(f PlaceholderFormat) DeleteBuilder {
	return Set(b, "PlaceholderFormat", f).(DeleteBuilder)
}

// RunWith sets a Runner (like Couchbase DB connection) to be used with e.g. Execute.
func (b DeleteBuilder) RunWith(runner QueryRunner) DeleteBuilder {
	return Set(b, "RunWith", runner).(DeleteBuilder)
}

// Execute builds and sends the query to the runner set by RunWith.
func (b DeleteBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(deleteData)

	if data.RunWith == nil {
		return nil, RunnerNotSet
	}

	query, args, err := data.ToN1ql()
	if err != nil {
		return nil, err
	}

	return data.RunWith.Execute(query, args...)
}

// ToN1ql builds the query into a N1QL string and args.
func (b DeleteBuilder) ToN1ql() (string, []interface{}, error) {
	data := GetStruct(b).(deleteData)
	return data.ToN1ql()
}

// MustN1ql builds the query into a N1QL string and args, and panics on error.
func (b DeleteBuilder) MustN1ql() (string, []interface{}) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b DeleteBuilder) Prefix(sql string, args ...interface{}) DeleteBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the beginning of the query.
func (b DeleteBuilder) PrefixExpr(expr N1qlizer) DeleteBuilder {
	return Append(b, "Prefixes", expr).(DeleteBuilder)
}

// From sets the table to delete from.
func (b DeleteBuilder) From(from string) DeleteBuilder {
	return Set(b, "From", from).(DeleteBuilder)
}

// UseKeys sets the USE KEYS clause of the query.
func (b DeleteBuilder) UseKeys(keys string) DeleteBuilder {
	return Set(b, "UseKeys", keys).(DeleteBuilder)
}

// Where adds an expression to the WHERE clause of the query.
func (b DeleteBuilder) Where(pred interface{}, args ...interface{}) DeleteBuilder {
	return Append(b, "WhereParts", Expr(pred, args...)).(DeleteBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b DeleteBuilder) Limit(limit uint64) DeleteBuilder {
	return Set(b, "Limit", fmt.Sprintf("%d", limit)).(DeleteBuilder)
}

// Offset sets an OFFSET clause on the query.
func (b DeleteBuilder) Offset(offset uint64) DeleteBuilder {
	return Set(b, "Offset", fmt.Sprintf("%d", offset)).(DeleteBuilder)
}

// Suffix adds an expression to the end of the query.
func (b DeleteBuilder) Suffix(sql string, args ...interface{}) DeleteBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b DeleteBuilder) SuffixExpr(expr N1qlizer) DeleteBuilder {
	return Append(b, "Suffixes", expr).(DeleteBuilder)
}
