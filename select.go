package n1qlizer

import (
	"bytes"
	"fmt"
	"strings"
)

// selectData stores the state of a SELECT query as it is built
type selectData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           QueryRunner
	Prefixes          []N1qlizer
	Options           []string
	Columns           []N1qlizer
	From              N1qlizer
	Joins             []N1qlizer
	WhereParts        []N1qlizer
	GroupBys          []string
	HavingParts       []N1qlizer
	OrderByParts      []N1qlizer
	Limit             string
	Offset            string
	Suffixes          []N1qlizer
	UseKeys           string
}

func (d *selectData) ToN1ql() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *selectData) toN1qlRaw() (sqlStr string, args []any, err error) {
	if len(d.Columns) == 0 {
		err = fmt.Errorf("select statements must have at least one result column")
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

	sql.WriteString("SELECT ")

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	if len(d.Columns) > 0 {
		args, err = buildClauses(d.Columns, sql, ", ", args)
		if err != nil {
			return
		}
	}

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = buildClauses([]N1qlizer{d.From}, sql, "", args)
		if err != nil {
			return
		}

		if d.UseKeys != "" {
			sql.WriteString(" USE KEYS ")
			sql.WriteString(d.UseKeys)
		}
	}

	if len(d.Joins) > 0 {
		sql.WriteString(" ")
		args, err = buildClauses(d.Joins, sql, " ", args)
		if err != nil {
			return
		}
	}

	if len(d.WhereParts) > 0 {
		sql.WriteString(" WHERE ")
		args, err = buildClauses(d.WhereParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.GroupBys) > 0 {
		sql.WriteString(" GROUP BY ")
		sql.WriteString(strings.Join(d.GroupBys, ", "))
	}

	if len(d.HavingParts) > 0 {
		sql.WriteString(" HAVING ")
		args, err = buildClauses(d.HavingParts, sql, " AND ", args)
		if err != nil {
			return
		}
	}

	if len(d.OrderByParts) > 0 {
		sql.WriteString(" ORDER BY ")
		args, err = buildClauses(d.OrderByParts, sql, ", ", args)
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

// SelectBuilder builds SELECT statements.
type SelectBuilder Builder

func init() {
	Register(SelectBuilder{}, selectData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b SelectBuilder) PlaceholderFormat(f PlaceholderFormat) SelectBuilder {
	return Set[SelectBuilder, PlaceholderFormat](b, "PlaceholderFormat", f)
}

// RunWith sets a Runner (like a Couchbase DB connection) to be used with e.g. Execute.
func (b SelectBuilder) RunWith(runner QueryRunner) SelectBuilder {
	return Set[SelectBuilder, QueryRunner](b, "RunWith", runner)
}

// Execute builds and executes the query.
func (b SelectBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(selectData)
	if data.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecuteWith(data.RunWith, b)
}

// ToN1ql builds the query into a N1QL string and bound args.
func (b SelectBuilder) ToN1ql() (string, []any, error) {
	data := GetStruct(b).(selectData)
	return data.ToN1ql()
}

// toN1qlRaw is used to generate N1QL for embedded usage in other queries.
func (b SelectBuilder) toN1qlRaw() (string, []any, error) {
	data := GetStruct(b).(selectData)
	return data.toN1qlRaw()
}

// MustN1ql builds the query into a N1QL string and bound args.
//
// MustN1ql panics if there are any errors.
func (b SelectBuilder) MustN1ql() (string, []any) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query
func (b SelectBuilder) Prefix(sql string, args ...any) SelectBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the very beginning of the query
func (b SelectBuilder) PrefixExpr(expr N1qlizer) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "Prefixes", expr)
}

// Distinct adds a DISTINCT clause to the query.
func (b SelectBuilder) Distinct() SelectBuilder {
	return b.Options("DISTINCT")
}

// Options adds options to the query.
func (b SelectBuilder) Options(options ...string) SelectBuilder {
	return Set[SelectBuilder, []string](b, "Options", options)
}

// Columns adds result columns to the query.
func (b SelectBuilder) Columns(columns ...string) SelectBuilder {
	parts := make([]N1qlizer, 0, len(columns))
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	return Set[SelectBuilder, []N1qlizer](b, "Columns", parts)
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the column string, for example:
//
//	.Column("IF(n_subscribers > ?, ?, ?)", 100, "HIGH", "LOW")
func (b SelectBuilder) Column(column any, args ...any) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "Columns", Expr(column, args...))
}

// From sets the FROM clause of the query.
func (b SelectBuilder) From(from string) SelectBuilder {
	return Set[SelectBuilder, N1qlizer](b, "From", newPart(from))
}

// UseKeys sets the USE KEYS clause of the query.
func (b SelectBuilder) UseKeys(keys string) SelectBuilder {
	return Set[SelectBuilder, string](b, "UseKeys", keys)
}

// FromSelect sets a subquery into the FROM clause of the query.
func (b SelectBuilder) FromSelect(from SelectBuilder, alias string) SelectBuilder {
	return Set[SelectBuilder, N1qlizer](b, "From", Alias(from, alias))
}

// JoinClause adds a join clause to the query.
func (b SelectBuilder) JoinClause(join string, args ...any) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "Joins", Expr(join, args...))
}

// Join adds a JOIN clause to the query.
func (b SelectBuilder) Join(join string, rest ...any) SelectBuilder {
	return b.JoinClause("JOIN "+join, rest...)
}

// LeftJoin adds a LEFT JOIN clause to the query.
func (b SelectBuilder) LeftJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("LEFT JOIN "+join, rest...)
}

// RightJoin adds a RIGHT JOIN clause to the query.
func (b SelectBuilder) RightJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("RIGHT JOIN "+join, rest...)
}

// InnerJoin adds an INNER JOIN clause to the query.
func (b SelectBuilder) InnerJoin(join string, rest ...any) SelectBuilder {
	return b.JoinClause("INNER JOIN "+join, rest...)
}

// Where adds an expression to the WHERE clause of the query.
func (b SelectBuilder) Where(pred any, args ...any) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "WhereParts", Expr(pred, args...))
}

// GroupBy adds GROUP BY expressions to the query.
func (b SelectBuilder) GroupBy(groupBys ...string) SelectBuilder {
	return Set[SelectBuilder, []string](b, "GroupBys", groupBys)
}

// Having adds an expression to the HAVING clause of the query.
func (b SelectBuilder) Having(pred any, rest ...any) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "HavingParts", Expr(pred, rest...))
}

// OrderBy adds ORDER BY expressions to the query.
func (b SelectBuilder) OrderBy(orderBys ...string) SelectBuilder {
	parts := make([]N1qlizer, 0, len(orderBys))
	for _, str := range orderBys {
		parts = append(parts, newPart(str))
	}
	return Set[SelectBuilder, []N1qlizer](b, "OrderByParts", parts)
}

// OrderByClause adds ORDER BY expressions to the query with a custom clause.
//
// This is a more flexible version of OrderBy, and can be used for complex
// expressions like "ORDER BY field ASC NULLS FIRST".
func (b SelectBuilder) OrderByClause(pred any, args ...any) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "OrderByParts", Expr(pred, args...))
}

// Limit sets a LIMIT clause on the query.
func (b SelectBuilder) Limit(limit uint64) SelectBuilder {
	return Set[SelectBuilder, string](b, "Limit", fmt.Sprintf("%d", limit))
}

// Offset sets an OFFSET clause on the query.
func (b SelectBuilder) Offset(offset uint64) SelectBuilder {
	return Set[SelectBuilder, string](b, "Offset", fmt.Sprintf("%d", offset))
}

// Suffix adds an expression to the end of the query
func (b SelectBuilder) Suffix(sql string, args ...any) SelectBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query
func (b SelectBuilder) SuffixExpr(expr N1qlizer) SelectBuilder {
	return Append[SelectBuilder, N1qlizer](b, "Suffixes", expr)
}
