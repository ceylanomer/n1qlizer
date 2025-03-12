package n1qlizer

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
)

// analyticsSelectData stores the state of an Analytics SELECT query as it is built
type analyticsSelectData struct {
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
	LetsClause        map[string]N1qlizer // Maps variable names to their values
	Window            string
	Suffixes          []N1qlizer
}

func (d *analyticsSelectData) ToN1ql() (sqlStr string, args []any, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *analyticsSelectData) toN1qlRaw() (sqlStr string, args []any, err error) {
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

	if len(d.LetsClause) > 0 {
		sql.WriteString(" LET ")
		isFirst := true
		// sort keys for consistent output
		keys := make([]string, 0, len(d.LetsClause))
		for k := range d.LetsClause {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			expr := d.LetsClause[k]
			if !isFirst {
				sql.WriteString(", ")
			}
			isFirst = false

			sql.WriteString(k)
			sql.WriteString(" = ")

			exprSql, exprArgs, err := expr.ToN1ql()
			if err != nil {
				return "", nil, err
			}

			sql.WriteString(exprSql)
			args = append(args, exprArgs...)
		}
	}

	if d.From != nil {
		sql.WriteString(" FROM ")
		args, err = buildClauses([]N1qlizer{d.From}, sql, "", args)
		if err != nil {
			return
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

	if len(d.Window) > 0 {
		sql.WriteString(" WINDOW ")
		sql.WriteString(d.Window)
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

// AnalyticsSelectBuilder builds Couchbase Analytics SELECT statements.
type AnalyticsSelectBuilder Builder

func init() {
	Register(AnalyticsSelectBuilder{}, analyticsSelectData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Question or Dollar) for the
// query.
func (b AnalyticsSelectBuilder) PlaceholderFormat(f PlaceholderFormat) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, PlaceholderFormat](b, "PlaceholderFormat", f)
}

// RunWith sets a Runner (like a Couchbase DB connection) to be used with e.g. Execute.
func (b AnalyticsSelectBuilder) RunWith(runner QueryRunner) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, QueryRunner](b, "RunWith", runner)
}

// RunWithContext sets a QueryRunnerContext (like a Couchbase DB connection with context methods)
// to be used with e.g. ExecuteContext.
func (b AnalyticsSelectBuilder) RunWithContext(runner QueryRunnerContext) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, QueryRunner](b, "RunWith", runner)
}

// Execute builds and executes the query.
func (b AnalyticsSelectBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(analyticsSelectData)
	if data.RunWith == nil {
		return nil, RunnerNotSet
	}
	return ExecuteWith(data.RunWith, b)
}

// ExecuteContext builds and executes the query using the provided context.
func (b AnalyticsSelectBuilder) ExecuteContext(ctx context.Context) (QueryResult, error) {
	data := GetStruct(b).(analyticsSelectData)
	if data.RunWith == nil {
		return nil, RunnerNotSet
	}

	runner, ok := data.RunWith.(QueryRunnerContext)
	if !ok {
		return nil, RunnerNotQueryRunnerContext
	}

	return ExecuteContextWith(ctx, runner, b)
}

// ToN1ql builds the query into a N1QL string and bound args.
func (b AnalyticsSelectBuilder) ToN1ql() (string, []any, error) {
	data := GetStruct(b).(analyticsSelectData)
	return data.ToN1ql()
}

// MustN1ql builds the query into a N1QL string and bound args.
//
// MustN1ql panics if there are any errors.
func (b AnalyticsSelectBuilder) MustN1ql() (string, []any) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Columns adds result columns to the query.
func (b AnalyticsSelectBuilder) Columns(columns ...string) AnalyticsSelectBuilder {
	parts := make([]N1qlizer, 0, len(columns))
	for _, str := range columns {
		parts = append(parts, newPart(str))
	}
	return Set[AnalyticsSelectBuilder, []N1qlizer](b, "Columns", parts)
}

// Column adds a result column to the query.
// Unlike Columns, Column accepts args which will be bound to placeholders in
// the column string, for example:
//
//	.Column("IF(n_subscribers > ?, ?, ?)", 100, "HIGH", "LOW")
func (b AnalyticsSelectBuilder) Column(column any, args ...any) AnalyticsSelectBuilder {
	return Append[AnalyticsSelectBuilder, N1qlizer](b, "Columns", Expr(column, args...))
}

// From sets the FROM clause of the query.
func (b AnalyticsSelectBuilder) From(from string) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, N1qlizer](b, "From", newPart(from))
}

// Let adds a LET binding variable to the query.
func (b AnalyticsSelectBuilder) Let(variable string, value any) AnalyticsSelectBuilder {
	data := GetStruct(b).(analyticsSelectData)

	if data.LetsClause == nil {
		data.LetsClause = make(map[string]N1qlizer)
	}

	var expr N1qlizer
	switch v := value.(type) {
	case N1qlizer:
		expr = v
	default:
		expr = Expr("?", value)
	}

	data.LetsClause[variable] = expr
	return Set[AnalyticsSelectBuilder, map[string]N1qlizer](b, "LetsClause", data.LetsClause)
}

// Window sets the WINDOW clause for window functions.
func (b AnalyticsSelectBuilder) Window(windowClause string) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, string](b, "Window", windowClause)
}

// Where adds an expression to the WHERE clause of the query.
func (b AnalyticsSelectBuilder) Where(pred any, args ...any) AnalyticsSelectBuilder {
	return Append[AnalyticsSelectBuilder, N1qlizer](b, "WhereParts", Expr(pred, args...))
}

// GroupBy adds GROUP BY expressions to the query.
func (b AnalyticsSelectBuilder) GroupBy(groupBys ...string) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, []string](b, "GroupBys", groupBys)
}

// Having adds an expression to the HAVING clause of the query.
func (b AnalyticsSelectBuilder) Having(pred any, rest ...any) AnalyticsSelectBuilder {
	return Append[AnalyticsSelectBuilder, N1qlizer](b, "HavingParts", Expr(pred, rest...))
}

// OrderBy adds ORDER BY expressions to the query.
func (b AnalyticsSelectBuilder) OrderBy(orderBys ...string) AnalyticsSelectBuilder {
	parts := make([]N1qlizer, 0, len(orderBys))
	for _, str := range orderBys {
		parts = append(parts, newPart(str))
	}
	return Set[AnalyticsSelectBuilder, []N1qlizer](b, "OrderByParts", parts)
}

// Limit sets a LIMIT clause on the query.
func (b AnalyticsSelectBuilder) Limit(limit uint64) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, string](b, "Limit", fmt.Sprintf("%d", limit))
}

// Offset sets an OFFSET clause on the query.
func (b AnalyticsSelectBuilder) Offset(offset uint64) AnalyticsSelectBuilder {
	return Set[AnalyticsSelectBuilder, string](b, "Offset", fmt.Sprintf("%d", offset))
}

// AnalyticsSelect creates a new AnalyticsSelectBuilder for Couchbase Analytics queries.
func AnalyticsSelect(columns ...string) AnalyticsSelectBuilder {
	sb := StatementBuilderType(EmptyBuilder)
	asb := AnalyticsSelectBuilder(sb)
	asb = asb.PlaceholderFormat(Question)
	return asb.Columns(columns...)
}

// Examples of Analytics specific functions

// ArrayAvg returns an Analytics array_avg function call
func ArrayAvg(arr string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_AVG(%s)", arr))
}

// ArraySum returns an Analytics array_sum function call
func ArraySum(arr string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_SUM(%s)", arr))
}

// ArrayMin returns an Analytics array_min function call
func ArrayMin(arr string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_MIN(%s)", arr))
}

// ArrayMax returns an Analytics array_max function call
func ArrayMax(arr string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_MAX(%s)", arr))
}

// ArrayCount returns an Analytics array_count function call
func ArrayCount(arr string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_COUNT(%s)", arr))
}

// ArrayFilter returns an Analytics array_filter function call
func ArrayFilter(arr, variable, condition string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_FILTER(%s, %s, %s)", arr, variable, condition))
}

// ArrayFlatten returns an Analytics array_flatten function call
func ArrayFlatten(arr string) N1qlizer {
	return Expr(fmt.Sprintf("ARRAY_FLATTEN(%s)", arr))
}

// ObjectPairs returns an Analytics object_pairs function call
func ObjectPairs(obj string) N1qlizer {
	return Expr(fmt.Sprintf("OBJECT_PAIRS(%s)", obj))
}

// ObjectNames returns an Analytics object_names function call
func ObjectNames(obj string) N1qlizer {
	return Expr(fmt.Sprintf("OBJECT_NAMES(%s)", obj))
}

// ObjectValues returns an Analytics object_values function call
func ObjectValues(obj string) N1qlizer {
	return Expr(fmt.Sprintf("OBJECT_VALUES(%s)", obj))
}

// ObjectRemove returns an Analytics object_remove function call
func ObjectRemove(obj string, fields ...string) N1qlizer {
	quotedFields := make([]string, len(fields))
	for i, field := range fields {
		quotedFields[i] = fmt.Sprintf("\"%s\"", field)
	}

	return Expr(fmt.Sprintf("OBJECT_REMOVE(%s, %s)", obj, strings.Join(quotedFields, ", ")))
}

// ObjectPut returns an Analytics object_put function call
func ObjectPut(obj, fieldName, value string) N1qlizer {
	return Expr(fmt.Sprintf("OBJECT_PUT(%s, \"%s\", %s)", obj, fieldName, value))
}
