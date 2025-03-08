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
	SetClauses        map[string]interface{}
	WhereParts        []N1qlizer
	UseKeys           string
	Limit             string
	Offset            string
	Suffixes          []N1qlizer
}

func (d *updateData) ToN1ql() (sqlStr string, args []interface{}, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *updateData) toN1qlRaw() (sqlStr string, args []interface{}, err error) {
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

	// Sort keys for deterministic output
	keys := make([]string, 0, len(d.SetClauses))
	for key := range d.SetClauses {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i, key := range keys {
		if i > 0 {
			sql.WriteString(", ")
		}

		value := d.SetClauses[key]
		if n1ql, ok := value.(N1qlizer); ok {
			vsql, vargs, err := n1ql.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			sql.WriteString(key)
			sql.WriteString(" = ")
			sql.WriteString(vsql)
			args = append(args, vargs...)
		} else {
			sql.WriteString(key)
			sql.WriteString(" = ?")
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

// UpdateBuilder builds SQL UPDATE statements.
type UpdateBuilder Builder

func init() {
	Register(UpdateBuilder{}, updateData{})
}

// PlaceholderFormat sets PlaceholderFormat (e.g. Dollar) for the query.
func (b UpdateBuilder) PlaceholderFormat(f PlaceholderFormat) UpdateBuilder {
	return Set(b, "PlaceholderFormat", f).(UpdateBuilder)
}

// RunWith sets a Runner (like Couchbase DB connection) to be used with e.g. Execute.
func (b UpdateBuilder) RunWith(runner QueryRunner) UpdateBuilder {
	return Set(b, "RunWith", runner).(UpdateBuilder)
}

// Execute builds and sends the query to the runner set by RunWith.
func (b UpdateBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(updateData)

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
func (b UpdateBuilder) ToN1ql() (string, []interface{}, error) {
	data := GetStruct(b).(updateData)
	return data.ToN1ql()
}

// MustN1ql builds the query into a N1QL string and args, and panics on error.
func (b UpdateBuilder) MustN1ql() (string, []interface{}) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b UpdateBuilder) Prefix(sql string, args ...interface{}) UpdateBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the beginning of the query.
func (b UpdateBuilder) PrefixExpr(expr N1qlizer) UpdateBuilder {
	return Append(b, "Prefixes", expr).(UpdateBuilder)
}

// Table sets the table to be updated.
func (b UpdateBuilder) Table(table string) UpdateBuilder {
	return Set(b, "Table", table).(UpdateBuilder)
}

// UseKeys sets the USE KEYS clause of the query.
func (b UpdateBuilder) UseKeys(keys string) UpdateBuilder {
	return Set(b, "UseKeys", keys).(UpdateBuilder)
}

// Set adds SET clauses to the query.
func (b UpdateBuilder) Set(column string, value interface{}) UpdateBuilder {
	if GetStruct(b).(updateData).SetClauses == nil {
		return Set(b, "SetClauses", map[string]interface{}{
			column: value,
		}).(UpdateBuilder)
	}

	data := GetStruct(b).(updateData)
	data.SetClauses[column] = value
	return Set(b, "SetClauses", data.SetClauses).(UpdateBuilder)
}

// SetMap adds SET clauses to the query from a map.
func (b UpdateBuilder) SetMap(clauses map[string]interface{}) UpdateBuilder {
	// Merges with existing set clauses
	if GetStruct(b).(updateData).SetClauses == nil {
		return Set(b, "SetClauses", clauses).(UpdateBuilder)
	}

	data := GetStruct(b).(updateData)
	for key, value := range clauses {
		data.SetClauses[key] = value
	}
	return Set(b, "SetClauses", data.SetClauses).(UpdateBuilder)
}

// Where adds WHERE expressions to the query.
func (b UpdateBuilder) Where(pred interface{}, args ...interface{}) UpdateBuilder {
	return Append(b, "WhereParts", Expr(pred, args...)).(UpdateBuilder)
}

// Limit sets a LIMIT clause on the query.
func (b UpdateBuilder) Limit(limit uint64) UpdateBuilder {
	return Set(b, "Limit", fmt.Sprintf("%d", limit)).(UpdateBuilder)
}

// Offset sets an OFFSET clause on the query.
func (b UpdateBuilder) Offset(offset uint64) UpdateBuilder {
	return Set(b, "Offset", fmt.Sprintf("%d", offset)).(UpdateBuilder)
}

// Suffix adds an expression to the end of the query.
func (b UpdateBuilder) Suffix(sql string, args ...interface{}) UpdateBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b UpdateBuilder) SuffixExpr(expr N1qlizer) UpdateBuilder {
	return Append(b, "Suffixes", expr).(UpdateBuilder)
}
