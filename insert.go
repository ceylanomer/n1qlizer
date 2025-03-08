package n1qlizer

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

// insertData stores the state of an INSERT query as it is built
type insertData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           QueryRunner
	Prefixes          []N1qlizer
	Options           []string
	Into              string
	Columns           []string
	Values            [][]interface{}
	Suffixes          []N1qlizer
	SetMap            map[string]interface{}
}

func (d *insertData) ToN1ql() (sqlStr string, args []interface{}, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *insertData) toN1qlRaw() (sqlStr string, args []interface{}, err error) {
	if len(d.Into) == 0 {
		err = fmt.Errorf("insert statements must specify a table")
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

	sql.WriteString("INSERT ")

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(d.Into)

	if len(d.Columns) > 0 {
		sql.WriteString(" (")
		sql.WriteString(strings.Join(d.Columns, ", "))
		sql.WriteString(")")
	}

	if len(d.Values) > 0 {
		sql.WriteString(" VALUES ")

		valuesStrings := make([]string, len(d.Values))
		for i, values := range d.Values {
			valueStrings := make([]string, len(values))
			for j, value := range values {
				if expr, ok := value.(N1qlizer); ok {
					vsql, vargs, err := expr.ToN1ql()
					if err != nil {
						return "", nil, err
					}
					valueStrings[j] = vsql
					args = append(args, vargs...)
				} else {
					valueStrings[j] = "?"
					args = append(args, value)
				}
			}
			valuesStrings[i] = fmt.Sprintf("(%s)", strings.Join(valueStrings, ", "))
		}

		sql.WriteString(strings.Join(valuesStrings, ", "))
	}

	if len(d.SetMap) > 0 {
		if len(d.Values) > 0 {
			return "", nil, fmt.Errorf("insert statements cannot use both VALUES and SET")
		}

		sql.WriteString(" SET ")

		// Sort keys for consistent output
		keys := make([]string, 0, len(d.SetMap))
		for key := range d.SetMap {
			keys = append(keys, key)
		}
		sort.Strings(keys)

		// Couchbase inserts can use SET clause
		sets := make([]string, 0, len(d.SetMap))
		for _, column := range keys {
			value := d.SetMap[column]
			if n1ql, ok := value.(N1qlizer); ok {
				vsql, vargs, err := n1ql.ToN1ql()
				if err != nil {
					return "", nil, err
				}
				sets = append(sets, fmt.Sprintf("%s=%s", column, vsql))
				args = append(args, vargs...)
			} else {
				sets = append(sets, fmt.Sprintf("%s=?", column))
				args = append(args, value)
			}
		}
		sql.WriteString(strings.Join(sets, ", "))
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

// InsertBuilder builds SQL INSERT statements.
type InsertBuilder Builder

func init() {
	Register(InsertBuilder{}, insertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Dollar) for the query.
func (b InsertBuilder) PlaceholderFormat(f PlaceholderFormat) InsertBuilder {
	return Set(b, "PlaceholderFormat", f).(InsertBuilder)
}

// Runner methods

// RunWith sets a Runner (like Couchbase DB connection) to be used with e.g. Execute.
func (b InsertBuilder) RunWith(runner QueryRunner) InsertBuilder {
	return Set(b, "RunWith", runner).(InsertBuilder)
}

// Execute builds and sends the query to the runner set by RunWith.
func (b InsertBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(insertData)

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
func (b InsertBuilder) ToN1ql() (string, []interface{}, error) {
	data := GetStruct(b).(insertData)
	return data.ToN1ql()
}

// MustN1ql builds the query into a N1QL string and args, and panics on error.
func (b InsertBuilder) MustN1ql() (string, []interface{}) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b InsertBuilder) Prefix(sql string, args ...interface{}) InsertBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the beginning of the query.
func (b InsertBuilder) PrefixExpr(expr N1qlizer) InsertBuilder {
	return Append(b, "Prefixes", expr).(InsertBuilder)
}

// Options adds keyword options before the INTO clause of the query.
func (b InsertBuilder) Options(options ...string) InsertBuilder {
	return Extend(b, "Options", options).(InsertBuilder)
}

// Into sets the INTO clause of the query.
func (b InsertBuilder) Into(into string) InsertBuilder {
	return Set(b, "Into", into).(InsertBuilder)
}

// Columns adds columns to the query.
func (b InsertBuilder) Columns(columns ...string) InsertBuilder {
	return Extend(b, "Columns", columns).(InsertBuilder)
}

// Values adds a single row's values to the query.
func (b InsertBuilder) Values(values ...interface{}) InsertBuilder {
	return Append(b, "Values", values).(InsertBuilder)
}

// SetMap set columns and values for insert in one step.
func (b InsertBuilder) SetMap(clauses map[string]interface{}) InsertBuilder {
	// Couchbase prefers working with JSON objects
	// We'll convert the map to SET clauses
	if GetStruct(b).(insertData).SetMap == nil {
		return Set(b, "SetMap", clauses).(InsertBuilder)
	}

	data := GetStruct(b).(insertData)
	for key, value := range clauses {
		data.SetMap[key] = value
	}
	return Set(b, "SetMap", data.SetMap).(InsertBuilder)
}

// Suffix adds an expression to the end of the query.
func (b InsertBuilder) Suffix(sql string, args ...interface{}) InsertBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b InsertBuilder) SuffixExpr(expr N1qlizer) InsertBuilder {
	return Append(b, "Suffixes", expr).(InsertBuilder)
}
