package n1qlizer

import (
	"bytes"
	"fmt"
	"strings"
)

// upsertData stores the state of an UPSERT query as it is built
type upsertData struct {
	PlaceholderFormat PlaceholderFormat
	RunWith           QueryRunner
	Prefixes          []N1qlizer
	Options           []string
	Into              string
	Key               string
	Value             interface{}
	Columns           []string
	Values            [][]interface{}
	Suffixes          []N1qlizer
	SetMap            map[string]interface{}
}

func (d *upsertData) ToN1ql() (sqlStr string, args []interface{}, err error) {
	sqlStr, args, err = d.toN1qlRaw()
	if err != nil {
		return
	}

	sqlStr, err = d.PlaceholderFormat.ReplacePlaceholders(sqlStr)
	return
}

func (d *upsertData) toN1qlRaw() (sqlStr string, args []interface{}, err error) {
	if len(d.Into) == 0 {
		err = fmt.Errorf("upsert statements must specify a bucket")
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

	sql.WriteString("UPSERT ")

	if len(d.Options) > 0 {
		sql.WriteString(strings.Join(d.Options, " "))
		sql.WriteString(" ")
	}

	sql.WriteString("INTO ")
	sql.WriteString(d.Into)

	// Couchbase's UPSERT has a special syntax for keys and values
	if d.Key != "" && d.Value != nil {
		// UPSERT INTO bucket (KEY, VALUE) VALUES ("key1", {"field": "value"})
		sql.WriteString(" (KEY, VALUE) VALUES (")
		if strings.HasPrefix(d.Key, "?") {
			sql.WriteString(d.Key)
			args = append(args, d.Key[1:]) // Assuming ? is a placeholder
		} else {
			sql.WriteString("?")
			args = append(args, d.Key)
		}
		sql.WriteString(", ")

		if expr, ok := d.Value.(N1qlizer); ok {
			vsql, vargs, err := expr.ToN1ql()
			if err != nil {
				return "", nil, err
			}
			sql.WriteString(vsql)
			args = append(args, vargs...)
		} else {
			sql.WriteString("?")
			args = append(args, d.Value)
		}
		sql.WriteString(")")
	} else if len(d.Columns) > 0 && len(d.Values) > 0 {
		// Standard INSERT-like syntax
		sql.WriteString(" (")
		sql.WriteString(strings.Join(d.Columns, ", "))
		sql.WriteString(")")

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
	} else if len(d.SetMap) > 0 {
		// Use SET for individual fields
		sql.WriteString(" SET ")
		sets := make([]string, 0, len(d.SetMap))
		for column, value := range d.SetMap {
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

// UpsertBuilder builds Couchbase N1QL UPSERT statements.
type UpsertBuilder Builder

func init() {
	Register(UpsertBuilder{}, upsertData{})
}

// Format methods

// PlaceholderFormat sets PlaceholderFormat (e.g. Dollar) for the query.
func (b UpsertBuilder) PlaceholderFormat(f PlaceholderFormat) UpsertBuilder {
	return Set(b, "PlaceholderFormat", f).(UpsertBuilder)
}

// Runner methods

// RunWith sets a Runner (like Couchbase DB connection) to be used with e.g. Execute.
func (b UpsertBuilder) RunWith(runner QueryRunner) UpsertBuilder {
	return Set(b, "RunWith", runner).(UpsertBuilder)
}

// Execute builds and sends the query to the runner set by RunWith.
func (b UpsertBuilder) Execute() (QueryResult, error) {
	data := GetStruct(b).(upsertData)

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
func (b UpsertBuilder) ToN1ql() (string, []interface{}, error) {
	data := GetStruct(b).(upsertData)
	return data.ToN1ql()
}

// MustN1ql builds the query into a N1QL string and args, and panics on error.
func (b UpsertBuilder) MustN1ql() (string, []interface{}) {
	sql, args, err := b.ToN1ql()
	if err != nil {
		panic(err)
	}
	return sql, args
}

// Prefix adds an expression to the beginning of the query.
func (b UpsertBuilder) Prefix(sql string, args ...interface{}) UpsertBuilder {
	return b.PrefixExpr(Expr(sql, args...))
}

// PrefixExpr adds an expression to the beginning of the query.
func (b UpsertBuilder) PrefixExpr(expr N1qlizer) UpsertBuilder {
	return Append(b, "Prefixes", expr).(UpsertBuilder)
}

// Options adds keyword options before the INTO clause of the query.
func (b UpsertBuilder) Options(options ...string) UpsertBuilder {
	return Extend(b, "Options", options).(UpsertBuilder)
}

// Into sets the INTO clause of the query.
func (b UpsertBuilder) Into(into string) UpsertBuilder {
	return Set(b, "Into", into).(UpsertBuilder)
}

// Document sets the key and document value for upsert.
// This is Couchbase-specific and allows direct insertion of a JSON document.
func (b UpsertBuilder) Document(key string, value interface{}) UpsertBuilder {
	b = Set(b, "Key", key).(UpsertBuilder)
	return Set(b, "Value", value).(UpsertBuilder)
}

// Columns adds columns to the query.
func (b UpsertBuilder) Columns(columns ...string) UpsertBuilder {
	return Extend(b, "Columns", columns).(UpsertBuilder)
}

// Values adds a single row's values to the query.
func (b UpsertBuilder) Values(values ...interface{}) UpsertBuilder {
	return Append(b, "Values", values).(UpsertBuilder)
}

// SetMap set columns and values for upsert in one step.
func (b UpsertBuilder) SetMap(clauses map[string]interface{}) UpsertBuilder {
	// Couchbase prefers working with JSON objects
	// We'll convert the map to SET clauses
	if GetStruct(b).(upsertData).SetMap == nil {
		return Set(b, "SetMap", clauses).(UpsertBuilder)
	}

	data := GetStruct(b).(upsertData)
	if data.SetMap == nil {
		data.SetMap = map[string]interface{}{}
	}

	for key, value := range clauses {
		data.SetMap[key] = value
	}
	return Set(b, "SetMap", data.SetMap).(UpsertBuilder)
}

// Suffix adds an expression to the end of the query.
func (b UpsertBuilder) Suffix(sql string, args ...interface{}) UpsertBuilder {
	return b.SuffixExpr(Expr(sql, args...))
}

// SuffixExpr adds an expression to the end of the query.
func (b UpsertBuilder) SuffixExpr(expr N1qlizer) UpsertBuilder {
	return Append(b, "Suffixes", expr).(UpsertBuilder)
}
