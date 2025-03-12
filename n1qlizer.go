// Package n1qlizer provides a fluent Couchbase N1QL query generator.
//
// It is inspired by github.com/Masterminds/squirrel
package n1qlizer

import (
	"bytes"
	"fmt"
	"strings"
)

// N1qlizer is the interface that wraps the ToN1ql method.
//
// ToN1ql returns a N1QL representation of the N1qlizer, along with a slice of args
// as passed to Couchbase SDK. It can also return an error.
type N1qlizer interface {
	ToN1ql() (string, []interface{}, error)
}

// rawN1qlizer is expected to do what N1qlizer does, but without finalizing placeholders.
// This is useful for nested queries.
type rawN1qlizer interface {
	toN1qlRaw() (string, []interface{}, error)
}

// QueryExecutor is the interface that wraps the Execute method.
//
// Execute executes the given N1QL query as implemented by Couchbase SDK.
type QueryExecutor interface {
	Execute(query string, args ...interface{}) (QueryResult, error)
}

// QueryResult represents the result of a N1QL query
type QueryResult interface {
	One(valuePtr interface{}) error
	All(slicePtr interface{}) error
	Close() error
}

// QueryRunner is the interface that combines query execution and result handling
type QueryRunner interface {
	QueryExecutor
}

// PlaceholderFormat is the interface that wraps the ReplacePlaceholders method.
type PlaceholderFormat interface {
	ReplacePlaceholders(sql string) (string, error)
}

// DebugN1qlizer calls ToN1ql on s and shows the approximate N1QL to be executed
//
// If ToN1ql returns an error, the result of this method will look like:
// "[ToN1ql error: %s]" or "[DebugN1qlizer error: %s]"
//
// IMPORTANT: As its name suggests, this function should only be used for
// debugging. While the string result *might* be valid N1QL, this function does
// not try very hard to ensure it. Additionally, executing the output of this
// function with any untrusted user input is certainly insecure.
func DebugN1qlizer(s N1qlizer) string {
	sql, args, err := s.ToN1ql()
	if err != nil {
		return fmt.Sprintf("[ToN1ql error: %s]", err)
	}

	// Handle both $ and ? placeholders
	buf := &bytes.Buffer{}
	i := 0

	// First handle $ placeholders (Couchbase style)
	for {
		p := strings.Index(sql, "$")
		if p == -1 {
			break
		}
		if len(sql[p:]) > 1 && sql[p:p+2] == "$$" { // escape $$ => $
			buf.WriteString(sql[:p])
			buf.WriteString("$")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			if i >= len(args) {
				return fmt.Sprintf(
					"[DebugN1qlizer error: too many placeholders in %#v for %d args]",
					sql, len(args))
			}
			buf.WriteString(sql[:p])
			// Find the end of the parameter name (should be a digit)
			end := p + 1
			for end < len(sql) && sql[end] >= '0' && sql[end] <= '9' {
				end++
			}
			fmt.Fprintf(buf, "'%v'", args[i])
			// advance our sql string "cursor" beyond the arg we placed
			sql = sql[end:]
			i++
		}
	}

	// Now handle ? placeholders
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}

		if i >= len(args) {
			return fmt.Sprintf(
				"[DebugN1qlizer error: too many placeholders in %#v for %d args]",
				sql, len(args))
		}

		buf.WriteString(sql[:p])
		fmt.Fprintf(buf, "'%v'", args[i])
		sql = sql[p+1:]
		i++
	}

	if i < len(args) {
		return fmt.Sprintf(
			"[DebugN1qlizer error: not enough placeholders in %#v for %d args]",
			sql, len(args))
	}

	// "append" any remaning sql that won't need interpolating
	buf.WriteString(sql)
	return buf.String()
}

// Dollar is a PlaceholderFormat instance that replaces placeholders with
// dollar-prefixed positional placeholders (e.g. $1, $2, $3).
// This is the format used by Couchbase N1QL.
var Dollar = dollarFormat{}

type dollarFormat struct{}

func (dollarFormat) ReplacePlaceholders(sql string) (string, error) {
	return replacePositionalPlaceholders(sql, "$")
}

type questionFormat struct{}

func (questionFormat) ReplacePlaceholders(sql string) (string, error) {
	return sql, nil
}

func replacePositionalPlaceholders(sql, prefix string) (string, error) {
	buf := &bytes.Buffer{}
	i := 0
	for {
		p := strings.Index(sql, "?")
		if p == -1 {
			break
		}
		if len(sql[p:]) > 1 && sql[p:p+2] == "??" { // escape ?? => ?
			buf.WriteString(sql[:p])
			buf.WriteString("?")
			if len(sql[p:]) == 1 {
				break
			}
			sql = sql[p+2:]
		} else {
			i++
			buf.WriteString(sql[:p])
			fmt.Fprintf(buf, "%s%d", prefix, i)
			sql = sql[p+1:]
		}
	}
	buf.WriteString(sql)
	return buf.String(), nil
}

// RunnerNotSet is returned by methods that need a Runner if it isn't set.
var RunnerNotSet = fmt.Errorf("cannot run; no Runner set (RunWith)")

// buildClauses is a helper function to build query clauses.
func buildClauses(parts []N1qlizer, sql *bytes.Buffer, sep string, args []interface{}) ([]interface{}, error) {
	for i, p := range parts {
		partSQL, partArgs, err := p.ToN1ql()
		if err != nil {
			return nil, err
		}
		if len(partSQL) > 0 {
			if i > 0 && len(sep) > 0 {
				sql.WriteString(sep)
			}
			sql.WriteString(partSQL)
			args = append(args, partArgs...)
		}
	}
	return args, nil
}

// StatementBuilderType is the type of StatementBuilder.
type StatementBuilderType Builder

// Select returns a SelectBuilder for this StatementBuilderType.
func (b StatementBuilderType) Select(columns ...string) SelectBuilder {
	return SelectBuilder(b).Columns(columns...)
}

// Insert returns a InsertBuilder for this StatementBuilderType.
func (b StatementBuilderType) Insert(into string) InsertBuilder {
	return InsertBuilder(b).Into(into)
}

// Upsert returns a UpsertBuilder for this StatementBuilderType.
// This is specific to Couchbase and is the preferred way to insert documents.
func (b StatementBuilderType) Upsert(into string) UpsertBuilder {
	return UpsertBuilder(b).Into(into)
}

// Update returns a UpdateBuilder for this StatementBuilderType.
func (b StatementBuilderType) Update(table string) UpdateBuilder {
	return UpdateBuilder(b).Table(table)
}

// Delete returns a DeleteBuilder for this StatementBuilderType.
func (b StatementBuilderType) Delete(from string) DeleteBuilder {
	return DeleteBuilder(b).From(from)
}

// AnalyticsSelect returns an AnalyticsSelectBuilder for this StatementBuilderType.
// This is specific to the Couchbase Analytics Service.
func (b StatementBuilderType) AnalyticsSelect(columns ...string) AnalyticsSelectBuilder {
	return AnalyticsSelectBuilder(b).Columns(columns...)
}

// PlaceholderFormat sets the PlaceholderFormat for this StatementBuilderType.
func (b StatementBuilderType) PlaceholderFormat(f PlaceholderFormat) StatementBuilderType {
	return Set(b, "PlaceholderFormat", f).(StatementBuilderType)
}

// RunWith sets the QueryRunner that this StatementBuilderType should execute
// queries with.
func (b StatementBuilderType) RunWith(runner QueryRunner) StatementBuilderType {
	return Set(b, "RunWith", runner).(StatementBuilderType)
}

// RunWithContext sets the QueryRunnerContext that this StatementBuilderType should execute
// queries with context support.
func (b StatementBuilderType) RunWithContext(runner QueryRunnerContext) StatementBuilderType {
	return Set(b, "RunWith", runner).(StatementBuilderType)
}

// StatementBuilder is a parent builder for other statement builders.
var Question = questionFormat{}
var StatementBuilder = StatementBuilderType(EmptyBuilder).PlaceholderFormat(Question)

// Select returns a new SelectBuilder, optionally setting some result columns.
//
// See SelectBuilder.Columns.
func Select(columns ...string) SelectBuilder {
	return StatementBuilder.Select(columns...)
}

// Insert returns a new InsertBuilder with the given table name.
//
// See InsertBuilder.Into.
func Insert(into string) InsertBuilder {
	return StatementBuilder.Insert(into)
}

// Upsert returns a new UpsertBuilder with the given table name.
//
// See UpsertBuilder.Into.
func Upsert(into string) UpsertBuilder {
	return StatementBuilder.Upsert(into)
}

// Update returns a new UpdateBuilder with the given table name.
//
// See UpdateBuilder.Table.
func Update(table string) UpdateBuilder {
	return StatementBuilder.Update(table)
}

// Delete returns a new DeleteBuilder with the given table name.
//
// See DeleteBuilder.Table.
func Delete(from string) DeleteBuilder {
	return StatementBuilder.Delete(from)
}