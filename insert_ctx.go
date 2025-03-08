package n1qlizer

import (
	"context"
)

// ExecuteContext builds and executes the query with the context and runner set by RunWith.
func (d *insertData) ExecuteContext(ctx context.Context) (QueryResult, error) {
	if d.RunWith == nil {
		return nil, RunnerNotSet
	}

	runner, ok := d.RunWith.(QueryRunnerContext)
	if !ok {
		return nil, RunnerNotQueryRunnerContext
	}

	return ExecuteContextWith(ctx, runner, d)
}

// ExecuteContext builds and executes the query with the context and runner set by RunWith.
func (b InsertBuilder) ExecuteContext(ctx context.Context) (QueryResult, error) {
	data := GetStruct(b).(insertData)
	return data.ExecuteContext(ctx)
}

// RunWithContext sets a Runner (like a Couchbase DB connection with Context support) to be used with e.g. ExecuteContext.
func (b InsertBuilder) RunWithContext(runner QueryRunnerContext) InsertBuilder {
	return setRunnerContext(b, runner).(InsertBuilder)
}
