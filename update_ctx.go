package n1qlizer

import (
	"context"
)

// ExecuteContext builds and executes the query with the context and runner set by RunWith.
func (d *updateData) ExecuteContext(ctx context.Context) (QueryResult, error) {
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
func (b UpdateBuilder) ExecuteContext(ctx context.Context) (QueryResult, error) {
	data := GetStruct(b).(updateData)
	return data.ExecuteContext(ctx)
}

// RunWithContext sets a Runner (like a Couchbase DB connection with Context support) to be used with e.g. ExecuteContext.
func (b UpdateBuilder) RunWithContext(runner QueryRunnerContext) UpdateBuilder {
	return setRunnerContext(b, runner).(UpdateBuilder)
}
