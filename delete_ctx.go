package n1qlizer

import (
	"context"
)

// ExecuteContext builds and executes the query with the context and runner set by RunWith.
func (d *deleteData) ExecuteContext(ctx context.Context) (QueryResult, error) {
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
func (b DeleteBuilder) ExecuteContext(ctx context.Context) (QueryResult, error) {
	data := GetStruct(b).(deleteData)
	return data.ExecuteContext(ctx)
}

// RunWithContext sets a Runner (like a Couchbase DB connection with Context support) to be used with e.g. ExecuteContext.
func (b DeleteBuilder) RunWithContext(runner QueryRunnerContext) DeleteBuilder {
	return setRunnerContext(b, runner).(DeleteBuilder)
}
