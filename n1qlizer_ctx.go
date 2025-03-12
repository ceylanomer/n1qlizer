package n1qlizer

import (
	"context"
	"fmt"
)

// QueryExecutorContext is the interface that wraps the ExecuteContext method.
//
// ExecuteContext executes the given N1QL query with context as implemented by Couchbase SDK.
type QueryExecutorContext interface {
	ExecuteContext(ctx context.Context, query string, args ...interface{}) (QueryResult, error)
}

// QueryRunnerContext is the interface that combines QueryExecutor and QueryExecutorContext.
type QueryRunnerContext interface {
	QueryExecutor
	QueryExecutorContext
}

// StdCb encompasses the standard methods of Couchbase SDK that execute queries.
type StdCb interface {
	Execute(query string, args ...interface{}) (QueryResult, error)
}

// StdCbCtx encompasses the standard methods of Couchbase SDK that execute queries with context.
type StdCbCtx interface {
	ExecuteContext(ctx context.Context, query string, args ...interface{}) (QueryResult, error)
}

// WrapStdCb wraps a type implementing the standard Couchbase SDK interface with methods that
// n1qlizer expects.
func WrapStdCb(stdCb StdCb) QueryRunner {
	return &stdCbRunner{stdCb}
}

type stdCbRunner struct {
	StdCb
}

// Execute builds and executes the given query.
func (r *stdCbRunner) Execute(query string, args ...interface{}) (QueryResult, error) {
	return r.StdCb.Execute(query, args...)
}

// WrapStdCbCtx wraps a type implementing the standard Couchbase SDK interface with context support
// with methods that n1qlizer expects.
func WrapStdCbCtx(stdCb StdCbCtx) QueryRunnerContext {
	return &stdCbRunnerCtx{stdCb}
}

type stdCbRunnerCtx struct {
	StdCbCtx
}

// Execute builds and executes the given query.
func (r *stdCbRunnerCtx) Execute(query string, args ...interface{}) (QueryResult, error) {
	return r.ExecuteContext(context.Background(), query, args...)
}

// ExecuteContext builds and executes the given query with context.
func (r *stdCbRunnerCtx) ExecuteContext(ctx context.Context, query string, args ...interface{}) (QueryResult, error) {
	return r.StdCbCtx.ExecuteContext(ctx, query, args...)
}

// setRunWith sets the RunWith value for StatementBuilderType.
func setRunWith(b interface{}, runner QueryRunner) interface{} {
	switch r := runner.(type) {
	case StdCbCtx:
		runner = WrapStdCbCtx(r)
	case StdCb:
		runner = WrapStdCb(r)
	}
	return Set(b, "RunWith", runner)
}

// RunnerNotQueryRunnerContext is returned by QueryRowContext if the RunWith value doesn't implement QueryRunnerContext.
var RunnerNotQueryRunnerContext = fmt.Errorf("cannot QueryRowContext; Runner is not a QueryRunnerContext")

// ExecuteContextWith builds and executes a query with context.
func ExecuteContextWith(ctx context.Context, db QueryExecutorContext, n N1qlizer) (res QueryResult, err error) {
	query, args, err := n.ToN1ql()
	if err != nil {
		return nil, err
	}
	return db.ExecuteContext(ctx, query, args...)
}
