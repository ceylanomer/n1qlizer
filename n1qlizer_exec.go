package n1qlizer

// ExecuteWith executes the given N1QLizer using the provided QueryExecutor.
// This function is similar to ExecuteContextWith but does not use a context.
func ExecuteWith(db QueryExecutor, n N1qlizer) (res QueryResult, err error) {
	query, args, err := n.ToN1ql()
	if err != nil {
		return nil, err
	}

	return db.Execute(query, args...)
}
