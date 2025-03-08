package n1qlizer

import (
	"fmt"
	"strings"
)

// FTSSearchOptions represents options for a Full-Text Search query
type FTSSearchOptions struct {
	IndexName string
	Analyzer  string
	Fuzziness int
	Boost     float64
	Limit     int
	Score     string   // Name of the field to store the score in
	Highlight bool     // Whether to enable highlighting
	Fields    []string // Fields to search in
}

// FTSMatch creates a Full-Text Search match expression
func FTSMatch(query string, options ...FTSSearchOptions) N1qlizer {
	opts := FTSSearchOptions{}
	if len(options) > 0 {
		opts = options[0]
	}

	params := make([]string, 0)

	if opts.IndexName == "" {
		return Expr("ERROR: FTS index name is required")
	}

	// Basic search query
	searchQuery := fmt.Sprintf("SEARCH(%s, ", opts.IndexName)

	// If fields are specified, create a field-specific search
	if len(opts.Fields) > 0 {
		fieldQueries := make([]string, len(opts.Fields))
		for i, field := range opts.Fields {
			fieldQueries[i] = fmt.Sprintf("%s:%s", field, query)
		}
		searchQuery += fmt.Sprintf("\"%s\"", strings.Join(fieldQueries, " OR "))
	} else {
		searchQuery += fmt.Sprintf("\"%s\"", query)
	}

	// Add options
	if opts.Analyzer != "" {
		params = append(params, fmt.Sprintf("\"analyzer\": \"%s\"", opts.Analyzer))
	}

	if opts.Fuzziness > 0 {
		params = append(params, fmt.Sprintf("\"fuzziness\": %d", opts.Fuzziness))
	}

	if opts.Boost > 0 {
		params = append(params, fmt.Sprintf("\"boost\": %f", opts.Boost))
	}

	if len(params) > 0 {
		searchQuery += fmt.Sprintf(", {%s}", strings.Join(params, ", "))
	}

	searchQuery += ")"

	// Add scoring if specified
	if opts.Score != "" {
		searchQuery = fmt.Sprintf("%s AS %s", searchQuery, opts.Score)
	}

	return Expr(searchQuery)
}

// FTSPhraseMatch creates a Full-Text Search phrase match expression
func FTSPhraseMatch(query string, options ...FTSSearchOptions) N1qlizer {
	opts := FTSSearchOptions{}
	if len(options) > 0 {
		opts = options[0]
	}

	if opts.IndexName == "" {
		return Expr("ERROR: FTS index name is required")
	}

	// Handle already quoted phrases to avoid double-quoting
	queryToUse := query
	if strings.HasPrefix(query, "\"") && strings.HasSuffix(query, "\"") {
		// Remove the quotes since we'll add them in the SEARCH function
		queryToUse = query[1 : len(query)-1]
	}

	// Build a direct search query
	searchQuery := fmt.Sprintf("SEARCH(%s, \"%s\"", opts.IndexName, queryToUse)

	// Add options
	params := make([]string, 0)
	if opts.Analyzer != "" {
		params = append(params, fmt.Sprintf("\"analyzer\": \"%s\"", opts.Analyzer))
	}

	if opts.Fuzziness > 0 {
		params = append(params, fmt.Sprintf("\"fuzziness\": %d", opts.Fuzziness))
	}

	if opts.Boost > 0 {
		params = append(params, fmt.Sprintf("\"boost\": %f", opts.Boost))
	}

	if len(params) > 0 {
		searchQuery += fmt.Sprintf(", {%s}", strings.Join(params, ", "))
	}

	searchQuery += ")"

	// Add scoring if specified
	if opts.Score != "" {
		searchQuery = fmt.Sprintf("%s AS %s", searchQuery, opts.Score)
	}

	return Expr(searchQuery)
}

// FTSWildcardMatch creates a Full-Text Search wildcard match expression
func FTSWildcardMatch(pattern string, options ...FTSSearchOptions) N1qlizer {
	return FTSMatch(fmt.Sprintf("*%s*", pattern), options...)
}

// FTSPrefixMatch creates a Full-Text Search prefix match expression
func FTSPrefixMatch(prefix string, options ...FTSSearchOptions) N1qlizer {
	return FTSMatch(fmt.Sprintf("%s*", prefix), options...)
}

// FTSRangeMatch creates a Full-Text Search range match expression
func FTSRangeMatch(field string, min, max interface{}, options ...FTSSearchOptions) N1qlizer {
	var rangeQuery string

	if min != nil && max != nil {
		rangeQuery = fmt.Sprintf("%s:[%v TO %v]", field, min, max)
	} else if min != nil {
		rangeQuery = fmt.Sprintf("%s:>=%v", field, min)
	} else if max != nil {
		rangeQuery = fmt.Sprintf("%s:<=%v", field, max)
	} else {
		return Expr("ERROR: At least one of min or max must be specified")
	}

	return FTSMatch(rangeQuery, options...)
}

// FTSConjunction creates a conjunction (AND) of multiple FTS expressions
func FTSConjunction(expressions ...N1qlizer) N1qlizer {
	if len(expressions) == 0 {
		return Expr("")
	}

	if len(expressions) == 1 {
		return expressions[0]
	}

	queries := make([]string, len(expressions))
	args := make([]interface{}, 0)

	for i, expr := range expressions {
		sql, exprArgs, err := expr.ToN1ql()
		if err != nil {
			return Expr(fmt.Sprintf("ERROR: %s", err.Error()))
		}

		queries[i] = sql
		args = append(args, exprArgs...)
	}

	return Expr(fmt.Sprintf("(%s)", strings.Join(queries, " AND ")), args...)
}

// FTSDisjunction creates a disjunction (OR) of multiple FTS expressions
func FTSDisjunction(expressions ...N1qlizer) N1qlizer {
	if len(expressions) == 0 {
		return Expr("")
	}

	if len(expressions) == 1 {
		return expressions[0]
	}

	queries := make([]string, len(expressions))
	args := make([]interface{}, 0)

	for i, expr := range expressions {
		sql, exprArgs, err := expr.ToN1ql()
		if err != nil {
			return Expr(fmt.Sprintf("ERROR: %s", err.Error()))
		}

		queries[i] = sql
		args = append(args, exprArgs...)
	}

	return Expr(fmt.Sprintf("(%s)", strings.Join(queries, " OR ")), args...)
}

// FTSSearchService creates an expression to use Couchbase's dedicated search service
func FTSSearchService(indexName, query string, options ...interface{}) N1qlizer {
	var fieldsVal string
	var limit, offset int
	var highlightStyle, scoreField string
	var explain bool

	if indexName == "" {
		return Expr("ERROR: FTS index name is required")
	}

	// Process options
	for i := 0; i < len(options); i += 2 {
		if i+1 >= len(options) {
			break
		}

		key, ok := options[i].(string)
		if !ok {
			continue
		}

		value := options[i+1]

		switch key {
		case "fields":
			if fields, ok := value.([]string); ok {
				fieldsStr := make([]string, len(fields))
				for i, field := range fields {
					fieldsStr[i] = fmt.Sprintf("\"%s\"", field)
				}
				fieldsVal = fmt.Sprintf("[%s]", strings.Join(fieldsStr, ", "))
			}
		case "limit":
			if v, ok := value.(int); ok {
				limit = v
			}
		case "offset":
			if v, ok := value.(int); ok {
				offset = v
			}
		case "highlight":
			if style, ok := value.(string); ok {
				highlightStyle = style
			}
		case "score":
			if field, ok := value.(string); ok {
				scoreField = field
			}
		case "explain":
			if v, ok := value.(bool); ok {
				explain = v
			}
		}
	}

	// Build the SEARCH function call
	searchArgs := make([]string, 0)
	searchArgs = append(searchArgs, fmt.Sprintf("index: %s", indexName))
	searchArgs = append(searchArgs, fmt.Sprintf("query: \"%s\"", query))

	if fieldsVal != "" {
		searchArgs = append(searchArgs, fmt.Sprintf("fields: %s", fieldsVal))
	}

	if limit > 0 {
		searchArgs = append(searchArgs, fmt.Sprintf("limit: %d", limit))
	}

	if offset > 0 {
		searchArgs = append(searchArgs, fmt.Sprintf("offset: %d", offset))
	}

	if highlightStyle != "" {
		searchArgs = append(searchArgs, fmt.Sprintf("highlight: {\"style\":\"%s\"}", highlightStyle))
	}

	if explain {
		searchArgs = append(searchArgs, "explain: true")
	}

	searchCall := fmt.Sprintf("SEARCH({%s})", strings.Join(searchArgs, ", "))

	if scoreField != "" {
		searchCall = fmt.Sprintf("%s AS %s", searchCall, scoreField)
	}

	return Expr(searchCall)
}

// SelectBuilder method for FTS

// WithSearch adds a SEARCH clause to the WHERE part of a query
func (b SelectBuilder) WithSearch(search N1qlizer) SelectBuilder {
	return b.Where(search)
}
