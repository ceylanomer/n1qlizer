package n1qlizer

import (
	"strings"
	"testing"
)

func TestFTSMatch(t *testing.T) {
	t.Run("Basic match", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSMatch("laptop", options)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		if !strings.Contains(sql, "SEARCH(product_index, \"laptop\")") {
			t.Errorf("Expected 'SEARCH(product_index, \"laptop\")', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Match with fields", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
			Fields:    []string{"name", "description"},
		}
		expr := FTSMatch("laptop", options)
		sql, _, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		if !strings.Contains(sql, "SEARCH(product_index, \"name:laptop OR description:laptop\")") {
			t.Errorf("Expected search with field specification, got '%s'", sql)
		}
	})

	t.Run("Match with analyzer", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
			Analyzer:  "standard",
		}
		expr := FTSMatch("laptop", options)
		sql, _, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		if !strings.Contains(sql, "\"analyzer\": \"standard\"") {
			t.Errorf("Expected analyzer option, got '%s'", sql)
		}
	})

	t.Run("Match with fuzziness", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
			Fuzziness: 2,
		}
		expr := FTSMatch("laptop", options)
		sql, _, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		if !strings.Contains(sql, "\"fuzziness\": 2") {
			t.Errorf("Expected fuzziness option, got '%s'", sql)
		}
	})

	t.Run("Match with boost", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
			Boost:     1.5,
		}
		expr := FTSMatch("laptop", options)
		sql, _, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		if !strings.Contains(sql, "\"boost\": 1.5") {
			t.Errorf("Expected boost option, got '%s'", sql)
		}
	})

	t.Run("Match with score", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
			Score:     "relevance",
		}
		expr := FTSMatch("laptop", options)
		sql, _, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		if !strings.Contains(sql, "AS relevance") {
			t.Errorf("Expected score field, got '%s'", sql)
		}
	})

	t.Run("No index name", func(t *testing.T) {
		options := FTSSearchOptions{}
		expr := FTSMatch("laptop", options)
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "ERROR: FTS index name is required") {
			t.Errorf("Expected error for missing index name, got '%s'", sql)
		}
	})
}

func TestFTSPhraseMatch(t *testing.T) {
	t.Run("Basic phrase match", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSPhraseMatch("gaming laptop", options)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS phrase match: %v", err)
		}

		if !strings.Contains(sql, "\"gaming laptop\"") {
			t.Errorf("Expected phrase to be quoted, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Pre-quoted phrase", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSPhraseMatch("\"gaming laptop\"", options)
		sql, _, _ := expr.ToN1ql()

		// Should not double-quote
		if strings.Contains(sql, "\"\"gaming laptop\"\"") {
			t.Errorf("Expected no double-quoting, got '%s'", sql)
		}
	})
}

func TestFTSWildcardMatch(t *testing.T) {
	t.Run("Wildcard match", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSWildcardMatch("laptop", options)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS wildcard match: %v", err)
		}

		if !strings.Contains(sql, "*laptop*") {
			t.Errorf("Expected wildcards around term, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestFTSPrefixMatch(t *testing.T) {
	t.Run("Prefix match", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSPrefixMatch("lap", options)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS prefix match: %v", err)
		}

		if !strings.Contains(sql, "lap*") {
			t.Errorf("Expected wildcard after term, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestFTSRangeMatch(t *testing.T) {
	t.Run("Inclusive range", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSRangeMatch("price", 100, 500, options)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS range match: %v", err)
		}

		if !strings.Contains(sql, "price:[100 TO 500]") {
			t.Errorf("Expected inclusive range expression, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Minimum only", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSRangeMatch("price", 100, nil, options)
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "price:>=100") {
			t.Errorf("Expected greater-than-equal expression, got '%s'", sql)
		}
	})

	t.Run("Maximum only", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSRangeMatch("price", nil, 500, options)
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "price:<=500") {
			t.Errorf("Expected less-than-equal expression, got '%s'", sql)
		}
	})

	t.Run("No bounds", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSRangeMatch("price", nil, nil, options)
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "ERROR: At least one of min or max must be specified") {
			t.Errorf("Expected error message, got '%s'", sql)
		}
	})
}

func TestFTSConjunctionDisjunction(t *testing.T) {
	t.Run("Conjunction", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr1 := FTSMatch("laptop", options)
		expr2 := FTSRangeMatch("price", 100, 500, options)

		conj := FTSConjunction(expr1, expr2)
		sql, args, err := conj.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS conjunction: %v", err)
		}

		if !strings.Contains(sql, "AND") {
			t.Errorf("Expected AND operator, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Empty conjunction", func(t *testing.T) {
		conj := FTSConjunction()
		sql, args, err := conj.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build empty FTS conjunction: %v", err)
		}

		if sql != "" {
			t.Errorf("Expected empty string, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Single element conjunction", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr := FTSMatch("laptop", options)
		conj := FTSConjunction(expr)
		sql1, _, _ := conj.ToN1ql()
		sql2, _, _ := expr.ToN1ql()

		if sql1 != sql2 {
			t.Errorf("Expected single element to be returned as-is, got '%s' vs '%s'", sql1, sql2)
		}
	})

	t.Run("Disjunction", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		expr1 := FTSMatch("laptop", options)
		expr2 := FTSMatch("desktop", options)

		disj := FTSDisjunction(expr1, expr2)
		sql, args, err := disj.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS disjunction: %v", err)
		}

		if !strings.Contains(sql, "OR") {
			t.Errorf("Expected OR operator, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestFTSSearchService(t *testing.T) {
	t.Run("Basic search service", func(t *testing.T) {
		expr := FTSSearchService("product_index", "laptop")
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS search service: %v", err)
		}

		if !strings.Contains(sql, "SEARCH({index: product_index, query: \"laptop\"}") {
			t.Errorf("Expected basic search service expression, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("With fields", func(t *testing.T) {
		expr := FTSSearchService("product_index", "laptop",
			"fields", []string{"name", "description"})
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "fields: [\"name\", \"description\"]") {
			t.Errorf("Expected fields specification, got '%s'", sql)
		}
	})

	t.Run("With limit and offset", func(t *testing.T) {
		expr := FTSSearchService("product_index", "laptop",
			"limit", 10, "offset", 20)
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "limit: 10") || !strings.Contains(sql, "offset: 20") {
			t.Errorf("Expected limit and offset, got '%s'", sql)
		}
	})

	t.Run("With highlighting", func(t *testing.T) {
		expr := FTSSearchService("product_index", "laptop",
			"highlight", "html")
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "highlight: {\"style\":\"html\"}") {
			t.Errorf("Expected highlight style, got '%s'", sql)
		}
	})

	t.Run("With score field", func(t *testing.T) {
		expr := FTSSearchService("product_index", "laptop",
			"score", "relevance")
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "AS relevance") {
			t.Errorf("Expected score field, got '%s'", sql)
		}
	})

	t.Run("With explain", func(t *testing.T) {
		expr := FTSSearchService("product_index", "laptop",
			"explain", true)
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "explain: true") {
			t.Errorf("Expected explain flag, got '%s'", sql)
		}
	})

	t.Run("Missing index name", func(t *testing.T) {
		expr := FTSSearchService("", "laptop")
		sql, _, _ := expr.ToN1ql()

		if !strings.Contains(sql, "ERROR: FTS index name is required") {
			t.Errorf("Expected error for missing index name, got '%s'", sql)
		}
	})
}

func TestWithSearch(t *testing.T) {
	t.Run("WithSearch in SelectBuilder", func(t *testing.T) {
		options := FTSSearchOptions{
			IndexName: "product_index",
		}
		search := FTSMatch("laptop", options)

		sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)
		builder := sb.
			Select("id", "name", "price").
			From("products").
			WithSearch(search)

		sql, args, err := builder.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build select with search: %v", err)
		}

		if !strings.Contains(sql, "WHERE SEARCH(product_index, \"laptop\")") {
			t.Errorf("Expected search in WHERE clause, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}
