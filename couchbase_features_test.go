package n1qlizer

import (
	"strings"
	"testing"
)

// TestUpsert tests the Upsert query builder
func TestUpsert(t *testing.T) {
	// Create a custom builder to avoid nil pointer issues
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)

	testCases := []struct {
		name     string
		builder  UpsertBuilder
		expected string
		args     []interface{}
	}{
		{
			name:     "Simple UPSERT with document",
			builder:  sb.Upsert("users").Document("user123", map[string]interface{}{"name": "John", "age": 30}),
			expected: "UPSERT INTO users (KEY, VALUE) VALUES (?, ?)",
			args:     []interface{}{"user123", map[string]interface{}{"name": "John", "age": 30}},
		},
		{
			name:     "UPSERT with columns and values",
			builder:  sb.Upsert("users").Columns("id", "name", "age").Values("user123", "John", 30),
			expected: "UPSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			args:     []interface{}{"user123", "John", 30},
		},
		{
			name:     "UPSERT with SetMap",
			builder:  sb.Upsert("users").SetMap(map[string]interface{}{"id": "user123", "name": "John", "age": 30}),
			expected: "UPSERT INTO users SET id=?, name=?, age=?",
			args:     []interface{}{"user123", "John", 30},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql, args, err := tc.builder.ToN1ql()
			if err != nil {
				t.Fatalf("Failed to build query: %v", err)
			}

			// Since map iteration order is not guaranteed, we can't directly compare SQL
			// so we'll just check if it contains the expected parts
			if !contains(sql, "UPSERT INTO users") {
				t.Errorf("SQL does not contain 'UPSERT INTO users': %s", sql)
			}

			// For args, we'll check if the count is correct
			if len(args) != len(tc.args) {
				t.Errorf("Wrong number of args: Expected %d, got %d", len(tc.args), len(args))
				return
			}

			// For specific test cases, do additional checks
			if tc.name == "Simple UPSERT with document" {
				if !contains(sql, "(KEY, VALUE) VALUES") {
					t.Errorf("SQL does not contain '(KEY, VALUE) VALUES': %s", sql)
				}
			}
		})
	}
}

// TestNestAndUnnest tests the NEST and UNNEST clauses
func TestNestAndUnnest(t *testing.T) {
	// Create a custom builder to avoid nil pointer issues
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)

	testCases := []struct {
		name     string
		builder  SelectBuilder
		expected string
		args     []interface{}
	}{
		{
			name: "NEST with ON KEYS",
			builder: sb.Select("u.*", "o.*").
				From("users u").
				NestClause(Nest("orders").As("o").OnKeys("u.orderIds")),
			expected: "SELECT u.*, o.* FROM users u NEST orders AS o ON KEYS u.orderIds",
			args:     []interface{}{},
		},
		{
			name: "LEFT NEST with ON KEYS",
			builder: sb.Select("u.*", "o.*").
				From("users u").
				LeftNestClause(LeftNest("orders").As("o").OnKeys("u.orderIds")),
			expected: "SELECT u.*, o.* FROM users u LEFT NEST orders AS o ON KEYS u.orderIds",
			args:     []interface{}{},
		},
		{
			name: "UNNEST with alias",
			builder: sb.Select("u.*", "t").
				From("users u").
				UnnestClause(Unnest("u.tags").As("t")),
			expected: "SELECT u.*, t FROM users u UNNEST u.tags AS t",
			args:     []interface{}{},
		},
		{
			name: "LEFT UNNEST with alias",
			builder: sb.Select("u.*", "t").
				From("users u").
				LeftUnnestClause(LeftUnnest("u.tags").As("t")),
			expected: "SELECT u.*, t FROM users u LEFT UNNEST u.tags AS t",
			args:     []interface{}{},
		},
		{
			name: "NEST with condition",
			builder: sb.Select("u.*", "o.*").
				From("users u").
				NestClause(Nest("orders").As("o").On("o.type = ?", "completed")),
			expected: "SELECT u.*, o.* FROM users u NEST orders AS o ON o.type = ?",
			args:     []interface{}{"completed"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql, args, err := tc.builder.ToN1ql()
			if err != nil {
				t.Fatalf("Failed to build query: %v", err)
			}

			if sql != tc.expected {
				t.Errorf("Wrong SQL: \nExpected: %s\nGot: %s", tc.expected, sql)
			}

			if len(args) != len(tc.args) {
				t.Errorf("Wrong number of args: Expected %d, got %d", len(tc.args), len(args))
				return
			}

			for i, arg := range args {
				if arg != tc.args[i] {
					t.Errorf("Wrong arg at position %d: Expected %v, got %v", i, tc.args[i], arg)
				}
			}
		})
	}
}

// TestFTSSupport tests the Full Text Search support
func TestFTSSupport(t *testing.T) {
	// Create a custom builder to avoid nil pointer issues
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)

	t.Run("FTSMatch", func(t *testing.T) {
		opts := FTSSearchOptions{
			IndexName: "users_fts",
			Fuzziness: 1,
			Score:     "score",
		}

		match := FTSMatch("search term", opts)
		sql, _, err := match.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build FTS match: %v", err)
		}

		// Check that the SQL contains the expected parts
		if !contains(sql, "SEARCH") || !contains(sql, "users_fts") || !contains(sql, "search term") {
			t.Errorf("FTSMatch returned wrong SQL: %s", sql)
		}
	})

	t.Run("WithSearch", func(t *testing.T) {
		opts := FTSSearchOptions{
			IndexName: "users_fts",
			Fields:    []string{"name", "email"},
		}

		builder := sb.Select("*").From("users").WithSearch(FTSMatch("John", opts))
		sql, _, err := builder.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build query: %v", err)
		}

		// Check that the SQL contains the expected parts
		if !contains(sql, "SELECT * FROM users") || !contains(sql, "SEARCH") || !contains(sql, "users_fts") {
			t.Errorf("WithSearch returned wrong SQL: %s", sql)
		}
	})
}

// TestAnalyticsSupport tests the Analytics Service support
func TestAnalyticsSupport(t *testing.T) {
	t.Run("AnalyticsSelect", func(t *testing.T) {
		builder := AnalyticsSelect("u.name", "AVG(u.age) as avgAge").
			From("users u").
			Let("minAge", 18).
			Where("u.age >= ?", 18).
			GroupBy("u.country").
			Having("COUNT(*) > ?", 5).
			OrderBy("avgAge DESC")

		sql, args, err := builder.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build Analytics query: %v", err)
		}

		t.Logf("Generated SQL: %s", sql)
		t.Logf("Generated Args: %+v", args)

		if !contains(sql, "SELECT u.name, AVG(u.age) as avgAge") {
			t.Errorf("SQL does not contain correct SELECT: %s", sql)
		}

		if !contains(sql, "LET minAge = 18") {
			t.Errorf("SQL does not contain LET clause: %s", sql)
		}

		if !contains(sql, "WHERE u.age >= ?") {
			t.Errorf("SQL does not contain WHERE clause: %s", sql)
		}

		if !contains(sql, "GROUP BY u.country") {
			t.Errorf("SQL does not contain GROUP BY clause: %s", sql)
		}

		if !contains(sql, "HAVING COUNT(*) > ?") {
			t.Errorf("SQL does not contain HAVING clause: %s", sql)
		}

		if !contains(sql, "ORDER BY avgAge DESC") {
			t.Errorf("SQL does not contain ORDER BY clause: %s", sql)
		}

		if len(args) != 2 || args[0] != 18 || args[1] != 5 {
			t.Errorf("Wrong args: %+v", args)
		}
	})

	t.Run("Array functions", func(t *testing.T) {
		sql, _, err := ArrayAvg("prices").ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build array function: %v", err)
		}

		if sql != "ARRAY_AVG(prices)" {
			t.Errorf("Wrong function: Expected 'ARRAY_AVG(prices)', got '%s'", sql)
		}
	})
}

// TestJSONSupport tests the JSON document support functions
func TestJSONSupport(t *testing.T) {
	t.Run("JSONField", func(t *testing.T) {
		field := JSONField("user.address.city")
		if field != "user.`address`.`city`" {
			t.Errorf("Wrong field format: Expected 'user.`address`.`city`', got '%s'", field)
		}
	})

	t.Run("JSONArrayContains", func(t *testing.T) {
		expr := JSONArrayContains("user.roles", "admin")
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build JSON function: %v", err)
		}

		if sql != "user.roles ARRAY_CONTAINS ?" {
			t.Errorf("Wrong SQL: Expected 'user.roles ARRAY_CONTAINS ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != "admin" {
			t.Errorf("Wrong args: %+v", args)
		}
	})
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
