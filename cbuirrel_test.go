package n1qlizer

import (
	"strings"
	"testing"
)

func TestStatementBuilder(t *testing.T) {
	// Initialize a builder manually to avoid nil pointer
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Dollar)

	// Test that PlaceholderFormat is set correctly
	sql, args, err := sb.Select("*").From("users").Where("id = ?", 1).ToN1ql()
	if err != nil {
		t.Fatalf("Failed to build query: %v", err)
	}

	// Check that placeholder was replaced correctly
	if !strings.Contains(sql, "id = $1") {
		t.Errorf("Wrong SQL, expected 'id = $1', got: %s", sql)
	}

	if len(args) != 1 || args[0] != 1 {
		t.Errorf("Wrong args: %+v", args)
	}
}

func TestDebugN1qlizer(t *testing.T) {
	// Test DebugN1qlizer with a simple query
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)
	query := sb.Select("*").From("users").Where("name = ?", "John")
	debug := DebugN1qlizer(query)

	// We expect something like "SELECT * FROM users WHERE name = 'John'"
	if !strings.Contains(debug, "SELECT * FROM users WHERE name = 'John'") {
		t.Errorf("DebugN1qlizer returned wrong SQL: %s", debug)
	}
}

// TestExprs tests the expression builders (Eq, Lt, Gt, etc.)
func TestExprs(t *testing.T) {
	testCases := []struct {
		name     string
		expr     N1qlizer
		expected string
		args     []interface{}
	}{
		{
			name:     "Eq with single condition",
			expr:     Eq{"id": 1},
			expected: "id = ?",
			args:     []interface{}{1},
		},
		{
			name:     "Eq with multiple conditions",
			expr:     Eq{"id": 1, "name": "test"},
			expected: "id = ? AND name = ?",
			args:     []interface{}{1, "test"},
		},
		{
			name:     "Eq with nil value",
			expr:     Eq{"id": nil},
			expected: "id IS NULL",
			args:     []interface{}{},
		},
		{
			name:     "NotEq with single condition",
			expr:     NotEq{"id": 1},
			expected: "id <> ?",
			args:     []interface{}{1},
		},
		{
			name:     "NotEq with nil value",
			expr:     NotEq{"id": nil},
			expected: "id IS NOT NULL",
			args:     []interface{}{},
		},
		{
			name:     "Lt with value",
			expr:     Lt{"id": 1},
			expected: "id < ?",
			args:     []interface{}{1},
		},
		{
			name:     "Lte with value",
			expr:     Lte{"id": 1},
			expected: "id <= ?",
			args:     []interface{}{1},
		},
		{
			name:     "Gt with value",
			expr:     Gt{"id": 1},
			expected: "id > ?",
			args:     []interface{}{1},
		},
		{
			name:     "Gte with value",
			expr:     Gte{"id": 1},
			expected: "id >= ?",
			args:     []interface{}{1},
		},
		{
			name:     "And with multiple conditions",
			expr:     And{Eq{"id": 1}, Eq{"name": "test"}},
			expected: "(id = ? AND name = ?)",
			args:     []interface{}{1, "test"},
		},
		{
			name:     "Or with multiple conditions",
			expr:     Or{Eq{"id": 1}, Eq{"name": "test"}},
			expected: "(id = ? OR name = ?)",
			args:     []interface{}{1, "test"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql, args, err := tc.expr.ToN1ql()
			if err != nil {
				t.Fatalf("Failed to build expression: %v", err)
			}

			if sql != tc.expected {
				t.Errorf("Wrong SQL: \nExpected: %s\nGot: %s", tc.expected, sql)
			}

			if len(args) != len(tc.args) {
				t.Errorf("Wrong number of args: Expected %d, got %d", len(tc.args), len(args))
				t.Errorf("Args: %+v", args)
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

// TestSelect tests the Select query builder
func TestSelect(t *testing.T) {
	// Create a custom builder to avoid nil pointer issues
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)

	testCases := []struct {
		name     string
		builder  SelectBuilder
		expected string
		args     []interface{}
	}{
		{
			name:     "Simple SELECT",
			builder:  sb.Select("*").From("users"),
			expected: "SELECT * FROM users",
			args:     []interface{}{},
		},
		{
			name:     "SELECT with WHERE",
			builder:  sb.Select("id", "name").From("users").Where("id = ?", 1),
			expected: "SELECT id, name FROM users WHERE id = ?",
			args:     []interface{}{1},
		},
		{
			name:     "SELECT with multiple WHERE conditions",
			builder:  sb.Select("*").From("users").Where("id = ?", 1).Where("name = ?", "test"),
			expected: "SELECT * FROM users WHERE id = ? AND name = ?",
			args:     []interface{}{1, "test"},
		},
		{
			name:     "SELECT with ORDER BY",
			builder:  sb.Select("*").From("users").OrderBy("name ASC"),
			expected: "SELECT * FROM users ORDER BY name ASC",
			args:     []interface{}{},
		},
		{
			name:     "SELECT with LIMIT and OFFSET",
			builder:  sb.Select("*").From("users").Limit(10).Offset(5),
			expected: "SELECT * FROM users LIMIT 10 OFFSET 5",
			args:     []interface{}{},
		},
		{
			name:     "SELECT with JOIN",
			builder:  sb.Select("u.id", "e.email").From("users u").Join("emails e ON e.user_id = u.id"),
			expected: "SELECT u.id, e.email FROM users u JOIN emails e ON e.user_id = u.id",
			args:     []interface{}{},
		},
		{
			name:     "SELECT with USE KEYS",
			builder:  sb.Select("*").From("users").UseKeys("'user123'"),
			expected: "SELECT * FROM users USE KEYS 'user123'",
			args:     []interface{}{},
		},
		{
			name:     "SELECT with GROUP BY and HAVING",
			builder:  sb.Select("country", "COUNT(*) as count").From("users").GroupBy("country").Having("count > ?", 5),
			expected: "SELECT country, COUNT(*) as count FROM users GROUP BY country HAVING count > ?",
			args:     []interface{}{5},
		},
		{
			name:     "SELECT with Eq in WHERE",
			builder:  sb.Select("*").From("users").Where(Eq{"name": "test"}),
			expected: "SELECT * FROM users WHERE name = ?",
			args:     []interface{}{"test"},
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

// TestDollarFormat tests the Dollar placeholder format
func TestDollarFormat(t *testing.T) {
	testCases := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "Simple query",
			sql:      "SELECT * FROM users WHERE id = ?",
			expected: "SELECT * FROM users WHERE id = $1",
		},
		{
			name:     "Multiple placeholders",
			sql:      "SELECT * FROM users WHERE id = ? AND name = ?",
			expected: "SELECT * FROM users WHERE id = $1 AND name = $2",
		},
		{
			name:     "Escaped question mark",
			sql:      "SELECT * FROM users WHERE id = ? AND name LIKE '%??%'",
			expected: "SELECT * FROM users WHERE id = $1 AND name LIKE '%?%'",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := Dollar.ReplacePlaceholders(tc.sql)
			if err != nil {
				t.Fatalf("Failed to replace placeholders: %v", err)
			}

			if sql != tc.expected {
				t.Errorf("Wrong SQL: \nExpected: %s\nGot: %s", tc.expected, sql)
			}
		})
	}
}
