package n1qlizer

import (
	"strings"
	"testing"
)

// TestDelete tests the Delete query builder
func TestDelete(t *testing.T) {
	// Create a custom builder to avoid nil pointer issues
	sb := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Question)

	testCases := []struct {
		name     string
		builder  DeleteBuilder
		expected string
		args     []interface{}
	}{
		{
			name:     "Simple DELETE",
			builder:  sb.Delete("users").Where("id = ?", "user123"),
			expected: "DELETE FROM users WHERE id = ?",
			args:     []interface{}{"user123"},
		},
		{
			name:     "DELETE with multiple WHERE conditions",
			builder:  sb.Delete("users").Where("status = ?", "inactive").Where("last_login < ?", "2022-01-01"),
			expected: "DELETE FROM users WHERE status = ? AND last_login < ?",
			args:     []interface{}{"inactive", "2022-01-01"},
		},
		{
			name:     "DELETE with LIMIT clause",
			builder:  sb.Delete("users").Where("status = ?", "inactive").Suffix("LIMIT 100"),
			expected: "DELETE FROM users WHERE status = ? LIMIT 100",
			args:     []interface{}{"inactive"},
		},
		{
			name:     "DELETE with USE KEYS",
			builder:  sb.Delete("users").Prefix("USE KEYS ?", "user123"),
			expected: "USE KEYS ? DELETE FROM users",
			args:     []interface{}{"user123"},
		},
		{
			name:     "DELETE with RETURNING clause",
			builder:  sb.Delete("users").Where("id = ?", "user123").Suffix("RETURNING meta().id"),
			expected: "DELETE FROM users WHERE id = ? RETURNING meta().id",
			args:     []interface{}{"user123"},
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

			// Compare args
			for i, arg := range args {
				if arg != tc.args[i] {
					t.Errorf("Wrong arg at position %d: Expected %v, got %v", i, tc.args[i], arg)
				}
			}
		})
	}
}

// TestDeleteWithExpressions tests the Delete builder with expressions
func TestDeleteWithExpressions(t *testing.T) {
	// Test DELETE with Eq expression
	builder := StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Dollar).Delete("users").Where(Eq{"status": "inactive"})
	sql, args, err := builder.ToN1ql()
	if err != nil {
		t.Fatalf("Failed to build query: %v", err)
	}

	if !strings.Contains(sql, "DELETE FROM users") || !strings.Contains(sql, "WHERE status = $1") {
		t.Errorf("SQL does not contain required parts: %s", sql)
	}

	if len(args) != 1 || args[0] != "inactive" {
		t.Errorf("Wrong args: %+v", args)
	}

	// Test DELETE with multiple expressions
	builder = StatementBuilderType{builderMap: NewMap()}.PlaceholderFormat(Dollar).Delete("users").
		Where(And{
			Eq{"status": "inactive"},
			Lt{"last_login": "2022-01-01"},
		})

	sql, args, err = builder.ToN1ql()
	if err != nil {
		t.Fatalf("Failed to build query: %v", err)
	}

	if !strings.Contains(sql, "DELETE FROM users") || !strings.Contains(sql, "WHERE (status = $1 AND last_login < $2)") {
		t.Errorf("SQL does not contain required parts: %s", sql)
	}

	if len(args) != 2 || args[0] != "inactive" || args[1] != "2022-01-01" {
		t.Errorf("Wrong args: %+v", args)
	}
}
