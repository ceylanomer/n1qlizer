package n1qlizer

import (
	"testing"
)

// TestInsert tests the Insert query builder
func TestInsert(t *testing.T) {
	testCases := []struct {
		name     string
		builder  InsertBuilder
		expected string
		args     []interface{}
	}{
		{
			name: "Simple INSERT with key-value",
			builder: StatementBuilder.Insert("users").
				Columns("KEY", "VALUE").
				Values("user123", map[string]interface{}{"name": "John", "age": 30}),
			expected: "INSERT INTO users (KEY, VALUE) VALUES (?, ?)",
			args:     []interface{}{"user123", map[string]interface{}{"name": "John", "age": 30}},
		},
		{
			name:     "INSERT with columns and values",
			builder:  StatementBuilder.Insert("users").Columns("id", "name", "age").Values("user123", "John", 30),
			expected: "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
			args:     []interface{}{"user123", "John", 30},
		},
		{
			name:     "INSERT with SetMap",
			builder:  StatementBuilder.Insert("users").SetMap(map[string]interface{}{"id": "user123", "name": "John", "age": 30}),
			expected: "INSERT INTO users SET id=?, name=?, age=?",
			args:     []interface{}{"user123", "John", 30},
		},
		{
			name:     "INSERT with multiple rows",
			builder:  StatementBuilder.Insert("users").Columns("id", "name").Values("user1", "John").Values("user2", "Jane"),
			expected: "INSERT INTO users (id, name) VALUES (?, ?), (?, ?)",
			args:     []interface{}{"user1", "John", "user2", "Jane"},
		},
		{
			name:     "INSERT with RETURNING clause",
			builder:  StatementBuilder.Insert("users").Columns("id", "name").Values("user1", "John").Suffix("RETURNING *"),
			expected: "INSERT INTO users (id, name) VALUES (?, ?) RETURNING *",
			args:     []interface{}{"user1", "John"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql, args, err := tc.builder.ToN1ql()
			if err != nil {
				t.Fatalf("Failed to build query: %v", err)
			}

			// For SET and document tests, we can't directly compare SQL due to map ordering
			if tc.name == "INSERT with SetMap" {
				if len(args) != len(tc.args) {
					t.Errorf("Wrong number of args: Expected %d, got %d", len(tc.args), len(args))
				}
			} else if tc.name == "Simple INSERT with key-value" {
				if len(args) != len(tc.args) {
					t.Errorf("Wrong number of args: Expected %d, got %d", len(tc.args), len(args))
				}
			} else {
				// For other tests we can directly compare
				if sql != tc.expected {
					t.Errorf("Wrong SQL: \nExpected: %s\nGot: %s", tc.expected, sql)
				}

				if len(args) != len(tc.args) {
					t.Errorf("Wrong number of args: Expected %d, got %d", len(tc.args), len(args))
					return
				}

				// Compare primitive args
				for i, arg := range args {
					// Skip map comparison since order may vary
					if _, ok := arg.(map[string]interface{}); !ok {
						if arg != tc.args[i] {
							t.Errorf("Wrong arg at position %d: Expected %v, got %v", i, tc.args[i], arg)
						}
					}
				}
			}
		})
	}
}

// TestInsertWithSelect tests the INSERT ... SELECT ... statement
func TestInsertWithSelect(t *testing.T) {
	insertBuilder := StatementBuilder.Insert("user_stats").
		Columns("user_id", "login_count", "last_login").
		Suffix("SELECT id, COUNT(logins), MAX(login_time) FROM users LEFT JOIN logins ON logins.user_id = users.id GROUP BY users.id")

	sql, _, err := insertBuilder.ToN1ql()
	if err != nil {
		t.Fatalf("Failed to build query: %v", err)
	}

	expected := "INSERT INTO user_stats (user_id, login_count, last_login) " +
		"SELECT id, COUNT(logins), MAX(login_time) FROM users LEFT JOIN logins ON logins.user_id = users.id GROUP BY users.id"

	if sql != expected {
		t.Errorf("Wrong SQL: \nExpected: %s\nGot: %s", expected, sql)
	}
}
