package n1qlizer

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONField(t *testing.T) {
	t.Run("Simple field", func(t *testing.T) {
		field := JSONField("user")
		if field != "user" {
			t.Errorf("Expected 'user', got '%s'", field)
		}
	})

	t.Run("Nested field", func(t *testing.T) {
		field := JSONField("user.name")
		if field != "user.`name`" {
			t.Errorf("Expected 'user.`name`', got '%s'", field)
		}
	})

	t.Run("Deeply nested field", func(t *testing.T) {
		field := JSONField("user.address.city")
		if field != "user.`address`.`city`" {
			t.Errorf("Expected 'user.`address`.`city`', got '%s'", field)
		}
	})
}

func TestJSONArrayContains(t *testing.T) {
	t.Run("Array contains string", func(t *testing.T) {
		expr := JSONArrayContains("tags", "important")
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build ARRAY_CONTAINS: %v", err)
		}

		if sql != "tags ARRAY_CONTAINS ?" {
			t.Errorf("Expected 'tags ARRAY_CONTAINS ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != "important" {
			t.Errorf("Expected args [important], got %v", args)
		}
	})

	t.Run("Array contains number", func(t *testing.T) {
		expr := JSONArrayContains("values", 42)
		_, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build ARRAY_CONTAINS: %v", err)
		}

		if len(args) != 1 || args[0] != 42 {
			t.Errorf("Expected args [42], got %v", args)
		}
	})
}

func TestJSONDocument(t *testing.T) {
	t.Run("Simple document", func(t *testing.T) {
		doc := AsDocument(map[string]interface{}{
			"name": "John",
			"age":  30,
		})

		sql, args, err := doc.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build JSON document: %v", err)
		}

		var parsedDoc map[string]interface{}
		if err := json.Unmarshal([]byte(sql), &parsedDoc); err != nil {
			t.Fatalf("Result is not valid JSON: %v", err)
		}

		if parsedDoc["name"] != "John" || parsedDoc["age"] != float64(30) {
			t.Errorf("JSON document doesn't match expected values: %v", parsedDoc)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Struct document", func(t *testing.T) {
		type Person struct {
			Name string `json:"name"`
			Age  int    `json:"age"`
		}

		doc := AsDocument(Person{
			Name: "John",
			Age:  30,
		})

		sql, _, err := doc.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build JSON document: %v", err)
		}

		var parsedDoc map[string]interface{}
		if err := json.Unmarshal([]byte(sql), &parsedDoc); err != nil {
			t.Fatalf("Result is not valid JSON: %v", err)
		}

		if parsedDoc["name"] != "John" || parsedDoc["age"] != float64(30) {
			t.Errorf("JSON document doesn't match expected values: %v", parsedDoc)
		}
	})
}

func TestJSONArray(t *testing.T) {
	t.Run("Array of values", func(t *testing.T) {
		expr := JSONArray("a", 1, true)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build JSON array: %v", err)
		}

		if sql != "ARRAY_CONSTRUCTOR(?,?,?)" {
			t.Errorf("Expected 'ARRAY_CONSTRUCTOR(?,?,?)', got '%s'", sql)
		}

		if len(args) != 3 || args[0] != "a" || args[1] != 1 || args[2] != true {
			t.Errorf("Expected args [a, 1, true], got %v", args)
		}
	})

	t.Run("Empty array", func(t *testing.T) {
		expr := JSONArray()
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build empty JSON array: %v", err)
		}

		if sql != "ARRAY_CONSTRUCTOR()" {
			t.Errorf("Expected 'ARRAY_CONSTRUCTOR()', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestJSONObject(t *testing.T) {
	t.Run("Simple object", func(t *testing.T) {
		expr := JSONObject("name", "John", "age", 30)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build JSON object: %v", err)
		}

		if !strings.Contains(sql, "\"name\": ?") || !strings.Contains(sql, "\"age\": ?") {
			t.Errorf("Expected JSON object with name and age fields, got '%s'", sql)
		}

		if len(args) != 2 || args[0] != "John" || args[1] != 30 {
			t.Errorf("Expected args [John, 30], got %v", args)
		}
	})

	t.Run("Empty object", func(t *testing.T) {
		expr := JSONObject()
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build empty JSON object: %v", err)
		}

		if sql != "{}" {
			t.Errorf("Expected '{}', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Object with nested values", func(t *testing.T) {
		expr := JSONObject(
			"name", "John",
			"address", JSONObject("city", "New York", "zip", "10001"),
		)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build nested JSON object: %v", err)
		}

		if !strings.Contains(sql, "\"address\": ?") {
			t.Errorf("Expected object with address field, got '%s'", sql)
		}

		// First arg is "John", second should be another expr
		if len(args) != 2 || args[0] != "John" {
			t.Errorf("Expected args [John, <expr>], got %v", args)
		}
	})

	t.Run("Invalid key-value pairs", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected JSONObject to panic with odd number of arguments")
			}
		}()

		JSONObject("name", "John", "age") // Missing value for "age" key
	})

	t.Run("Non-string key", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected JSONObject to panic with non-string key")
			}
		}()

		JSONObject(123, "value") // Non-string key
	})
}

func TestNestedField(t *testing.T) {
	t.Run("Simple field", func(t *testing.T) {
		field := Field("user")
		if field.String() != "user" {
			t.Errorf("Expected 'user', got '%s'", field.String())
		}
	})

	t.Run("Nested field", func(t *testing.T) {
		field := Field("user", "name")
		if field.String() != "user.`name`" {
			t.Errorf("Expected 'user.`name`', got '%s'", field.String())
		}
	})

	t.Run("Deeply nested field", func(t *testing.T) {
		field := Field("user", "address", "city")
		if field.String() != "user.`address`.`city`" {
			t.Errorf("Expected 'user.`address`.`city`', got '%s'", field.String())
		}
	})
}

func TestUseIndex(t *testing.T) {
	t.Run("Simple index", func(t *testing.T) {
		idx := UseIndex{IndexName: "idx_users"}
		sql, args, err := idx.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build USE INDEX: %v", err)
		}

		if sql != "USE INDEX (`idx_users`)" {
			t.Errorf("Expected 'USE INDEX (`idx_users`)', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("GSI index", func(t *testing.T) {
		idx := UseIndexGSI("idx_users")
		sql, _, err := idx.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build USE INDEX GSI: %v", err)
		}

		if sql != "USE INDEX (`idx_users` USING GSI)" {
			t.Errorf("Expected 'USE INDEX (`idx_users` USING GSI)', got '%s'", sql)
		}
	})

	t.Run("VIEW index", func(t *testing.T) {
		idx := UseIndexView("idx_users")
		sql, _, err := idx.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build USE INDEX VIEW: %v", err)
		}

		if sql != "USE INDEX (`idx_users` USING VIEW)" {
			t.Errorf("Expected 'USE INDEX (`idx_users` USING VIEW)', got '%s'", sql)
		}
	})
}

func TestSubDocument(t *testing.T) {
	t.Run("Simple subdocument", func(t *testing.T) {
		doc := map[string]interface{}{
			"name": "John",
			"age":  30,
		}

		expr := SubDocument(doc)
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build subdocument: %v", err)
		}

		if sql != "?" {
			t.Errorf("Expected '?', got '%s'", sql)
		}

		if len(args) != 1 {
			t.Errorf("Expected 1 argument, got %d", len(args))
		}
	})

	t.Run("Nested subdocument", func(t *testing.T) {
		doc := map[string]interface{}{
			"user": map[string]interface{}{
				"name": "John",
				"age":  30,
			},
		}

		expr := SubDocument(doc, "user", "name")
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build nested subdocument: %v", err)
		}

		if sql != "?->`user`.`name`" {
			t.Errorf("Expected '?->`user`.`name`', got '%s'", sql)
		}

		if len(args) != 1 {
			t.Errorf("Expected 1 argument, got %d", len(args))
		}
	})
}
