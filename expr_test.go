package n1qlizer

import (
	"strings"
	"testing"
)

func TestExpr(t *testing.T) {
	t.Run("String expression", func(t *testing.T) {
		e := Expr("name = ?", "test")
		sql, args, err := e.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build expression: %v", err)
		}

		if sql != "name = ?" {
			t.Errorf("Expected 'name = ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != "test" {
			t.Errorf("Expected args [test], got %v", args)
		}
	})

	t.Run("N1qlizer as argument", func(t *testing.T) {
		inner := Expr("age > ?", 30)
		e := Expr("AND name = ? AND ?", "test", inner)
		sql, args, err := e.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build nested expression: %v", err)
		}

		if sql != "AND name = ? AND age > ?" {
			t.Errorf("Expected 'AND name = ? AND age > ?', got '%s'", sql)
		}

		if len(args) != 2 || args[0] != "test" || args[1] != 30 {
			t.Errorf("Expected args [test, 30], got %v", args)
		}
	})

	t.Run("Not enough arguments", func(t *testing.T) {
		e := Expr("name = ? AND age = ?", "test")
		_, _, err := e.ToN1ql()
		if err == nil {
			t.Error("Expected error for not enough arguments, got nil")
		}
	})

	t.Run("N1qlizer as first argument", func(t *testing.T) {
		inner := Expr("age > ?", 30)
		e := Expr(inner)
		sql, args, err := e.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build expression: %v", err)
		}

		if sql != "age > ?" {
			t.Errorf("Expected 'age > ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != 30 {
			t.Errorf("Expected args [30], got %v", args)
		}
	})

	t.Run("Non-string first argument", func(t *testing.T) {
		e := Expr(123)
		sql, args, err := e.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build expression: %v", err)
		}

		if sql != "123" {
			t.Errorf("Expected '123', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestAlias(t *testing.T) {
	t.Run("Simple alias", func(t *testing.T) {
		e := Alias(Expr("COUNT(*)"), "total")
		sql, args, err := e.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build alias expression: %v", err)
		}

		if sql != "(COUNT(*)) AS total" {
			t.Errorf("Expected '(COUNT(*)) AS total', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Alias with arguments", func(t *testing.T) {
		e := Alias(Expr("name = ?", "test"), "name_filter")
		sql, args, err := e.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build alias expression: %v", err)
		}

		if sql != "(name = ?) AS name_filter" {
			t.Errorf("Expected '(name = ?) AS name_filter', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != "test" {
			t.Errorf("Expected args [test], got %v", args)
		}
	})
}

func TestEq(t *testing.T) {
	t.Run("Simple equality", func(t *testing.T) {
		eq := Eq{"name": "test", "age": 30}
		sql, args, err := eq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build equality expression: %v", err)
		}

		// Order is alphabetical by key, so age comes before name
		if sql != "age = ? AND name = ?" {
			t.Errorf("Expected 'age = ? AND name = ?', got '%s'", sql)
		}

		if len(args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(args))
		}
	})

	t.Run("Equality with nil", func(t *testing.T) {
		eq := Eq{"name": nil}
		sql, args, err := eq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build equality expression: %v", err)
		}

		if sql != "name IS NULL" {
			t.Errorf("Expected 'name IS NULL', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Equality with slice", func(t *testing.T) {
		eq := Eq{"id": []interface{}{1, 2, 3}}
		sql, args, err := eq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build equality expression: %v", err)
		}

		if sql != "id IN (?,?,?)" {
			t.Errorf("Expected 'id IN (?,?,?)', got '%s'", sql)
		}

		if len(args) != 3 {
			t.Errorf("Expected 3 args, got %d", len(args))
		}
	})

	t.Run("Equality with empty slice", func(t *testing.T) {
		eq := Eq{"id": []interface{}{}}
		sql, args, err := eq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build equality expression: %v", err)
		}

		if sql != "1=0" {
			t.Errorf("Expected '1=0' (always false), got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Equality with N1qlizer", func(t *testing.T) {
		subExpr := Expr("LENGTH(?) > 5", "test_value")
		eq := Eq{"is_valid": subExpr}
		sql, args, err := eq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build equality expression: %v", err)
		}

		if sql != "is_valid = LENGTH(?) > 5" {
			t.Errorf("Expected 'is_valid = LENGTH(?) > 5', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != "test_value" {
			t.Errorf("Expected args [test_value], got %v", args)
		}
	})

	t.Run("Empty Eq", func(t *testing.T) {
		eq := Eq{}
		sql, args, err := eq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build equality expression: %v", err)
		}

		if sql != "" {
			t.Errorf("Expected empty string, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestNotEq(t *testing.T) {
	t.Run("Simple inequality", func(t *testing.T) {
		neq := NotEq{"name": "test", "age": 30}
		sql, args, err := neq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build inequality expression: %v", err)
		}

		// Order is alphabetical by key, so age comes before name
		if sql != "age <> ? AND name <> ?" {
			t.Errorf("Expected 'age <> ? AND name <> ?', got '%s'", sql)
		}

		if len(args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(args))
		}
	})

	t.Run("Inequality with nil", func(t *testing.T) {
		neq := NotEq{"name": nil}
		sql, args, err := neq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build inequality expression: %v", err)
		}

		if sql != "name IS NOT NULL" {
			t.Errorf("Expected 'name IS NOT NULL', got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Inequality with slice", func(t *testing.T) {
		neq := NotEq{"id": []interface{}{1, 2, 3}}
		sql, args, err := neq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build inequality expression: %v", err)
		}

		if sql != "id NOT IN (?,?,?)" {
			t.Errorf("Expected 'id NOT IN (?,?,?)', got '%s'", sql)
		}

		if len(args) != 3 {
			t.Errorf("Expected 3 args, got %d", len(args))
		}
	})

	t.Run("Inequality with empty slice", func(t *testing.T) {
		neq := NotEq{"id": []interface{}{}}
		sql, args, err := neq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build inequality expression: %v", err)
		}

		if sql != "1=1" {
			t.Errorf("Expected '1=1' (always true), got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("Empty NotEq", func(t *testing.T) {
		neq := NotEq{}
		sql, args, err := neq.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build inequality expression: %v", err)
		}

		if sql != "" {
			t.Errorf("Expected empty string, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})
}

func TestComparisonExpressions(t *testing.T) {
	t.Run("Lt", func(t *testing.T) {
		lt := Lt{"age": 30}
		sql, args, err := lt.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build Lt expression: %v", err)
		}

		if sql != "age < ?" {
			t.Errorf("Expected 'age < ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != 30 {
			t.Errorf("Expected args [30], got %v", args)
		}
	})

	t.Run("Lte", func(t *testing.T) {
		lte := Lte{"age": 30}
		sql, args, err := lte.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build Lte expression: %v", err)
		}

		if sql != "age <= ?" {
			t.Errorf("Expected 'age <= ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != 30 {
			t.Errorf("Expected args [30], got %v", args)
		}
	})

	t.Run("Gt", func(t *testing.T) {
		gt := Gt{"age": 30}
		sql, args, err := gt.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build Gt expression: %v", err)
		}

		if sql != "age > ?" {
			t.Errorf("Expected 'age > ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != 30 {
			t.Errorf("Expected args [30], got %v", args)
		}
	})

	t.Run("Gte", func(t *testing.T) {
		gte := Gte{"age": 30}
		sql, args, err := gte.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build Gte expression: %v", err)
		}

		if sql != "age >= ?" {
			t.Errorf("Expected 'age >= ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != 30 {
			t.Errorf("Expected args [30], got %v", args)
		}
	})

	t.Run("Comparison with nil", func(t *testing.T) {
		lt := Lt{"age": nil}
		_, _, err := lt.ToN1ql()
		if err == nil {
			t.Error("Expected error when using nil with comparison operator, got nil")
		}
	})

	t.Run("Multiple comparisons", func(t *testing.T) {
		lt := Lt{"age": 30, "price": 100}
		sql, args, err := lt.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build multiple Lt expression: %v", err)
		}

		if sql != "age < ? AND price < ?" {
			t.Errorf("Expected 'age < ? AND price < ?', got '%s'", sql)
		}

		if len(args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(args))
		}
	})
}

func TestAndOr(t *testing.T) {
	t.Run("And", func(t *testing.T) {
		and := And{
			Eq{"name": "test"},
			Gt{"age": 30},
		}
		sql, args, err := and.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build And expression: %v", err)
		}

		if !strings.Contains(sql, "name = ?") || !strings.Contains(sql, "age > ?") {
			t.Errorf("Expected to contain 'name = ?' and 'age > ?', got '%s'", sql)
		}

		if len(args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(args))
		}
	})

	t.Run("Or", func(t *testing.T) {
		or := Or{
			Eq{"name": "test"},
			Gt{"age": 30},
		}
		sql, args, err := or.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build Or expression: %v", err)
		}

		if !strings.Contains(sql, "name = ?") || !strings.Contains(sql, "age > ?") {
			t.Errorf("Expected to contain 'name = ?' and 'age > ?', got '%s'", sql)
		}

		if !strings.Contains(sql, " OR ") {
			t.Errorf("Expected to contain OR operator, got '%s'", sql)
		}

		if len(args) != 2 {
			t.Errorf("Expected 2 args, got %d", len(args))
		}
	})

	t.Run("Empty And", func(t *testing.T) {
		and := And{}
		sql, args, err := and.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build empty And expression: %v", err)
		}

		if sql != "" {
			t.Errorf("Expected empty string for empty And, got '%s'", sql)
		}

		if len(args) != 0 {
			t.Errorf("Expected empty args, got %v", args)
		}
	})

	t.Run("And with one element", func(t *testing.T) {
		and := And{Eq{"name": "test"}}
		sql, args, err := and.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build And with one expression: %v", err)
		}

		if sql != "name = ?" {
			t.Errorf("Expected 'name = ?', got '%s'", sql)
		}

		if len(args) != 1 || args[0] != "test" {
			t.Errorf("Expected args [test], got %v", args)
		}
	})

	t.Run("Nested And/Or", func(t *testing.T) {
		expr := And{
			Eq{"name": "test"},
			Or{
				Gt{"age": 30},
				Lt{"age": 10},
			},
		}
		sql, args, err := expr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build nested And/Or expression: %v", err)
		}

		if !strings.Contains(sql, "name = ?") || !strings.Contains(sql, "age > ?") || !strings.Contains(sql, "age < ?") {
			t.Errorf("Expected to contain all conditions, got '%s'", sql)
		}

		if !strings.Contains(sql, " AND ") || !strings.Contains(sql, " OR ") {
			t.Errorf("Expected to contain both AND and OR operators, got '%s'", sql)
		}

		if len(args) != 3 {
			t.Errorf("Expected 3 args, got %d", len(args))
		}
	})
}
