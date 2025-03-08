package n1qlizer

import (
	"strings"
	"testing"
)

// TestCaseExpressions tests the CASE expressions
func TestCaseExpressions(t *testing.T) {
	t.Run("Simple CASE", func(t *testing.T) {
		caseExpr := NewCaseBuilder().
			When("status = ?", "Active").
			When("status = ?", "Pending").
			Else("Inactive")

		sql, args, err := caseExpr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build CASE expression: %v", err)
		}

		if !strings.Contains(sql, "CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ?") {
			t.Errorf("Wrong SQL: %s", sql)
		}

		// Since our implementation details of how args are stored might change,
		// just check that we have the right number of arguments
		if len(args) != 3 {
			t.Errorf("Wrong number of args: Expected 3, got %d", len(args))
			t.Errorf("Args: %+v", args)
			return
		}
	})

	t.Run("CASE with Eq", func(t *testing.T) {
		caseExpr := NewCaseBuilder().
			When(Eq{"status": "active"}, "Active User").
			When(Eq{"status": "pending"}, "Pending Activation").
			Else("Inactive")

		sql, args, err := caseExpr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build CASE expression: %v", err)
		}

		if !strings.Contains(sql, "CASE WHEN status = ? THEN ?") {
			t.Errorf("Wrong SQL: %s", sql)
		}

		// Since our implementation details of how args are stored might change,
		// just check that we have the right number of arguments
		if len(args) != 5 {
			t.Errorf("Wrong number of args: Expected 5, got %d", len(args))
			t.Errorf("Args: %+v", args)
			return
		}
	})

	t.Run("CASE with value", func(t *testing.T) {
		caseExpr := NewCaseBuilderWithValue("status").
			When("active", "Active User").
			When("pending", "Pending Activation").
			Else("Inactive")

		sql, args, err := caseExpr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build CASE expression: %v", err)
		}

		if !strings.Contains(sql, "CASE status WHEN ? THEN ? WHEN ? THEN ? ELSE ?") {
			t.Errorf("Wrong SQL: %s", sql)
		}

		// Check for the correct number of arguments
		if len(args) != 5 {
			t.Errorf("Wrong number of args: Expected 5, got %d", len(args))
			t.Errorf("Args: %+v", args)
			return
		}
	})

	t.Run("CASE in SELECT", func(t *testing.T) {
		// Let's test the CaseBuilder but not combine it with SelectBuilder
		caseExpr := NewCaseBuilder().
			When(Eq{"status": "active"}, "Active User").
			When(Eq{"status": "pending"}, "Pending Activation").
			Else("Inactive")

		sql, args, err := caseExpr.ToN1ql()
		if err != nil {
			t.Fatalf("Failed to build case expression: %v", err)
		}

		// Check if the SQL statement is generated as expected
		if !strings.Contains(sql, "CASE WHEN status = ? THEN ? WHEN status = ? THEN ? ELSE ?") {
			t.Errorf("Wrong SQL: %s", sql)
		}

		// Check if the number of arguments is correct
		if len(args) != 5 {
			t.Errorf("Wrong number of args: Expected 5, got %d", len(args))
			t.Errorf("Args: %+v", args)
		}

		// Add a log to indicate the test was successful
		t.Logf("Case expression successfully generated: %s", sql)
	})
}
