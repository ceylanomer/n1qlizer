# n1qlizer - Fluent Couchbase N1QL Query Generator for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/ceylanomer/n1qlizer.svg)](https://pkg.go.dev/github.com/ceylanomer/n1qlizer)

## Overview

**n1qlizer** is a fluent query builder for Couchbase's N1QL language, inspired by [Squirrel](https://github.com/Masterminds/squirrel). It helps you build N1QL queries from composable parts using a clean, readable syntax.

Instead of concatenating strings or using complex template engines, n1qlizer lets you build queries programmatically, making them more maintainable and less error-prone. It handles parameter placeholders, escaping, and query composition while providing Couchbase-specific features not found in standard SQL builders.

Key benefits:
- Type-safe, fluent API for building N1QL queries
- Generic types for improved type safety (Go 1.18+)
- Automatic parameter binding and placeholder generation 
- Support for all major N1QL operations (SELECT, INSERT, UPDATE, DELETE, UPSERT)
- Integration with Couchbase-specific features (USE KEYS, NEST/UNNEST, FTS, etc.)
- Ability to execute queries directly with the Couchbase SDK

## Installation & Setup

### Prerequisites
- Go 1.18 or higher
- A Couchbase database (for executing queries)

### Installing the Package

```bash
go get github.com/ceylanomer/n1qlizer
```

### Import in Your Project

```go
import "github.com/ceylanomer/n1qlizer"
```

## Go 1.18+ Features

n1qlizer fully leverages Go 1.18 features:

### Generics

The library uses generics extensively to provide type safety throughout all operations:

```go
// Type-safe list creation
userList := n1qlizer.NewGenericList[User]()

// Type-safe map operations
userMap := n1qlizer.NewGenericMap[User]()

// Type-safe builder operations
userBuilder := n1qlizer.Set[MyBuilder, User](builder, "user", userObject)
```

### Builder Methods with Type Parameters

All builder methods now use Go's generic type parameters for increased type safety:

```go
// Previous approach (pre-Go 1.18, no longer supported)
// userBuilder := userBuilder.Set("name", "John").(UserBuilder)

// New generic approach (Go 1.18+)
userBuilder := userBuilder.Set[UserBuilder, string]("name", "John")
```

For example, when building queries:

```go
// Type-safe builder with generics
selectBuilder := n1qlizer.SelectBuilder{}.
    From[n1qlizer.SelectBuilder]("users").
    Where[n1qlizer.SelectBuilder]("status = ?", "active")
```

### 'any' Type

The library uses the `any` type alias instead of `interface{}` for improved readability:

```go
// Function signatures use 'any' instead of interface{}
func Execute(query string, args ...any) (QueryResult, error)

// Defining maps with the 'any' type
data := map[string]any{
    "name": "John",
    "age": 30,
    "roles": []string{"admin", "user"},
}
```

## Usage Examples

### Basic Query Building

#### SELECT Queries

```go
// Build a simple SELECT query
users := n1qlizer.Select("*").From("users").Where(n1qlizer.Eq{"type": "user"})
sql, args, err := users.ToN1ql()
// sql == "SELECT * FROM users WHERE type = ?"
// args == []any{"user"}

// With multiple conditions
query := n1qlizer.Select("name", "email").
    From("users").
    Where(n1qlizer.And{
        n1qlizer.Eq{"status": "active"},
        n1qlizer.Gt{"age": 18},
    }).
    OrderBy("name ASC").
    Limit(10)
sql, args, err := query.ToN1ql()
```

#### INSERT Queries

```go
sql, args, err := n1qlizer.
    Insert("users").
    Columns("id", "name", "age").
    Values("user123", "Joe", 30).
    Values("user456", "Larry", n1qlizer.Expr("? + 5", 12)).
    ToN1ql()
// sql == "INSERT INTO users (id,name,age) VALUES (?,?,?),(?,?,? + 5)"
// args == []any{"user123", "Joe", 30, "user456", "Larry", 12}
```

#### UPDATE Queries

```go
sql, args, err := n1qlizer.
    Update("users").
    UseKeys("'user123'").
    Set("name", "Moe Howard").
    Set("updated_at", n1qlizer.Expr("NOW()")).
    ToN1ql()
// sql == "UPDATE users USE KEYS 'user123' SET name = ?, updated_at = NOW()"
// args == []any{"Moe Howard"}
```

#### DELETE Queries

```go
sql, args, err := n1qlizer.
    Delete("users").
    Where(n1qlizer.Eq{"status": "inactive"}).
    Limit(10).
    ToN1ql()
// sql == "DELETE FROM users WHERE status = ? LIMIT 10"
// args == []any{"inactive"}
```

### Couchbase-Specific Features

#### UPSERT Operation

```go
// Couchbase-specific UPSERT operation - preferred over INSERT
sql, args, err := n1qlizer.
    Upsert("users").
    Document("user123", map[string]any{
        "name": "Joe Smith",
        "email": "joe@example.com",
        "roles": []string{"admin", "user"},
    }).
    ToN1ql()
// sql == "UPSERT INTO users (KEY, VALUE) VALUES (?, ?)"
```

#### USE KEYS Clause

```go
users := n1qlizer.Select("*").From("users").UseKeys("'user123', 'user456'")
sql, args, err := users.ToN1ql()
// sql == "SELECT * FROM users USE KEYS 'user123', 'user456'"
```

#### USE INDEX Clause

```go
users := n1qlizer.Select("*").
    From("users").
    Prefix(n1qlizer.UseIndexGSI("users_by_email")).
    Where(n1qlizer.Eq{"email": "user@example.com"})
sql, args, err := users.ToN1ql()
// sql == "USE INDEX (`users_by_email` USING GSI) SELECT * FROM users WHERE email = ?"
// args == []any{"user@example.com"}
```

#### NEST and UNNEST Operations

```go
// NEST operation joins a document with another bucket
sql, args, err := n1qlizer.
    Select("u.name", "o.orderDate", "o.total").
    From("users AS u").
    NestClause(n1qlizer.Nest("orders").As("o").OnKeys("u.orderIds")).
    Where(n1qlizer.Gt{"o.total": 100}).
    ToN1ql()
// sql == "SELECT u.name, o.orderDate, o.total FROM users AS u NEST orders AS o ON KEYS u.orderIds WHERE o.total > ?"
// args == []any{100}

// UNNEST flattens an array within a document
sql, args, err := n1qlizer.
    Select("u.name", "t").
    From("users AS u").
    UnnestClause(n1qlizer.Unnest("u.tags").As("t")).
    Where(n1qlizer.Eq{"t": "admin"}).
    ToN1ql()
// sql == "SELECT u.name, t FROM users AS u UNNEST u.tags AS t WHERE t = ?"
// args == []any{"admin"}
```

#### Analytics Queries

```go
// Analytics queries with the LET clause
sql, args, err := n1qlizer.
    AnalyticsSelect("u.name", "AVG(u.age) as avgAge").
    From("users u").
    Let("minAge", 18).
    Where("u.age >= ?", 18).
    GroupBy("u.country").
    Having("COUNT(*) > ?", 5).
    OrderBy("avgAge DESC").
    ToN1ql()
// sql == "SELECT u.name, AVG(u.age) as avgAge LET minAge = ? FROM users u WHERE u.age >= ? GROUP BY u.country HAVING COUNT(*) > ? ORDER BY avgAge DESC"
// args == []any{18, 18, 5}
```

#### Full Text Search (FTS)

```go
// FTS search with options
opts := n1qlizer.FTSSearchOptions{
    IndexName: "users_fts",
    Fields:    []string{"name", "email"},
    Fuzziness: 1,
}

sql, args, err := n1qlizer.
    Select("*").
    From("users").
    WithSearch(n1qlizer.FTSMatch("John Smith", opts)).
    ToN1ql()
// sql == "SELECT * FROM users WHERE SEARCH(users_fts, { 'query': { 'match': 'John Smith' }, 'fields': ['name', 'email'], 'fuzziness': 1 })"
```

## Working with JSON

n1qlizer provides numerous helpers for working with JSON documents:

```go
// Create JSON documents
doc := n1qlizer.AsDocument(map[string]any{
    "name": "John Smith",
    "age": 30,
})

// Access nested fields
field := n1qlizer.JSONField("user.address.city") // "user.`address`.`city`"

// Check if an array contains a value
expr := n1qlizer.JSONArrayContains("user.roles", "admin")
// "user.roles ARRAY_CONTAINS ?"

// Create JSON arrays and objects
arr := n1qlizer.JSONArray("value1", "value2", 3)
obj := n1qlizer.JSONObject("name", "John", "age", 30)
```

## Executing Queries

n1qlizer can execute queries directly with a Couchbase connection:

```go
// Create a builder with a Couchbase connection
sb := n1qlizer.StatementBuilder.RunWith(couchbaseCluster)

// Build and execute the query in one step
result, err := sb.Select("*").
    From("users").
    Where(n1qlizer.Eq{"status": "active"}).
    Execute()

// Process results
for result.Next() {
    var user User
    if err := result.Row(&user); err != nil {
        // Handle error
    }
    // Use user...
}
```

## Migration from Pre-1.18 Code

If you're upgrading from a pre-1.18 version, here are the key changes:

1. Replace `interface{}` with `any` in your code
2. Update builder method calls to use the generic syntax: 
   ```go
   // Old
   builder.Set("field", value).(MyBuilder)
   
   // New
   builder.Set[MyBuilder, ValueType]("field", value)
   ```
3. If you've created custom builders, update them to use generics

## License

MIT License - See LICENSE file for details.