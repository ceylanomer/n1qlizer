# n1qlizer - Fluent Couchbase N1QL Query Generator for Go

[![Go Reference](https://pkg.go.dev/badge/github.com/ceylanomer/n1qlizer.svg)](https://pkg.go.dev/github.com/ceylanomer/n1qlizer)

## Overview

**n1qlizer** is a fluent query builder for Couchbase's N1QL language, inspired by [Squirrel](https://github.com/Masterminds/squirrel). It helps you build N1QL queries from composable parts using a clean, readable syntax.

Instead of concatenating strings or using complex template engines, n1qlizer lets you build queries programmatically, making them more maintainable and less error-prone. It handles parameter placeholders, escaping, and query composition while providing Couchbase-specific features not found in standard SQL builders.

Key benefits:
- Type-safe, fluent API for building N1QL queries
- Automatic parameter binding and placeholder generation 
- Support for all major N1QL operations (SELECT, INSERT, UPDATE, DELETE, UPSERT)
- Integration with Couchbase-specific features (USE KEYS, NEST/UNNEST, FTS, etc.)
- Ability to execute queries directly with the Couchbase SDK

## Installation & Setup

### Prerequisites
- Go 1.14 or higher
- A Couchbase database (for executing queries)

### Installing the Package

```bash
go get github.com/ceylanomer/n1qlizer
```

### Import in Your Project

```go
import "github.com/ceylanomer/n1qlizer"
```

## Usage Examples

### Basic Query Building

#### SELECT Queries

```go
// Build a simple SELECT query
users := n1qlizer.Select("*").From("users").Where(n1qlizer.Eq{"type": "user"})
sql, args, err := users.ToN1ql()
// sql == "SELECT * FROM users WHERE type = $1"
// args == []interface{}{"user"}

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
    Values("user456", "Larry", n1qlizer.Expr("$1 + 5", 12)).
    ToN1ql()
// sql == "INSERT INTO users (id,name,age) VALUES ($1,$2,$3),($4,$5,$6 + 5)"
```

#### UPDATE Queries

```go
sql, args, err := n1qlizer.
    Update("users").
    UseKeys("'user123'").
    Set("name", "Moe Howard").
    Set("updated_at", n1qlizer.Expr("NOW()")).
    ToN1ql()
// sql == "UPDATE users USE KEYS 'user123' SET name = $1, updated_at = NOW()"
```

#### DELETE Queries

```go
sql, args, err := n1qlizer.
    Delete("users").
    Where(n1qlizer.Eq{"status": "inactive"}).
    Limit(10).
    ToN1ql()
// sql == "DELETE FROM users WHERE status = $1 LIMIT 10"
```

### Couchbase-Specific Features

#### UPSERT Operation

```go
// Couchbase-specific UPSERT operation - preferred over INSERT
sql, args, err := n1qlizer.
    Upsert("users").
    Document("user123", map[string]interface{}{
        "name": "Joe Smith",
        "email": "joe@example.com",
        "roles": []string{"admin", "user"},
    }).
    ToN1ql()
// sql == "UPSERT INTO users (KEY, VALUE) VALUES ($1, $2)"
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
// sql == "USE INDEX (`users_by_email` USING GSI) SELECT * FROM users WHERE email = $1"
```

#### NEST and UNNEST Operations

```go
// NEST operation joins a document with another bucket
sql, args, err := n1qlizer.
    Select("u.name", "o.orderDate", "o.total").
    From("users AS u").
    Nest(n1qlizer.Nest("orders").As("o").OnKeys("u.orderIds")).
    Where(n1qlizer.Gt{"o.total": 100}).
    ToN1ql()
// sql == "SELECT u.name, o.orderDate, o.total FROM users AS u NEST orders AS o ON KEYS u.orderIds WHERE o.total > $1"

// UNNEST flattens an array within a document
sql, args, err := n1qlizer.
    Select("u.name", "t").
    From("users AS u").
    Unnest("u.tags").As("t").
    Where(n1qlizer.Eq{"t": "admin"}).
    ToN1ql()
// sql == "SELECT u.name, t FROM users AS u UNNEST u.tags AS t WHERE t = $1"
```

#### Full-Text Search Integration

```go
// Using full-text search (FTS) with N1QL
opts := n1qlizer.FTSSearchOptions{
    IndexName: "users_fts",
    Fuzziness: 1,
    Boost: 2.0,
    Score: "score",
    Fields: []string{"name", "email"},
}

sql, args, err := n1qlizer.
    Select("*").
    From("users").
    Where(n1qlizer.FTSMatch("smith", opts)).
    OrderBy("score DESC").
    ToN1ql()
```

#### JSON Document Support

```go
// Query with JSON paths
sql, args, err := n1qlizer.
    Select("*").
    From("users").
    Where(n1qlizer.Eq{n1qlizer.JSONField("user.address.city"): "New York"}).
    ToN1ql()
// sql == "SELECT * FROM users WHERE user.`address`.`city` = $1"

// Check if JSON array contains value
sql, args, err := n1qlizer.
    Select("*").
    From("users").
    Where(n1qlizer.JSONArrayContains("user.tags", "admin")).
    ToN1ql()
// sql == "SELECT * FROM users WHERE ARRAY_CONTAINS(user.tags, $1)"
```

### Executing Queries

To execute queries directly with Couchbase SDK, you need to implement the QueryRunner interface:

```go
import (
    "github.com/couchbase/gocb/v2"
    "github.com/ceylanomer/n1qlizer"
)

// Implement a Couchbase Runner
type CouchbaseRunner struct {
    cluster *gocb.Cluster
}

func (r *CouchbaseRunner) Execute(query string, args ...interface{}) (n1qlizer.QueryResult, error) {
    // Execute the query using the Couchbase SDK
    result, err := r.cluster.Query(query, &gocb.QueryOptions{
        PositionalParameters: args,
    })
    if err != nil {
        return nil, err
    }
    
    return &CouchbaseQueryResult{result}, nil
}

// Implement the QueryResult interface
type CouchbaseQueryResult struct {
    result *gocb.QueryResult
}

func (r *CouchbaseQueryResult) One(valuePtr interface{}) error {
    return r.result.One(valuePtr)
}

func (r *CouchbaseQueryResult) All(slicePtr interface{}) error {
    return r.result.All(slicePtr)
}

func (r *CouchbaseQueryResult) Close() error {
    return r.result.Close()
}

// Usage with runner
func main() {
    // Set up your Couchbase cluster and create a runner
    cluster, err := gocb.Connect("couchbase://localhost", gocb.ClusterOptions{
        Username: "Administrator",
        Password: "password",
    })
    if err != nil {
        panic(err)
    }
    
    runner := &CouchbaseRunner{cluster: cluster}
    
    // Build and execute a query
    result, err := n1qlizer.
        Select("*").
        From("users").
        Where(n1qlizer.Eq{"type": "admin"}).
        RunWith(runner).
        Execute()
    
    if err != nil {
        panic(err)
    }
    
    // Process results
    var adminUsers []interface{}
    err = result.All(&adminUsers)
    if err != nil {
        panic(err)
    }
    
    // Do something with adminUsers
    fmt.Printf("Found %d admin users\n", len(adminUsers))
}
```

## Configuration

### Placeholder Format

n1qlizer defaults to using positional parameters with dollar signs ($1, $2, etc.) which is compatible with Couchbase N1QL. You can change this behavior:

```go
// Get a new statement builder with a specific placeholder format
sb := n1qlizer.StatementBuilder.PlaceholderFormat(n1qlizer.Dollar)

// Create a query using this builder
query := sb.Select("*").From("users")
```

### Debugging Queries

You can use `DebugN1qlizer` to see the actual query with parameters interpolated:

```go
query := n1qlizer.Select("*").From("users").Where(n1qlizer.Eq{"id": "user123"})
debug := n1qlizer.DebugN1qlizer(query)
fmt.Println(debug)
// Outputs: SELECT * FROM users WHERE id = 'user123'
```

> ⚠️ **Warning**: Never use `DebugN1qlizer` output directly in production code as it doesn't properly escape values and could lead to N1QL injection vulnerabilities.

## Best Practices & Limitations

### Best Practices

1. **Parameter Binding**: Always use parameter binding with placeholders instead of string concatenation to prevent N1QL injection:

   ```go
   // Good
   query := n1qlizer.Select("*").From("users").Where(n1qlizer.Eq{"id": userID})
   
   // Bad - vulnerable to injection
   query := n1qlizer.Select("*").From("users").Where("id = '" + userID + "'")
   ```

2. **Error Handling**: Always check for errors when calling `ToN1ql()` or `Execute()`:

   ```go
   sql, args, err := query.ToN1ql()
   if err != nil {
       // Handle error
   }
   ```

3. **Document Operations**: Use `Upsert` instead of `Insert` for document operations to prevent key conflicts.

4. **Index Usage**: Explicitly specify indexes using `UseIndex` when you know which index will perform best.

5. **Query Reuse**: Build base queries once and then derive specific queries from them:

   ```go
   baseUserQuery := n1qlizer.Select("*").From("users")
   
   activeUsers := baseUserQuery.Where(n1qlizer.Eq{"status": "active"})
   adminUsers := baseUserQuery.Where(n1qlizer.Eq{"role": "admin"})
   ```

### Limitations

1. **Query Complexity**: Very complex queries might be clearer when written directly in N1QL string form.

2. **N1QL Version Compatibility**: Some features may depend on specific Couchbase Server versions.

3. **Performance Overhead**: There's a small overhead compared to raw N1QL strings, although it's negligible in most cases.

4. **Advanced N1QL Features**: Some very advanced N1QL features might not have specific builder methods and may require using `Expr()`.

## Contributing

Contributions to n1qlizer are welcome! Here's how you can contribute:

1. **Fork the Repository**: Create your own fork of the project.

2. **Create a Feature Branch**: 
   ```bash
   git checkout -b feature/my-new-feature
   ```

3. **Make Your Changes**: Implement your feature or bug fix.

4. **Write Tests**: Add tests for your changes to ensure they work as expected.

5. **Run Tests**:
   ```bash
   go test ./...
   ```

6. **Commit Your Changes**:
   ```bash
   git commit -am 'Add new feature: brief description'
   ```

7. **Push to Your Branch**:
   ```bash
   git push origin feature/my-new-feature
   ```

8. **Create a Pull Request**: Submit a PR from your fork to the main repository.

### Coding Guidelines

- Follow Go coding conventions and idiomatic Go
- Add comments for public functions and types
- Update documentation for new features
- Make sure all tests pass before submitting a PR

## License

n1qlizer is released under the MIT License. 