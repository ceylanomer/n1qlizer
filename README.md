# n1qlizer - fluent Couchbase N1QL query generator for Go

```go
import "github.com/ceylanomer/n1qlizer"
```

**n1qlizer** is a fluent query builder for Couchbase's N1QL language, inspired by [Squirrel](https://github.com/Masterminds/squirrel). n1qlizer helps you build N1QL queries from composable parts.


## Usage

```go
import "github.com/ceylanomer/n1qlizer"

// Build a SELECT query
users := n1qlizer.Select("*").From("users").Where(n1qlizer.Eq{"type": "user"})
sql, args, err := users.ToN1ql()
// sql == "SELECT * FROM users WHERE type = $1"
// args == []interface{}{"user"}

// Build an INSERT query
sql, args, err := n1qlizer.
    Insert("users").
    Columns("id", "name", "age").
    Values("user123", "moe", 13).
    Values("user456", "larry", n1qlizer.Expr("$1 + 5", 12)).
    ToN1ql()
// sql == "INSERT INTO users (id,name,age) VALUES ($1,$2,$3),($4,$5,$6 + 5)"

// Build an UPDATE query
sql, args, err := n1qlizer.
    Update("users").
    UseKeys("'user123'").
    Set("name", "Moe Howard").
    Set("updated_at", n1qlizer.Expr("NOW()")).
    ToN1ql()
// sql == "UPDATE users USE KEYS 'user123' SET name = $1, updated_at = NOW()"

// Build a DELETE query
sql, args, err := n1qlizer.
    Delete("users").
    Where(n1qlizer.Eq{"status": "inactive"}).
    Limit(10).
    ToN1ql()
// sql == "DELETE FROM users WHERE status = $1 LIMIT 10"
```

## Couchbase-Specific Features

n1qlizer adapts Squirrel's interface to work with Couchbase's N1QL language:

### USE KEYS Clause

```go
users := n1qlizer.Select("*").From("users").UseKeys("'user123', 'user456'")
sql, args, err := users.ToN1ql()
// sql == "SELECT * FROM users USE KEYS 'user123', 'user456'"
```

### USE INDEX Clause

```go
users := n1qlizer.Select("*").
    From("users").
    Prefix(n1qlizer.UseIndexGSI("users_by_email")).
    Where(n1qlizer.Eq{"email": "user@example.com"})
sql, args, err := users.ToN1ql()
// sql == "USE INDEX (`users_by_email` USING GSI) SELECT * FROM users WHERE email = $1"
```

### UPSERT Support

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

### NEST and UNNEST Operations

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

### Full-Text Search Integration

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
// sql == "SELECT * FROM users WHERE SEARCH(users_fts, \"name:smith OR email:smith\", {\"fuzziness\": 1, \"boost\": 2.000000}) AS score ORDER BY score DESC"
```

### Analytics Service Support

```go
// Using the Couchbase Analytics Service
sql, args, err := n1qlizer.
    AnalyticsSelect("u.name", n1qlizer.ArrayAvg("u.ratings") + " AS avgRating").
    From("users AS u").
    Let("maxRating", 5).
    Where(n1qlizer.Gt{n1qlizer.ArrayAvg("u.ratings"): 4.5}).
    GroupBy("u.country").
    Window("w AS (PARTITION BY u.country ORDER BY avgRating DESC)").
    ToN1ql()
```

### JSON Document Support

n1qlizer provides helpers for working with JSON documents:

```go
// Insert a JSON document
type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
    Age  int    `json:"age"`
}

user := User{ID: "user123", Name: "Joe", Age: 30}
sql, args, err := n1qlizer.
    Upsert("users").
    Document(user.ID, n1qlizer.AsDocument(user)).
    ToN1ql()
// sql == "UPSERT INTO users (KEY, VALUE) VALUES ($1, {"id":"user123","name":"Joe","age":30})"

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
// sql == "SELECT * FROM users WHERE user.tags ARRAY_CONTAINS $1"

// Array and Object functions
sql, args, err := n1qlizer.
    Select("*", n1qlizer.ArraySum("prices") + " AS total", n1qlizer.ObjectNames("metadata") + " AS keys").
    From("orders").
    Where(n1qlizer.Gt{n1qlizer.ArrayMax("ratings"): 4}).
    ToN1ql()
// sql == "SELECT *, ARRAY_SUM(prices) AS total, OBJECT_NAMES(metadata) AS keys FROM orders WHERE ARRAY_MAX(ratings) > $1"
```

### Case Expressions

```go
caseExpr := n1qlizer.NewCaseBuilder().
    When(n1qlizer.Eq{"status": "active"}, "Active User").
    When(n1qlizer.Eq{"status": "pending"}, "Pending Activation").
    Else("Inactive")

sql, args, err := n1qlizer.
    Select("name", n1qlizer.Expr("? AS status_text", caseExpr)).
    From("users").
    ToN1ql()
// sql == "SELECT name, (CASE WHEN status = $1 THEN $2 WHEN status = $3 THEN $4 ELSE $5 END) AS status_text FROM users"
```

### Context Support

All query builders support context-based operations for cancellation and timeouts:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Execute with context
n1qlizer.Select("*").
    From("users").
    Where(n1qlizer.Eq{"status": "active"}).
    RunWithContext(db).
    ExecuteContext(ctx)
```

## Couchbase Runner Implementation

To execute queries directly with the Couchbase SDK:

```go
import (
    "github.com/couchbase/gocb/v2"
    "github.com/ceylanomer/n1qlizer"
)

// Implement a Couchbase Runner
type CouchbaseRunner struct {
    cluster *gocb.Cluster
    bucket  string
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

func (r *CouchbaseRunner) ExecuteContext(ctx context.Context, query string, args ...interface{}) (n1qlizer.QueryResult, error) {
    // Execute the query with context using the Couchbase SDK
    result, err := r.cluster.Query(query, &gocb.QueryOptions{
        PositionalParameters: args,
        Context:              ctx,
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
runner := &CouchbaseRunner{cluster: cluster, bucket: "default"}
users, err := n1qlizer.
    Select("*").
    From("users").
    Where(n1qlizer.Eq{"type": "admin"}).
    RunWith(runner).
    Execute()

var adminUsers []User
err = users.All(&adminUsers)
```

## License

n1qlizer is released under the MIT License. 