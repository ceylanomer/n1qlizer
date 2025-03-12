# Upgrade from Go 1.14 to Go 1.18

This document outlines the changes made to upgrade the n1qlizer codebase from Go 1.14 to Go 1.18, embracing the new language features.

## Work in Progress Note

This upgrade is a work in progress. While the core functionality has been refactored to use Go 1.18 features, there are still some files that need to be updated to fully take advantage of generics and replace all instances of `interface{}` with `any`. Refer to the sections below for the current state of the upgrade.

## Known Issues

The following issues need to be addressed as part of the upgrade:

1. **Duplicate Function Declarations**: Functions have been consolidated into registry_impl.go, with the original functions in registry.go marked as deprecated. Callers should be updated to use the new functions.

2. **Type Conversion Issues**: Several methods in builder types have been updated to use generics. This process is now complete for DeleteBuilder, SelectBuilder, UpdateBuilder, and AnalyticsSelectBuilder.

3. **Incomplete Migration**: Some files still use `interface{}` instead of `any`, including:
   - json.go
   - expr.go
   - builder_test.go and other test files

4. **Builder Method Updates**: The following builder types have been updated to use the new generic pattern:
   ```go
   // Old pattern (using type assertions)
   return Set(b, "Field", value).(BuilderType)
   
   // New pattern (using generics)
   return Set[BuilderType, ValueType](b, "Field", value)
   ```

### Files Updated

Below is a summary of the files that have been updated:

| File | Changes Made |
| ---- | ------------ |
| delete.go | Fully migrated to use generics with Set[T, V] pattern |
| delete_ctx.go | Updated to use generics consistently |
| select.go | Updated builder methods to use generic functions and replaced `interface{}` with `any` |
| update.go | Updated builder methods to use generic functions and replaced `interface{}` with `any` |
| analytics.go | Updated builder methods to use generic functions and replaced `interface{}` with `any` |
| registry_impl.go | Added as a central implementation for registry functions |
| n1qlizer_exec.go | Added to implement the ExecuteWith function |
| builder.go | Updated with DEPRECATED comments for alias functions |
| registry.go | Updated with DEPRECATED comments and redirects to registry_impl.go |

### Files Needing Updates

Below is a summary of the files that still need to be updated:

| File | Changes Needed |
| ---- | -------------- |
| expr.go | Replace `interface{}` with `any` in: <br>- struct fields <br>- function parameters <br>- return types |
| json.go | Replace `interface{}` with `any` in function signatures and parameters |
| upsert.go | Update builder methods to use generic functions and replace `interface{}` with `any` |
| insert.go | Update builder methods to use generic functions and replace `interface{}` with `any` |
| list.go | Complete update to use generics for collections |
| map.go | Update to use generics consistently |
| All test files | Update test code to work with the new generics-based API |

## Key Changes

### 1. Registry Function Standardization

To resolve function conflicts, we've:
- Created a `registry_impl.go` file as the single source of truth for registry functions
- Added DEPRECATED comments to the original functions in `registry.go`
- Added DEPRECATED comments to alias functions in `builder.go`
- Documented the need to update callers to use the new standardized functions

### 2. Generic Builder Pattern

We've established a consistent pattern for builder methods using generics:

```go
// Before: Using type assertions
func (b DeleteBuilder) From(from string) DeleteBuilder {
    return Set(b, "From", from).(DeleteBuilder)
}

// After: Using generics
func (b DeleteBuilder) From(from string) DeleteBuilder {
    return Set[DeleteBuilder, string](b, "From", from)
}
```

This pattern has been applied to:
- DeleteBuilder
- SelectBuilder
- UpdateBuilder
- AnalyticsSelectBuilder

### 3. interface{} to any Migration

We've replaced `interface{}` with `any` throughout:
- Function parameters
- Return types
- Struct fields

This work is complete in the updated files but still needs to be done in some remaining files.

### 4. ExecuteWith Function

Added the `ExecuteWith` function to simplify query execution:

```go
// ExecuteWith executes the given N1QLizer using the provided QueryExecutor.
func ExecuteWith(db QueryExecutor, n N1qlizer) (res QueryResult, err error) {
    query, args, err := n.ToN1ql()
    if err != nil {
        return nil, err
    }
    return db.Execute(query, args...)
}
```

This standardized function replaces the duplicate execution logic that was present in each builder type.

## Next Steps

To complete the migration:

1. Update the remaining builder types (UpsertBuilder, InsertBuilder) to use the generic pattern
2. Replace all remaining instances of `interface{}` with `any`
3. Update test files to work with the generics-based API
4. Consider future enhancements like stronger typing for query results

## Backward Compatibility

While the library has been upgraded to use modern Go features, we've maintained backward compatibility where possible. Most existing code should continue to work without changes, but taking advantage of the new type-safe generic interfaces is recommended for new code.