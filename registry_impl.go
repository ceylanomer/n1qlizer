package n1qlizer

import (
	"reflect"
	"sync"
)

// registry_impl.go provides the centralized implementation of builder type registry functionality.
//
// This file was created to resolve function conflicts between builder.go and registry.go,
// by providing a single source of truth for the implementation of registry-related functions.
//
// IMPORTANT: All new code should use these functions directly rather than the aliases in
// builder.go or registry.go. The aliases are maintained only for backward compatibility
// and will eventually be deprecated.

var (
	// BuilderTypes stores the global registry of Builder types mapped to their corresponding struct types
	BuilderTypes = make(map[reflect.Type]reflect.Type)
	// BuilderMux provides thread-safe access to the BuilderTypes map
	BuilderMux sync.RWMutex
)

// RegisterBuilderType registers a Builder type and its corresponding struct type.
// This mapping affects the type of slices returned by Get and is required for
// GetStruct to work.
//
// Returns a Value containing an instance of the registered builderType.
//
// RegisterBuilderType will panic if builderType's underlying type is not Builder or
// if structType's Kind is not Struct.
func RegisterBuilderType(builderType reflect.Type, structType reflect.Type) *reflect.Value {
	BuilderMux.Lock()
	defer BuilderMux.Unlock()
	structType.NumField() // Panics if not a struct
	BuilderTypes[builderType] = structType
	emptyValue := reflect.ValueOf(EmptyBuilder).Convert(builderType)
	return &emptyValue
}

// RegisterBuilder wraps RegisterBuilderType, taking instances instead of types.
//
// Returns an empty instance of the registered builder type, which can be used
// as the initial value for builder expressions. See example.
func RegisterBuilder(builderProto any, structProto any) any {
	empty := RegisterBuilderType(
		reflect.TypeOf(builderProto),
		reflect.TypeOf(structProto),
	).Interface()
	return empty
}

// GetBuilderStructType returns the registered struct type for a given builder type
func GetBuilderStructType(builderType reflect.Type) reflect.Type {
	BuilderMux.RLock()
	defer BuilderMux.RUnlock()
	structType, ok := BuilderTypes[builderType]
	if !ok {
		return nil
	}
	return structType
}

// NewBuilderStruct returns a new value with the same type as a struct associated
// with the given Builder type.
func NewBuilderStruct(builderType reflect.Type) *reflect.Value {
	structType := GetBuilderStructType(builderType)
	if structType == nil {
		return nil
	}
	newStruct := reflect.New(structType).Elem()
	return &newStruct
}
