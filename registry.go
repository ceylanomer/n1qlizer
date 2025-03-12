package n1qlizer

import (
	"reflect"
)

// DEPRECATED: Bu dosya eski kod ile uyumluluk için korunmuştur.
// Yeni kodlar registry_impl.go dosyasındaki fonksiyonları kullanmalıdır.

// RegisterType maps the given builderType to a structType.
// This mapping affects the type of slices returned by Get and is required for
// GetStruct to work.
//
// Returns a Value containing an empty instance of the registered builderType.
//
// RegisterType will panic if builderType's underlying type is not Builder or
// if structType's Kind is not Struct.
func RegisterType(builderType reflect.Type, structType reflect.Type) *reflect.Value {
	return RegisterBuilderType(builderType, structType)
}

// Register wraps RegisterType, taking instances instead of Types.
//
// Returns an empty instance of the registered builder type which can be used
// as the initial value for builder expressions. See example.
func Register(builderProto any, structProto any) any {
	return RegisterBuilder(builderProto, structProto)
}

// getBuilderStructType returns the registered struct type for a given builder type
func getBuilderStructType(builderType reflect.Type) reflect.Type {
	return GetBuilderStructType(builderType)
}

// newBuilderStruct returns a new value with the same type as a struct registered for
// the given Builder type.
func newBuilderStruct(builderType reflect.Type) *reflect.Value {
	return NewBuilderStruct(builderType)
}
