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

// RegisterBuilderType, bir Builder tipini ve karşılık gelen struct tipini kaydeder.
// Bu eşleme, Get tarafından döndürülen dilimin tipini etkiler ve
// GetStruct'ın çalışması için gereklidir.
//
// Kayıtlı builderType'ın bir örneğini içeren bir Value döndürür.
//
// RegisterBuilderType, builderType'ın temel tipi Builder değilse veya
// structType'ın Kind'ı Struct değilse panik yapar.
func RegisterBuilderType(builderType reflect.Type, structType reflect.Type) *reflect.Value {
	BuilderMux.Lock()
	defer BuilderMux.Unlock()
	structType.NumField() // Struct değilse panik
	BuilderTypes[builderType] = structType
	emptyValue := reflect.ValueOf(EmptyBuilder).Convert(builderType)
	return &emptyValue
}

// RegisterBuilder, RegisterBuilderType'ı sarar, tip yerine örnekler alır.
//
// Kayıtlı builder tipinin boş bir örneğini döndürür, bu
// builder ifadeleri için başlangıç değeri olarak kullanılabilir. Örneğe bakınız.
func RegisterBuilder(builderProto any, structProto any) any {
	empty := RegisterBuilderType(
		reflect.TypeOf(builderProto),
		reflect.TypeOf(structProto),
	).Interface()
	return empty
}

// GetBuilderStructType, verilen bir builder tipi için kayıtlı struct tipini döndürür
func GetBuilderStructType(builderType reflect.Type) reflect.Type {
	BuilderMux.RLock()
	defer BuilderMux.RUnlock()
	structType, ok := BuilderTypes[builderType]
	if !ok {
		return nil
	}
	return structType
}

// NewBuilderStruct, verilen Builder tipi ile ilişkilendirilmiş struct tipinde
// yeni bir değer döndürür.
func NewBuilderStruct(builderType reflect.Type) *reflect.Value {
	structType := GetBuilderStructType(builderType)
	if structType == nil {
		return nil
	}
	newStruct := reflect.New(structType).Elem()
	return &newStruct
}
