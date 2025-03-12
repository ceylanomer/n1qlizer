package n1qlizer

import (
	"go/ast"
	"reflect"
)

// Builder stores a set of named values.
//
// New types can be declared with underlying type Builder and used with the
// functions in this package. See example.
//
// Instances of Builder should be treated as immutable. It is up to the
// implementor to ensure mutable values set on a Builder are not mutated while
// the Builder is in use.
type Builder struct {
	builderMap Map
}

var (
	EmptyBuilder      = Builder{NewMap()}
	emptyBuilderValue = reflect.ValueOf(EmptyBuilder)
)

// getBuilderMap retrieves the Map from a builder interface.
func getBuilderMap[T any](builder T) Map {
	// Use reflection to convert the builder to Builder type
	builderValue := reflect.ValueOf(builder)
	builderType := builderValue.Type()

	// Get the underlying Builder structure
	var b Builder
	if builderType.Kind() == reflect.Struct {
		b = reflect.ValueOf(builder).Convert(reflect.TypeOf(Builder{})).Interface().(Builder)
	} else {
		// Handle pointer or other types if needed
		panic("builder must be a struct type based on Builder")
	}

	if b.builderMap == nil {
		return NewMap()
	}

	return b.builderMap
}

// Set returns a copy of the given builder with a new value set for the given
// name.
//
// Set (and all other functions taking a builder in this package) will panic if
// the given builder's underlying type is not Builder.
func Set[T any, V any](builder T, name string, v V) T {
	b := Builder{getBuilderMap(builder).Set(name, v)}

	// Convert the Builder back to the original type T
	result := reflect.ValueOf(b).Convert(reflect.TypeOf(builder)).Interface().(T)
	return result
}

// Remove returns a copy of the given builder with the given named value unset.
func Remove[T any](builder T, name string) T {
	b := Builder{getBuilderMap(builder).Delete(name)}
	result := reflect.ValueOf(b).Convert(reflect.TypeOf(builder)).Interface().(T)
	return result
}

// Append returns a copy of the given builder with new value(s) appended to the
// named list. If the value was previously unset or set with Set (even to a e.g.
// slice values), the new value(s) will be appended to an empty list.
func Append[T any, V any](builder T, name string, vs ...V) T {
	return ExtendValues(builder, name, vs)
}

// Extend behaves like Append, except it takes a single slice or array value
// which will be concatenated to the named list.
//
// Unlike a variadic call to Append - which requires a []interface{} value -
// Extend accepts slices or arrays of any type.
//
// Extend will panic if the given value is not a slice, array, or nil.
func Extend[T any, V any](builder T, name string, vs []V) T {
	if vs == nil {
		return builder
	}

	maybeList, ok := getBuilderMap(builder).Lookup(name)

	var list List
	if ok {
		list, ok = maybeList.(List)
	}
	if !ok {
		list = NewList()
	}

	for _, v := range vs {
		list = list.Cons(v)
	}

	return Set(builder, name, list)
}

// ExtendValues is like Extend but allows passing any value that can be iterated using reflection.
// Used for backward compatibility.
func ExtendValues[T any](builder T, name string, vs any) T {
	if vs == nil {
		return builder
	}

	maybeList, ok := getBuilderMap(builder).Lookup(name)

	var list List
	if ok {
		list, ok = maybeList.(List)
	}
	if !ok {
		list = NewList()
	}

	forEachReflect(vs, func(v any) {
		list = list.Cons(v)
	})

	return Set(builder, name, list)
}

// listToSlice converts a List to a slice of the specified array type.
func listToSlice(list List, arrayType reflect.Type) reflect.Value {
	size := list.Size()
	slice := reflect.MakeSlice(arrayType, size, size)
	for i := size - 1; i >= 0; i-- {
		val := reflect.ValueOf(list.Head())
		slice.Index(i).Set(val)
		list = list.Tail()
	}
	return slice
}

var anyArrayType = reflect.TypeOf([]any{})

// getComponentType gets the type of a registered struct field for this builder type
func getComponentType(builderType reflect.Type, name string) reflect.Type {
	structType := GetBuilderStructType(builderType)
	if structType == nil {
		return nil
	}

	field, ok := structType.FieldByName(name)
	if !ok {
		return nil
	}

	if field.Type.Kind() == reflect.Slice {
		return field.Type.Elem()
	}

	return nil
}

// scanStruct populates a struct value with data from a builder
func scanStruct[T any](builder T, structVal *reflect.Value) any {
	getBuilderMap(builder).ForEach(func(name string, val any) {
		if ast.IsExported(name) {
			field := structVal.FieldByName(name)
			if field.IsValid() && field.CanSet() {
				// handle lists -> slices
				list, ok := val.(List)
				if ok {
					val = listToSlice(list, field.Type()).Interface()
				}

				v := reflect.ValueOf(val)
				field.Set(v)
			}
		}
	})
	return structVal.Interface()
}

// Get retrieves a single named value from the given builder.
//
// If the value was set with Append or Extend, the result will be a slice of the
// same concrete type as was appended to the list.
//
// Get will panic when getting a key that was set with Append or Extend and the
// types of values in the list don't match. Get will also panic if a value was
// set with Set using a type derived from a registered struct's exported field
// and the value set on the Builder is not assignable to the field.
func Get[T any](builder T, name string) (any, bool) {
	val, ok := getBuilderMap(builder).Lookup(name)
	if !ok {
		return nil, false
	}

	// dereference list values to slices
	list, ok := val.(List)
	if ok {
		// Check the concrete type of the list
		// TODO: Implement proper generic handling for lists
		ct := getComponentType(reflect.TypeOf(builder), name)
		if ct != nil {
			arrayType := reflect.SliceOf(ct)
			val = listToSlice(list, arrayType).Interface()
		} else {
			val = listToSlice(list, anyArrayType).Interface()
		}
	}

	return val, true
}

// GetMap returns a copy of the builder's underlying map of values.
//
// If any values were appended with Append or Extend, the value in the map will
// be the equivalent slice or array.
//
// See notes on Get regarding returned slices.
func GetMap[T any](builder T) map[string]any {
	m := getBuilderMap(builder)
	structType := GetBuilderStructType(reflect.TypeOf(builder))
	result := map[string]any{}

	m.ForEach(func(name string, val any) {
		// dereference list values to slices
		list, ok := val.(List)
		if ok {
			// use the struct field type if we have it
			var arrayType reflect.Type = anyArrayType
			if structType != nil {
				if field, ok := structType.FieldByName(name); ok {
					if field.Type.Kind() == reflect.Slice {
						arrayType = field.Type
					}
				}
			}
			val = listToSlice(list, arrayType).Interface()
		}
		result[name] = val
	})

	return result
}

// GetStruct returns a new value with the same type as a struct registered for
// the given Builder type with field values taken from the builder.
//
// GetStruct will panic if any of these "exported" values are not assignable to
// their corresponding struct fields.
func GetStruct[T any](builder T) any {
	structVal := NewBuilderStruct(reflect.TypeOf(builder))
	if structVal == nil {
		return nil
	}
	return scanStruct(builder, structVal)
}

// GetStructLike will panic if any of these "exported" values are not assignable to
// their corresponding struct fields.
func GetStructLike[T any, S any](builder T, strct S) S {
	structVal := reflect.New(reflect.TypeOf(strct)).Elem()
	result := scanStruct(builder, &structVal)
	return result.(S)
}
