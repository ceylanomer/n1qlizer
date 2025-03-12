package n1qlizer

import "reflect"

// convert converts a value from one type to another using reflection.
// This is provided for backward compatibility. New code should use type parameters instead.
func convert[From, To any](from From, to To) To {
	return reflect.
		ValueOf(from).
		Convert(reflect.TypeOf(to)).
		Interface().(To)
}

// forEach iterates over a slice or array and applies the given function to each element.
// This is provided for backward compatibility. New code should use range loops instead.
func forEach[T any](s []T, f func(T)) {
	for _, v := range s {
		f(v)
	}
}

// forEachReflect is the reflection-based version for cases where we need to handle interface{} slices.
// This should only be used as a fallback when the type is not known at compile time.
func forEachReflect(s any, f func(any)) {
	val := reflect.ValueOf(s)

	kind := val.Kind()
	if kind != reflect.Slice && kind != reflect.Array {
		panic(&reflect.ValueError{Method: "forEachReflect", Kind: kind})
	}

	l := val.Len()
	for i := 0; i < l; i++ {
		f(val.Index(i).Interface())
	}
}
