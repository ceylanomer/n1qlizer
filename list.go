package n1qlizer

// List is a persistent list of possibly heterogeneous values.
type List interface {
	// IsNil returns true if the list is empty
	IsNil() bool

	// Cons returns a new list with val as the head
	Cons(val any) List

	// Head returns the first element of the list;
	// panics if the list is empty
	Head() any

	// Tail returns a list with all elements except the head;
	// panics if the list is empty
	Tail() List

	// Size returns the list's length.  This takes O(1) time.
	Size() int

	// ForEach executes a callback for each value in the list.
	ForEach(f func(any))

	// Reverse returns a list whose elements are in the opposite order as
	// the original list.
	Reverse() List
}

// GenericList is a type-safe persistent list using generics.
type GenericList[T any] interface {
	// IsNil returns true if the list is empty
	IsNil() bool

	// Cons returns a new list with val as the head
	Cons(val T) GenericList[T]

	// Head returns the first element of the list;
	// panics if the list is empty
	Head() T

	// Tail returns a list with all elements except the head;
	// panics if the list is empty
	Tail() GenericList[T]

	// Size returns the list's length.  This takes O(1) time.
	Size() int

	// ForEach executes a callback for each value in the list.
	ForEach(f func(T))

	// Reverse returns a list whose elements are in the opposite order as
	// the original list.
	Reverse() GenericList[T]
}

// genericList is the implementation of GenericList
type genericList[T any] struct {
	depth int // the number of nodes after, and including, this one
	value T
	tail  *genericList[T]
}

// Immutable (i.e. persistent) list
type list struct {
	depth int // the number of nodes after, and including, this one
	value any
	tail  *list
}

// An empty list shared by all lists
var nilList = &list{}

// NewList returns a new, empty list.  The result is a singly linked
// list implementation.  All lists share an empty tail, so allocating
// empty lists is efficient in time and memory.
func NewList() List {
	return nilList
}

// NewGenericList returns a new, empty generic list with type parameter T.
func NewGenericList[T any]() GenericList[T] {
	// Create an empty genericList instance for type T
	return &genericList[T]{depth: 0}
}

func (self *list) IsNil() bool {
	return self == nilList
}

func (self *list) Size() int {
	return self.depth
}

func (tail *list) Cons(val any) List {
	var xs list
	xs.depth = tail.depth + 1
	xs.value = val
	xs.tail = tail
	return &xs
}

func (self *list) Head() any {
	if self.IsNil() {
		panic("Called Head() on an empty list")
	}

	return self.value
}

func (self *list) Tail() List {
	if self.IsNil() {
		panic("Called Tail() on an empty list")
	}

	return self.tail
}

// ForEach executes a callback for each value in the list
func (self *list) ForEach(f func(any)) {
	if self.IsNil() {
		return
	}
	f(self.Head())
	self.Tail().ForEach(f)
}

// Reverse returns a list with elements in opposite order as this list
func (self *list) Reverse() List {
	reversed := NewList()
	self.ForEach(func(v any) { reversed = reversed.Cons(v) })
	return reversed
}

// GenericList implementations
func (self *genericList[T]) IsNil() bool {
	return self.depth == 0
}

func (self *genericList[T]) Size() int {
	return self.depth
}

func (tail *genericList[T]) Cons(val T) GenericList[T] {
	return &genericList[T]{
		depth: tail.depth + 1,
		value: val,
		tail:  tail,
	}
}

func (self *genericList[T]) Head() T {
	if self.IsNil() {
		panic("Called Head() on an empty generic list")
	}
	return self.value
}

func (self *genericList[T]) Tail() GenericList[T] {
	if self.IsNil() {
		panic("Called Tail() on an empty generic list")
	}
	return self.tail
}

func (self *genericList[T]) ForEach(f func(T)) {
	if self.IsNil() {
		return
	}
	f(self.Head())
	self.Tail().ForEach(f)
}

func (self *genericList[T]) Reverse() GenericList[T] {
	reversed := NewGenericList[T]()
	self.ForEach(func(v T) {
		reversed = reversed.Cons(v)
	})
	return reversed
}
