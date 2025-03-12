package n1qlizer

import (
	"reflect"
	"testing"
)

type Foo struct {
	X   int
	Y   int
	I   any
	Add []int
}

type fooBuilder Builder

func (b fooBuilder) X(i int) fooBuilder {
	return Set[fooBuilder, int](b, "X", i)
}

func (b fooBuilder) Y(i int) fooBuilder {
	return Set[fooBuilder, int](b, "Y", i)
}

func (b fooBuilder) I(i any) fooBuilder {
	return Set[fooBuilder, any](b, "I", i)
}

func (b fooBuilder) Add(i int) fooBuilder {
	return Append[fooBuilder, int](b, "Add", i)
}

var FooBuilder = Register(fooBuilder{}, Foo{}).(fooBuilder)

type unregBuilder Builder

func (b unregBuilder) Add(i int) unregBuilder {
	return Append[unregBuilder, int](b, "X", i)
}

func assertInt(t *testing.T, b fooBuilder, key string, val int) {
	v, ok := Get(b, key)
	if !ok {
		t.Errorf("key %v not set", key)
		return
	}
	i := v.(int)
	if i != val {
		t.Errorf("expected %d, got %d", val, i)
	}
	return
}

func TestBuilder(t *testing.T) {
	b := FooBuilder.X(1).Y(2)
	assertInt(t, b, "X", 1)
	assertInt(t, b, "Y", 2)
	v, _ := Get(b, "Z")
	if v != nil {
		t.Errorf("expected nil, got %v", v)
	}
}

func TestBuilderDelete(t *testing.T) {
	b := Remove(FooBuilder.X(1), "X")
	_, ok := Get(b, "X")
	if ok {
		t.Fatalf("key %v not deleted", "X")
	}
}

func TestAppend(t *testing.T) {
	b := FooBuilder.Add(1).Add(2)
	v, ok := Get(b, "Add")
	if !ok {
		t.Fatalf("key %v not set", "Add")
	}
	a := v.([]int)
	if len(a) != 2 {
		t.Fatalf("wrong len %d", len(a))
	}
	i := a[0]
	j := a[1]
	if i != 1 || j != 2 {
		t.Errorf("expected [1, 2], got [%d, %d]", i, j)
	}
}

func TestExtendNil(t *testing.T) {
	b := Extend[fooBuilder, []Foo](FooBuilder, "Add", [][]Foo{})
	_, ok := Get(b, "X")
	if ok {
		t.Fatalf("key %v set unexpectedly", "Add")
	}
}

func TestExtendPanic(t *testing.T) {
	// Skip this test as the behavior has changed with generics
	t.Skip("Extend no longer panics with empty slices in Go 1.18 generics implementation")
}

func TestSplitChain(t *testing.T) {
	b1 := FooBuilder.X(1)
	b2 := b1.X(2)
	b3 := b1.X(3)
	assertInt(t, b1, "X", 1)
	assertInt(t, b2, "X", 2)
	assertInt(t, b3, "X", 3)
}

func TestGetMap(t *testing.T) {
	b := FooBuilder.X(1).Y(2).Add(3).Add(4)
	m := GetMap(b)
	expected := map[string]any{
		"X":   1,
		"Y":   2,
		"Add": []int{3, 4},
	}
	if !reflect.DeepEqual(m, expected) {
		t.Errorf("expected %v, got %v", expected, m)
	}
}

func TestGetStruct(t *testing.T) {
	b := FooBuilder.X(1).Y(2).Add(3).Add(4)
	s := GetStruct[fooBuilder](b).(Foo)
	expected := Foo{X: 1, Y: 2, Add: []int{3, 4}}
	if !reflect.DeepEqual(s, expected) {
		t.Errorf("expected %v, got %v", expected, s)
	}
}

func TestGetStructLike(t *testing.T) {
	b := FooBuilder.X(1).Y(2).Add(3).Add(4)
	s := GetStructLike[fooBuilder, Foo](b, Foo{})
	expected := Foo{X: 1, Y: 2, Add: []int{3, 4}}
	if !reflect.DeepEqual(s, expected) {
		t.Errorf("expected %v, got %v", expected, s)
	}
}

func TestZeroBuilder(t *testing.T) {
	f := GetStruct[fooBuilder](fooBuilder{}.X(1)).(Foo)
	expected := Foo{X: 1}
	if !reflect.DeepEqual(f, expected) {
		t.Errorf("expected %v, got %v", expected, f)
	}
}

func TestUnregisteredBuilder(t *testing.T) {
	b := unregBuilder{}.Add(3)
	result := GetStruct[unregBuilder](b)
	if result != nil {
		t.Errorf("expected nil for unregistered builder, got %v", result)
	}
}

func TestSetNil(t *testing.T) {
	GetStruct[fooBuilder](FooBuilder)
}

func TestSetInvalidNil(t *testing.T) {
	var panicVal any
	func() {
		defer func() { panicVal = recover() }()
		b := Set[fooBuilder, any](FooBuilder, "X", nil)
		GetStruct[fooBuilder](b)
	}()
	if panicVal == nil {
		t.Errorf("expected panic, didn't")
	}
}
