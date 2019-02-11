// Reference: https://en.wikipedia.org/wiki/Bidirectional_map
package treebidimap

import (
	"encoding/json"
	"fmt"
	"github.com/covrom/highloadcup2018/avltree"
	"strconv"
	"strings"
)

// Container is base interface that all data structures implement.
type Container interface {
	Empty() bool
	Size() int
	Clear()
	Values() []avltree.TreeValue
}

// Map interface that all maps implement
type IMap interface {
	Put(key uint32, value avltree.TreeValue)
	Get(key uint32) (value avltree.TreeValue, found bool)
	Remove(key uint32)
	Keys() []uint32

	Container
	// Empty() bool
	// Size() int
	// Clear()
	// Values() []interface{}
}

// BidiMap interface that all bidirectional maps implement (extends the Map interface)
type BidiMap interface {
	GetKey(value avltree.TreeValue) (key uint32, found bool)

	IMap
}

func assertMapImplementation() {
	var _ BidiMap = (*Map)(nil)
}

// Map holds the elements in two red-black trees.
type Map struct {
	forwardMap      avltree.Tree
	inverseMap      avltree.Tree
	keyComparator   avltree.Comparator
	valueComparator avltree.Comparator
}

type data struct {
	key   uint32
	value avltree.TreeValue
}

// NewWith instantiates a bidirectional map.
func NewWith(keyComparator avltree.Comparator, valueComparator avltree.Comparator) *Map {
	return &Map{
		forwardMap:      *avltree.NewWithComparator(keyComparator),
		inverseMap:      *avltree.NewWithComparator(valueComparator),
		keyComparator:   keyComparator,
		valueComparator: valueComparator,
	}
}

// Put inserts element into the map.
func (m *Map) Put(key uint32, value avltree.TreeValue) {
	if d, ok := m.forwardMap.Get(key); ok {
		m.inverseMap.Remove(d.(*data).value)
	}
	if d, ok := m.inverseMap.Get(value); ok {
		m.forwardMap.Remove(d.(*data).key)
	}
	d := &data{key: key, value: value}
	m.forwardMap.Put(key, d)
	m.inverseMap.Put(value, d)
}

// Get searches the element in the map by key and returns its value or nil if key is not found in map.
// Second return parameter is true if key was found, otherwise false.
func (m *Map) Get(key uint32) (value avltree.TreeValue, found bool) {
	if d, ok := m.forwardMap.Get(key); ok {
		return d.(*data).value, true
	}
	return nil, false
}

// GetKey searches the element in the map by value and returns its key or nil if value is not found in map.
// Second return parameter is true if value was found, otherwise false.
func (m *Map) GetKey(value avltree.TreeValue) (key uint32, found bool) {
	if d, ok := m.inverseMap.Get(value); ok {
		return d.(*data).key, true
	}
	return nil, false
}

// Remove removes the element from the map by key.
func (m *Map) Remove(key uint32) {
	if d, found := m.forwardMap.Get(key); found {
		m.forwardMap.Remove(key)
		m.inverseMap.Remove(d.(*data).value)
	}
}

// Empty returns true if map does not contain any elements
func (m *Map) Empty() bool {
	return m.Size() == 0
}

// Size returns number of elements in the map.
func (m *Map) Size() int {
	return m.forwardMap.Size()
}

// Keys returns all keys (ordered).
func (m *Map) Keys() []uint32 {
	return m.forwardMap.Keys()
}

// Values returns all values (ordered).
func (m *Map) Values() []avltree.TreeValue {
	return m.inverseMap.Keys()
}

// Clear removes all elements from the map.
func (m *Map) Clear() {
	m.forwardMap.Clear()
	m.inverseMap.Clear()
}

// String returns a string representation of container
func (m *Map) String() string {
	str := "TreeBidiMap\nmap["
	it := m.Iterator()
	for it.Next() {
		str += fmt.Sprintf("%v:%v ", it.Key(), it.Value())
	}
	return strings.TrimRight(str, " ") + "]"
}

// IteratorWithKey is a stateful iterator for ordered containers whose elements are key value pairs.
type IteratorWithKey interface {
	// Next moves the iterator to the next element and returns true if there was a next element in the container.
	// If Next() returns true, then next element's key and value can be retrieved by Key() and Value().
	// If Next() was called for the first time, then it will point the iterator to the first element if it exists.
	// Modifies the state of the iterator.
	Next() bool

	// Value returns the current element's value.
	// Does not modify the state of the iterator.
	Value() interface{}

	// Key returns the current element's key.
	// Does not modify the state of the iterator.
	Key() interface{}

	// Begin resets the iterator to its initial state (one-before-first)
	// Call Next() to fetch the first element if any.
	Begin()

	// First moves the iterator to the first element and returns true if there was a first element in the container.
	// If First() returns true, then first element's key and value can be retrieved by Key() and Value().
	// Modifies the state of the iterator.
	First() bool
}

// Copyright (c) 2015, Emir Pasic. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// ReverseIteratorWithKey is a stateful iterator for ordered containers whose elements are key value pairs.
//
// Essentially it is the same as IteratorWithKey, but provides additional:
//
// Prev() function to enable traversal in reverse
//
// Last() function to move the iterator to the last element.
type ReverseIteratorWithKey interface {
	// Prev moves the iterator to the previous element and returns true if there was a previous element in the container.
	// If Prev() returns true, then previous element's key and value can be retrieved by Key() and Value().
	// Modifies the state of the iterator.
	Prev() bool

	// End moves the iterator past the last element (one-past-the-end).
	// Call Prev() to fetch the last element if any.
	End()

	// Last moves the iterator to the last element and returns true if there was a last element in the container.
	// If Last() returns true, then last element's key and value can be retrieved by Key() and Value().
	// Modifies the state of the iterator.
	Last() bool

	IteratorWithKey
}

func assertIteratorImplementation() {
	var _ ReverseIteratorWithKey = (*Iterator)(nil)
}

// Iterator holding the iterator's state
type Iterator struct {
	iterator avltree.Iterator
}

// Iterator returns a stateful iterator whose elements are key/value pairs.
func (m *Map) Iterator() Iterator {
	return Iterator{iterator: m.forwardMap.Iterator()}
}

// Next moves the iterator to the next element and returns true if there was a next element in the container.
// If Next() returns true, then next element's key and value can be retrieved by Key() and Value().
// If Next() was called for the first time, then it will point the iterator to the first element if it exists.
// Modifies the state of the iterator.
func (iterator *Iterator) Next() bool {
	return iterator.iterator.Next()
}

// Prev moves the iterator to the previous element and returns true if there was a previous element in the container.
// If Prev() returns true, then previous element's key and value can be retrieved by Key() and Value().
// Modifies the state of the iterator.
func (iterator *Iterator) Prev() bool {
	return iterator.iterator.Prev()
}

// Value returns the current element's value.
// Does not modify the state of the iterator.
func (iterator *Iterator) Value() interface{} {
	return iterator.iterator.Value().(*data).value
}

// Key returns the current element's key.
// Does not modify the state of the iterator.
func (iterator *Iterator) Key() interface{} {
	return iterator.iterator.Key()
}

// Begin resets the iterator to its initial state (one-before-first)
// Call Next() to fetch the first element if any.
func (iterator *Iterator) Begin() {
	iterator.iterator.Begin()
}

// End moves the iterator past the last element (one-past-the-end).
// Call Prev() to fetch the last element if any.
func (iterator *Iterator) End() {
	iterator.iterator.End()
}

// First moves the iterator to the first element and returns true if there was a first element in the container.
// If First() returns true, then first element's key and value can be retrieved by Key() and Value().
// Modifies the state of the iterator
func (iterator *Iterator) First() bool {
	return iterator.iterator.First()
}

// Last moves the iterator to the last element and returns true if there was a last element in the container.
// If Last() returns true, then last element's key and value can be retrieved by Key() and Value().
// Modifies the state of the iterator.
func (iterator *Iterator) Last() bool {
	return iterator.iterator.Last()
}

// Copyright (c) 2015, Emir Pasic. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// EnumerableWithKey provides functions for ordered containers whose values whose elements are key/value pairs.
type EnumerableWithKey interface {
	// Each calls the given function once for each element, passing that element's key and value.
	Each(func(key interface{}, value interface{}))

	// Map invokes the given function once for each element and returns a container
	// containing the values returned by the given function as key/value pairs.
	// TODO need help on how to enforce this in containers (don't want to type assert when chaining)
	// Map(func(key interface{}, value interface{}) (interface{}, interface{})) Container

	// Select returns a new container containing all elements for which the given function returns a true value.
	// TODO need help on how to enforce this in containers (don't want to type assert when chaining)
	// Select(func(key interface{}, value interface{}) bool) Container

	// Any passes each element of the container to the given function and
	// returns true if the function ever returns true for any element.
	Any(func(key interface{}, value interface{}) bool) bool

	// All passes each element of the container to the given function and
	// returns true if the function returns true for all elements.
	All(func(key interface{}, value interface{}) bool) bool

	// Find passes each element of the container to the given function and returns
	// the first (key,value) for which the function is true or nil,nil otherwise if no element
	// matches the criteria.
	Find(func(key interface{}, value interface{}) bool) (interface{}, interface{})
}

func assertEnumerableImplementation() {
	var _ EnumerableWithKey = (*Map)(nil)
}

// Each calls the given function once for each element, passing that element's key and value.
func (m *Map) Each(f func(key interface{}, value interface{})) {
	iterator := m.Iterator()
	for iterator.Next() {
		f(iterator.Key(), iterator.Value())
	}
}

// Map invokes the given function once for each element and returns a container
// containing the values returned by the given function as key/value pairs.
func (m *Map) Map(f func(key1 interface{}, value1 interface{}) (interface{}, interface{})) *Map {
	newMap := NewWith(m.keyComparator, m.valueComparator)
	iterator := m.Iterator()
	for iterator.Next() {
		key2, value2 := f(iterator.Key(), iterator.Value())
		newMap.Put(key2, value2)
	}
	return newMap
}

// Select returns a new container containing all elements for which the given function returns a true value.
func (m *Map) Select(f func(key interface{}, value interface{}) bool) *Map {
	newMap := NewWith(m.keyComparator, m.valueComparator)
	iterator := m.Iterator()
	for iterator.Next() {
		if f(iterator.Key(), iterator.Value()) {
			newMap.Put(iterator.Key(), iterator.Value())
		}
	}
	return newMap
}

// Any passes each element of the container to the given function and
// returns true if the function ever returns true for any element.
func (m *Map) Any(f func(key interface{}, value interface{}) bool) bool {
	iterator := m.Iterator()
	for iterator.Next() {
		if f(iterator.Key(), iterator.Value()) {
			return true
		}
	}
	return false
}

// All passes each element of the container to the given function and
// returns true if the function returns true for all elements.
func (m *Map) All(f func(key interface{}, value interface{}) bool) bool {
	iterator := m.Iterator()
	for iterator.Next() {
		if !f(iterator.Key(), iterator.Value()) {
			return false
		}
	}
	return true
}

// Find passes each element of the container to the given function and returns
// the first (key,value) for which the function is true or nil,nil otherwise if no element
// matches the criteria.
func (m *Map) Find(f func(key interface{}, value interface{}) bool) (interface{}, interface{}) {
	iterator := m.Iterator()
	for iterator.Next() {
		if f(iterator.Key(), iterator.Value()) {
			return iterator.Key(), iterator.Value()
		}
	}
	return nil, nil
}

// Copyright (c) 2015, Emir Pasic. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// JSONSerializer provides JSON serialization
type JSONSerializer interface {
	// ToJSON outputs the JSON representation of containers's elements.
	ToJSON() ([]byte, error)
}

// JSONDeserializer provides JSON deserialization
type JSONDeserializer interface {
	// FromJSON populates containers's elements from the input JSON representation.
	FromJSON([]byte) error
}

func assertSerializationImplementation() {
	var _ JSONSerializer = (*Map)(nil)
	var _ JSONDeserializer = (*Map)(nil)
}

// ToJSON outputs the JSON representation of the map.
func (m *Map) ToJSON() ([]byte, error) {
	elements := make(map[string]interface{})
	it := m.Iterator()
	for it.Next() {
		elements[ToString(it.Key())] = it.Value()
	}
	return json.Marshal(&elements)
}

// FromJSON populates the map from the input JSON representation.
func (m *Map) FromJSON(data []byte) error {
	elements := make(map[string]interface{})
	err := json.Unmarshal(data, &elements)
	if err == nil {
		m.Clear()
		for key, value := range elements {
			m.Put(key, value)
		}
	}
	return err
}

// ToString converts a value to string.
func ToString(value interface{}) string {
	switch value.(type) {
	case string:
		return value.(string)
	case int8:
		return strconv.FormatInt(int64(value.(int8)), 10)
	case int16:
		return strconv.FormatInt(int64(value.(int16)), 10)
	case int32:
		return strconv.FormatInt(int64(value.(int32)), 10)
	case int64:
		return strconv.FormatInt(int64(value.(int64)), 10)
	case uint8:
		return strconv.FormatUint(uint64(value.(uint8)), 10)
	case uint16:
		return strconv.FormatUint(uint64(value.(uint16)), 10)
	case uint32:
		return strconv.FormatUint(uint64(value.(uint32)), 10)
	case uint64:
		return strconv.FormatUint(uint64(value.(uint64)), 10)
	case float32:
		return strconv.FormatFloat(float64(value.(float32)), 'g', -1, 64)
	case float64:
		return strconv.FormatFloat(float64(value.(float64)), 'g', -1, 64)
	case bool:
		return strconv.FormatBool(value.(bool))
	default:
		return fmt.Sprintf("%+v", value)
	}
}
