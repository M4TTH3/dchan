package dchan

import (
	"iter"
	"slices"
)

// OrderedSet is a set of elements that are ordered in the order of insertion.
// It prefers many reads and fewer puts and deletes.
type orderedSet[T comparable] struct {
	items    []T
	indexSet map[T]int
}

func newOrderedSet[T comparable]() *orderedSet[T] {
	return &orderedSet[T]{
		items:    make([]T, 0),
		indexSet: make(map[T]int),
	}
}

func newOrderedSetFromSlice[T comparable](items []T) *orderedSet[T] {
	set := newOrderedSet[T]()
	for _, item := range items {
		set.put(item)
	}
	return set
}

func newOrderedSetFromSeq[T comparable](seq iter.Seq[T]) *orderedSet[T] {
	set := newOrderedSet[T]()
	for item := range seq {
		set.put(item)
	}

	return set
}

// Has returns true if the element exists in the set.
// O(1)
func (s *orderedSet[T]) has(item T) bool {
	_, ok := s.indexSet[item]
	return ok
}

// Get returns the element at the given index and true if present, or the zero value and false if out of range.
func (s *orderedSet[T]) get(index int) (T, bool) {
	if index < 0 || index >= len(s.items) {
		var zero T
		return zero, false
	}

	return s.items[index], true
}


// Put adds an element to the set if it does not already exist and returns true if added, false if present.
// O(1)
func (s *orderedSet[T]) put(item T) bool {
	if s.has(item) {
		return false
	}
	s.items = append(s.items, item)
	s.indexSet[item] = len(s.items) - 1
	return true
}

// Delete removes an element from the set if it exists and returns true if deleted, false if not found.
// O(n)
func (s *orderedSet[T]) delete(item T) bool {
	idx, ok := s.indexSet[item]
	if !ok {
		return false
	}

	// Remove item from slice while preserving order
	s.items = append(s.items[:idx], s.items[idx+1:]...)
	delete(s.indexSet, item)

	// Update indices for items after the removed item
	for i := idx; i < len(s.items); i++ {
		s.indexSet[s.items[i]] = i
	}

	return true
}

// Len returns the number of elements in the set.
// O(1)
func (s *orderedSet[T]) len() int {
	return len(s.items)
}

// ToSlice returns a slice of the elements in the set.
// Note: this is a copy of the elements.
// O(n)
func (s *orderedSet[T]) toSlice() []T {
	return slices.Clone(s.items)
}

// Difference returns a new set with the elements that are in the first set but not in the second.
// O(n)
func (s *orderedSet[T]) difference(other *orderedSet[T]) *orderedSet[T] {
	newSet := newOrderedSet[T]()
	for _, item := range s.items {
		if !other.has(item) {
			newSet.put(item)
		}
	}

	return newSet
}