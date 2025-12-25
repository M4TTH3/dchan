package dchan

import (
	"testing"
)

func TestHas(t *testing.T) {
	s := newOrderedSet[string]()

	if s.has("a") {
		t.Error("empty set should not have 'a'")
	}

	s.put("a")
	if !s.has("a") {
		t.Error("set should have 'a' after put")
	}
}

func TestGet(t *testing.T) {
	s := newOrderedSet[string]()

	_, ok := s.get(0)
	if ok {
		t.Error("get(0) on empty set should return false")
	}

	s.put("a")
	s.put("b")

	val, ok := s.get(0)
	if !ok || val != "a" {
		t.Errorf("get(0) = %v, %v, want 'a', true", val, ok)
	}

	val, ok = s.get(1)
	if !ok || val != "b" {
		t.Errorf("get(1) = %v, %v, want 'b', true", val, ok)
	}
}

func TestPut(t *testing.T) {
	s := newOrderedSet[string]()

	if !s.put("a") {
		t.Error("put('a') should return true")
	}
	if s.put("a") {
		t.Error("put('a') again should return false")
	}
	if s.len() != 1 {
		t.Errorf("expected 1 item, got %d", s.len())
	}
}

func TestDelete(t *testing.T) {
	s := newOrderedSet[string]()

	if s.delete("a") {
		t.Error("delete from empty set should return false")
	}

	s.put("a")
	s.put("b")
	s.put("c")

	if !s.delete("b") {
		t.Error("delete('b') should return true")
	}
	if s.has("b") {
		t.Error("set should not have 'b' after delete")
	}
	if s.len() != 2 {
		t.Errorf("expected 2 items, got %d", s.len())
	}
	// Verify order is preserved
	if val, ok := s.get(0); !ok || val != "a" {
		t.Errorf("expected 'a', got %v", val)
	}
	if val, ok := s.get(1); !ok || val != "c" {
		t.Errorf("expected 'c', got %v", val)
	}
}

func TestOrderPreservation(t *testing.T) {
	s := newOrderedSet[string]()

	s.put("a")
	s.put("b")
	s.put("c")

	// Delete middle and verify order
	s.delete("b")
	if val, ok := s.get(0); !ok || val != "a" {
		t.Errorf("expected 'a', got %v", val)
	}
	if val, ok := s.get(1); !ok || val != "c" {
		t.Errorf("expected 'c', got %v", val)
	}

	// Re-add and verify it goes to end
	s.put("b")
	if val, ok := s.get(2); !ok || val != "b" {
		t.Errorf("expected 'b', got %v", val)
	}
}
