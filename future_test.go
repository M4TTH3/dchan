package dchan

import (
	"testing"
	"time"
)

func TestFuture(t *testing.T) {
	future := newFuture()
	future.set(nil)
	if err := future.Wait(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestFuture_Async(t *testing.T) {
	future := newFuture()
	go func() {
		time.Sleep(100 * time.Millisecond)
		future.set(nil)
	}()
	if err := future.Wait(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}