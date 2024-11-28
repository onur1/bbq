package bouncer_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/onur1/bouncer"
)

func TestReadWriteBasic(t *testing.T) {
	q := bouncer.New[int](16)

	for i := 1; i <= 5; i++ {
		q.Write(i)
	}

	b := make([]int, 2)

	results := [][]int{
		{1, 2},
		{3, 4},
		{5},
	}

	for i := 0; i < 3; i++ {
		n, err := q.Read(b)
		if err != nil {
			t.Fatalf("unexpected error %v", bouncer.ErrQueueClosed)
		}
		e := results[0]
		results = results[1:]
		if !reflect.DeepEqual(b[:n], e) {
			t.Fatalf("expected %v, got %v", e, b[:n])
		}
	}

	if len(results) != 0 {
		t.Fatalf("expected results to be fully retrieved, got %v", results)
	}
}

func TestReadWriteSingleItem(t *testing.T) {
	q := bouncer.New[int](1)

	n, err := q.Write(1)
	if err != nil {
		t.Fatalf("unexpected error on Write: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to insert 1 item, got %d", n)
	}

	b := make([]int, 1)
	m, err := q.Read(b)
	if err != nil {
		t.Fatalf("unexpected error on Read: %v", err)
	}
	if m != 1 {
		t.Fatalf("expected to read 1 item, got %d", m)
	}

	if b[0] != 1 {
		t.Fatalf("expected value 1, got %d", b[0])
	}
}

func TestSizePowerOfTwo(t *testing.T) {
	tests := []struct {
		input    int
		expected int
	}{
		{3, 4},  // Input 3 should round up to 4
		{-1, 1}, // Negative input should default to 1
		{0, 1},  // Zero should default to 1
		{2, 2},  // Input 2 should remain 2
	}

	for _, tt := range tests {
		q := bouncer.New[int](tt.input)
		if q.Size() != tt.expected {
			t.Fatalf("For input %d, expected size %d, got %d", tt.input, tt.expected, q.Size())
		}
	}
}

func TestUtility(t *testing.T) {
	q := bouncer.New[int](4)

	if q.Used() != 0 {
		t.Errorf("expected Used() to return 0, got %d", q.Used())
	}
	if q.Available() != 4 {
		t.Errorf("expected Available() to return 4, got %d", q.Available())
	}
	if !q.IsEmpty() {
		t.Errorf("expected IsEmpty() to return true, got false")
	}
	if q.IsFull() {
		t.Errorf("expected IsFull() to return false, got true")
	}

	// Add one item to the buffer
	q.Write(1)
	if q.Used() != 1 {
		t.Errorf("expected Used() to return 1, got %d", q.Used())
	}
	if q.Available() != 3 {
		t.Errorf("expected Available() to return 3, got %d", q.Available())
	}
	if q.IsEmpty() {
		t.Errorf("expected IsEmpty() to return false, got true")
	}
	if q.IsFull() {
		t.Errorf("expected IsFull() to return false, got true")
	}

	// Add three more items to fill the buffer
	q.Write(2, 3, 4)
	if q.Used() != 4 {
		t.Errorf("expected Used() to return 4, got %d", q.Used())
	}
	if q.Available() != 0 {
		t.Errorf("expected Available() to return 0, got %d", q.Available())
	}
	if q.IsEmpty() {
		t.Errorf("expected IsEmpty() to return false, got true")
	}
	if !q.IsFull() {
		t.Errorf("expected IsFull() to return true, got false")
	}

	// Remove two items from the buffer
	buf := make([]int, 2)
	q.Read(buf)
	if q.Used() != 2 {
		t.Errorf("expected Used() to return 2, got %d", q.Used())
	}
	if q.Available() != 2 {
		t.Errorf("expected Available() to return 2, got %d", q.Available())
	}
	if q.IsEmpty() {
		t.Errorf("expected IsEmpty() to return false, got true")
	}
	if q.IsFull() {
		t.Errorf("expected IsFull() to return false, got true")
	}

	// Remove the remaining items
	q.Read(buf[:2])
	if q.Used() != 0 {
		t.Errorf("expected Used() to return 0, got %d", q.Used())
	}
	if q.Available() != 4 {
		t.Errorf("expected Available() to return 4, got %d", q.Available())
	}
	if !q.IsEmpty() {
		t.Errorf("expected IsEmpty() to return true, got false")
	}
	if q.IsFull() {
		t.Errorf("expected IsFull() to return false, got true")
	}
}

