package bbq_test

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/onur1/bbq"
)

func TestReadWriteBasic(t *testing.T) {
	q := bbq.New[int](16)

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
			t.Fatalf("unexpected error %v", err)
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
	q := bbq.New[int](1)

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
		q := bbq.New[int](tt.input)
		if q.Size() != tt.expected {
			t.Fatalf("For input %d, expected size %d, got %d", tt.input, tt.expected, q.Size())
		}
	}
}

func TestReadUntilAvailableBeforeTimeout(t *testing.T) {
	q := bbq.New[int](4)

	go func() {
		time.Sleep(100 * time.Millisecond) // Simulate delay in writing data
		q.Write(1, 2, 3)
		time.Sleep(250 * time.Millisecond)
		q.Write(4)
	}()

	buf := make([]int, 4)
	n, err := q.ReadUntil(buf, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 4 {
		t.Fatalf("expected to read 4 items, but got %d", n)
	}
	expected := []int{1, 2, 3, 4}
	for i := 0; i < n; i++ {
		if buf[i] != expected[i] {
			t.Fatalf("at index %d: expected %d, but got %d", i, expected[i], buf[i])
		}
	}
}

func TestReadUntilTimeoutElapsed(t *testing.T) {
	q := bbq.New[int](4)

	buf := make([]int, 4)

	q.Write(1, 2)

	go func() {
		time.Sleep(500 * time.Millisecond) // Simulate delay beyond timeout
		q.Write(3, 4)
	}()

	n, err := q.ReadUntil(buf, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Assert that only the first two items are read
	if n != 2 {
		t.Fatalf("expected to read 2 items, but got %d", n)
	}
	expected := []int{1, 2}
	for i := 0; i < n; i++ {
		if buf[i] != expected[i] {
			t.Fatalf("at index %d: expected %d, but got %d", i, expected[i], buf[i])
		}
	}

	// Wait for the delayed write to complete
	time.Sleep(400 * time.Millisecond)

	// Read the remaining items
	n, err = q.Read(buf)
	if err != nil {
		t.Fatalf("unexpected error on second read: %v", err)
	}

	// Assert that the remaining items are read correctly
	if n != 2 {
		t.Fatalf("expected to read 2 items, but got %d", n)
	}
	expected = []int{3, 4}
	for i := 0; i < n; i++ {
		if buf[i] != expected[i] {
			t.Fatalf("at index %d: expected %d, but got %d", i, expected[i], buf[i])
		}
	}
}

func TestReadUntilQueueClosedFullyDrained(t *testing.T) {
	q := bbq.New[int](4)

	go func() {
		q.Write(6)
		q.Close()
	}()

	buf := make([]int, 4)

	n, err := q.ReadUntil(buf, 500*time.Millisecond)
	if err != nil && err != bbq.ErrQueueClosed {
		t.Fatalf("unexpected error on close: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected to read 1 item before queue is closed, but got %d", n)
	}
	if buf[0] != 6 {
		t.Fatalf("expected to read 6, but got %d", buf[0])
	}
}

func TestWriteAfterClose(t *testing.T) {
	for _, size := range []int{1, 2, 4} {
		q := bbq.New[int](size)

		if q.IsClosed() {
			t.Fatal("expected to be open")
		}

		n := 0
		for ; n < size; n++ {
			if _, err := q.Write(n); err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		q.Close()

		if !q.IsClosed() {
			t.Fatal("expected to be closed")
		}

		if _, err := q.Write(4); !errors.Is(err, bbq.ErrQueueClosed) {
			t.Fatalf("expected ErrQueueClosed, got %v", err)
		}

		// Verify the items
		buf := make([]int, size)
		n, err := q.Read(buf)
		if n != size || err != nil {
			t.Errorf("expected %d items and no error, got n=%d, err=%v", size, n, err)
		}
	}
}

func TestReadAfterClose(t *testing.T) {
	q := bbq.New[int](16)

	for i := 1; i <= 5; i++ {
		q.Write(i)
	}

	q.Close()

	b := make([]int, 3)

	results := [][]int{
		{1, 2, 3},
		{4, 5},
	}

	var gotErr error

	for i := 0; i < 3; i++ {
		n, err := q.Read(b)
		if err != nil {
			gotErr = err
			break
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

	if !errors.Is(gotErr, bbq.ErrQueueClosed) {
		t.Fatalf("expected a ErrQueueClosed, got %v", gotErr)
	}
}

func TestWriteConcurrent(t *testing.T) {
	q := bbq.New[int](256)

	var wg sync.WaitGroup
	totalProducers := 64
	itemsPerProducer := 4
	expectedTotalItems := totalProducers * itemsPerProducer

	for i := 0; i < totalProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < itemsPerProducer; j++ {
				_, err := q.Write(id*10 + j)
				if err != nil {
					t.Errorf("producer error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	q.Close()

	var collectedItems []int
	for xs := range q.SlicesWhen(expectedTotalItems, 0) {
		collectedItems = append(collectedItems, xs...)
	}

	if len(collectedItems) != expectedTotalItems {
		t.Fatalf("expected %d items, but collected %d", expectedTotalItems, len(collectedItems))
	}

	itemSet := make(map[int]bool)
	for _, item := range collectedItems {
		itemSet[item] = true
	}

	for i := 0; i < totalProducers; i++ {
		for j := 0; j < itemsPerProducer; j++ {
			expectedItem := i*10 + j
			if !itemSet[expectedItem] {
				t.Errorf("missing item: %d", expectedItem)
			}
		}
	}
}

func TestReadEmpty(t *testing.T) {
	q := bbq.New[int](16)

	b := make([]int, 2)

	// Launch a goroutine to get from the empty queue
	ch := make(chan struct{})

	go func() {
		n, err := q.Read(b)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if n != 1 {
			t.Errorf("expected 1, got %d", n)
		}
		ch <- struct{}{}
	}()

	// Simulate pushing an item after a delay
	time.Sleep(time.Millisecond * 30)
	_, err := q.Write(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Wait for the get operation to complete
	<-ch
}

func TestSlices(t *testing.T) {
	q := bbq.New[int](4)

	go func() {
		for i := 1; i <= 10; i++ {
			q.Write(i)
		}
		q.Close()
	}()

	expected := [][]int{
		{1, 2},
		{3, 4},
		{5, 6},
		{7, 8},
		{9, 10},
	}

	actual := [][]int{}
	for items := range q.Slices(2) {
		actual = append(actual, append([]int{}, items...))
	}

	if len(actual) != len(expected) {
		t.Fatalf("expected %d batches, got %d", len(expected), len(actual))
	}
	for i := range expected {
		if !reflect.DeepEqual(actual[i], expected[i]) {
			t.Errorf("expected batch %v, got %v", expected[i], actual[i])
		}
	}
}

func TestSlicesYieldStop(t *testing.T) {
	q := bbq.New[int](4)

	for i := 1; i <= 4; i++ {
		q.Write(i)
	}

	it := q.Slices(2)

	batches := [][]int{}
	it(func(items []int) bool {
		batches = append(batches, append([]int{}, items...))
		return false // Stop iteration after the first batch
	})

	// Validate only the first batch was retrieved
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	if !reflect.DeepEqual(batches[0], []int{1, 2}) {
		t.Fatalf("expected batch [1 2], got %v", batches[0])
	}
}

func TestQueueClosedDuringChunkedWriting(t *testing.T) {
	q := bbq.New[int](4)

	// Fill partially
	q.Write(1, 2)

	done := make(chan error)
	go func() {
		// Producer tries to add more items than the buffer can hold
		_, err := q.Write(3, 4, 5, 6)
		done <- err
	}()

	// Simulate a delay to allow partial processing
	time.Sleep(50 * time.Millisecond)

	// Close the queue mid-operation
	q.Close()

	// Check the error returned to the producer
	err := <-done
	if err == nil || !errors.Is(err, bbq.ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}

func TestQueueClosedWithMultipleWriters(t *testing.T) {
	q := bbq.New[int](2)
	q.Write(1, 2) // Fill the buffer

	producerErrors := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			_, err := q.Write(id)
			producerErrors <- err
		}(i)
	}

	// Simulate a delay to ensure producers are waiting
	time.Sleep(50 * time.Millisecond)

	// Close the queue while producers are blocked
	q.Close()

	// Verify all producers receive ErrQueueClosed
	for i := 0; i < 3; i++ {
		err := <-producerErrors
		if err == nil || !errors.Is(err, bbq.ErrQueueClosed) {
			t.Errorf("expected ErrQueueClosed for producer %d, got %v", i, err)
		}
	}
}

func TestReadWraparound(t *testing.T) {
	q := bbq.New[int](4) // Small buffer to force wrap-around

	// Step 1: Fill the buffer completely
	q.Write(1) // head = 0, tail = 1
	q.Write(2) // head = 0, tail = 2
	q.Write(3) // head = 0, tail = 3
	q.Write(4) // head = 0, tail = 0 (wrap-around)

	// Step 2: Consume some items to move head
	b := make([]int, 2)
	n, err := q.Read(b) // Consume two items, advancing head to 2
	if err != nil || n != 2 {
		t.Fatalf("Expected to consume 2 items, got %d and error %v", n, err)
	}
	if b[0] != 1 || b[1] != 2 {
		t.Fatalf("Unexpected values consumed: %v", b[:n])
	}

	// Step 3: Add more items to cause wrap-around
	if _, err := q.Write(5); err != nil { // head = 2, tail = 1
		t.Fatalf("Expected to put item, got error %v", err)
	}
	if _, err := q.Write(6); err != nil { // head = 2, tail = 2
		t.Fatalf("Expected to put item, got error %v", err)
	}

	// Step 4: Consume items across the wrap-around boundary
	b = make([]int, 4)
	n, err = q.Read(b) // Consume all items, spanning the wrap-around
	if err != nil || n != 4 {
		t.Fatalf("Expected to consume 4 items, got %d and error %v", n, err)
	}
	expected := []int{3, 4, 5, 6}
	for i, v := range expected {
		if b[i] != v {
			t.Fatalf("Unexpected value at index %d: got %d, want %d", i, b[i], v)
		}
	}
}

func TestWriteWraparound(t *testing.T) {
	// Step 1: Create a small buffer
	q := bbq.New[int](4)
	_, err := q.Write(1, 2) // Fill indices 0 and 1
	if err != nil {
		t.Fatalf("unexpected error during initial Write: %v", err)
	}

	// Step 3: Read one item to move the head forward
	buf := make([]int, 1)
	n, err := q.Read(buf) // Read index 0, freeing up one slot
	if err != nil {
		t.Fatalf("unexpected error during initial Read: %v", err)
	}
	if n != 1 || buf[0] != 1 {
		t.Fatalf("expected to read [1], but got %v", buf[:n])
	}

	// Step 4: Write additional items to cause wraparound
	_, err = q.Write(3, 4, 5) // Wraparound occurs here: 3 fills index 2, 4 fills index 3, 5 wraps to index 0
	if err != nil {
		t.Fatalf("unexpected error during wraparound Write: %v", err)
	}

	// Step 5: Read all remaining items to verify correctness
	buf = make([]int, 4)
	n, err = q.Read(buf) // Read indices 1, 2, 3, and wrapped index 0
	if err != nil {
		t.Fatalf("unexpected error during final Read: %v", err)
	}
	if n != 4 {
		t.Fatalf("expected to read 4 items, but got %d", n)
	}

	// Verify data integrity
	expected := []int{2, 3, 4, 5}
	for i := 0; i < n; i++ {
		if buf[i] != expected[i] {
			t.Fatalf("at index %d: expected %d, but got %d", i, expected[i], buf[i])
		}
	}
}

func TestLargeVolumeReadWrite(t *testing.T) {
	q := bbq.New[int](1000000)

	const itemCount = 1_000_000

	go func() {
		for i := 0; i < itemCount; i++ {
			q.Write(i)
		}
		q.Close()
	}()

	popped := 0
	b := make([]int, 1000)
	for {
		n, err := q.Read(b)
		if err != nil {
			if !errors.Is(err, bbq.ErrQueueClosed) {
				t.Fatalf("expected ErrQueueClosed, got %v", err)
			}
			break
		}
		popped += n
	}

	if popped != itemCount {
		t.Errorf("expected to get %d items, got %d", itemCount, popped)
	}
}

func TestUtility(t *testing.T) {
	q := bbq.New[int](4)

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

func TestItems(t *testing.T) {
	q := bbq.New[int](4)

	q.Write(1, 2, 3, 4)

	q.Close()

	var collected []int
	for item := range q.Items() {
		collected = append(collected, item)
	}

	expected := []int{1, 2, 3, 4}
	if !reflect.DeepEqual(collected, expected) {
		t.Fatalf("expected %v, got %v", expected, collected)
	}
}

func TestItemsYieldStop(t *testing.T) {
	q := bbq.New[int](4)

	for i := 1; i <= 4; i++ {
		q.Write(i)
	}

	it := q.Items()

	items := []int{}
	it(func(item int) bool {
		items = append(items, item)
		return false // Stop iteration after the first item
	})

	// Validate only the first item was retrieved
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}

	if items[0] != 1 {
		t.Fatalf("expected item 1, got %v", items[0])
	}
}

func TestPipe(t *testing.T) {
	a := bbq.New[int](4)
	b := bbq.New[int](2)
	c := bbq.New[int](16)

	go a.Pipe(c)
	go b.Pipe(c)

	a.Write(1)
	b.Write(2, 3, 4, 5, 6)
	a.Write(7, 8, 9, 10, 11)

	time.Sleep(time.Millisecond * 30) // Wait for c to collect everything
	if c.Used() != 11 {
		t.Fatalf("expected c to collect 11 items, got %d", c.Used())
	}
	c.Close()

	collected := make([]int, 0)

	for item := range c.Items() {
		collected = append(collected, item)
	}

	sort.Ints(collected)

	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

	if !reflect.DeepEqual(expected, collected) {
		t.Fatalf("expected %v, got %v", expected, collected)
	}
}

func TestPipeSourceClosed(t *testing.T) {
	a := bbq.New[int](2)
	b := bbq.New[int](2)

	time.AfterFunc(time.Millisecond*30, func() {
		a.Close()
	})

	_, err := a.Pipe(b)
	if !errors.Is(err, bbq.ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}

	if !a.IsClosed() {
		t.Fatal("expected a to be closed")
	}

	if b.IsClosed() {
		t.Fatal("expected b to be open")
	}
}

func TestPipeDestinationClosed(t *testing.T) {
	a := bbq.New[int](2)
	b := bbq.New[int](2)
	c := bbq.New[int](1)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := a.Pipe(c)
		if !errors.Is(err, bbq.ErrQueueClosed) {
			t.Errorf("expected ErrQueueClosed, got %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := b.Pipe(c)
		if !errors.Is(err, bbq.ErrQueueClosed) {
			t.Errorf("expected ErrQueueClosed, got %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := a.Write(1)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if n != 1 {
			t.Errorf("expected to have written 1 item, got %d", n)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		n, err := b.Write(2, 3, 4, 5, 6)
		if !errors.Is(err, bbq.ErrQueueClosed) {
			t.Errorf("expected ErrQueueClosed, got %v", err)
		}
		if n != 3 {
			t.Errorf("expected to have written 3 items, got %d", n)
		}
	}()

	c.Close()

	wg.Wait()

	if !a.IsClosed() {
		t.Fatal("expected a to be closed")
	}

	if !b.IsClosed() {
		t.Fatal("expected b to be closed")
	}
}

func TestSlicesDefaultBufferSize(t *testing.T) {
	q := bbq.New[int](16)

	for i := 0; i < 16; i++ {
		q.Write(i)
	}

	q.Close()

	for batch := range q.Slices(0) {
		if len(batch) != 16 {
			t.Fatalf("expected to retrieve 16 items in a batch, got %d", len(batch))
		}
	}
}

func TestSlicesWhenDefaultBufferSize(t *testing.T) {
	q := bbq.New[int](4)

	for i := 0; i < 4; i++ {
		q.Write(i)
	}

	q.Close()

	for batch := range q.SlicesWhen(0, 0) {
		if len(batch) != 4 {
			t.Fatalf("expected to retrieve 16 items in a batch, got %d", len(batch))
		}
	}
}

func TestSlicesWhen(t *testing.T) {
	q := bbq.New[int](8)

	go func() {
		i := 0
		for ; i < 10; i++ {
			q.Write(i)
			time.Sleep(time.Millisecond * 50)
		}
		q.Close()
	}()

	collected := make([][]int, 0)

	for xs := range q.SlicesWhen(4, time.Millisecond*100) {
		ys := make([]int, len(xs))
		copy(ys, xs)
		collected = append(collected, ys)
	}

	if len(collected) < 4 {
		t.Fatalf("expected to collect more than 4 batches, got %d", len(collected))
	}

	for _, xs := range collected {
		if len(xs) > 3 {
			t.Errorf("expected batches to have less than 4 items, got %d", len(xs))
		}
	}
}
