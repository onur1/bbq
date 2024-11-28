package bbq

import (
	"errors"
	"iter"
	"sync"
	"time"
)

var (
	// ErrQueueClosed is returned when operations are attempted on a closed queue.
	ErrQueueClosed = errors.New("bbq: operation on closed queue")
	// ErrInvalidSize is returned when a new size is invalid (e.g., smaller than the current size).
	ErrInvalidSize = errors.New("bbq: new size must be greater than the current size")
)

// BBQ is a thread-safe bounded queue that supports batch reads/writes and timeouts.
type BBQ[T any] struct {
	mask     int        // Mask for index wrapping
	mu       sync.Mutex // Protects all fields below
	size     int        // Size of the ring buffer
	buf      []T        // Ring buffer
	head     int        // Read index
	tail     int        // Write index
	count    int        // Number of items in the buffer
	canRead  *sync.Cond // Condition for consumers waiting for data
	canWrite *sync.Cond // Condition for producers waiting for space
	done     bool       // Flag to signal that queue is closed
	expired  bool       // Flag to signal that read operation timed out
}

// New creates a new BBQ instance with the specified size, rounding the size up
// to the nearest power of two if it is not already.
func New[T any](size int) *BBQ[T] {
	if size <= 0 || (size&(size-1)) != 0 {
		n := 1
		for n < size {
			n <<= 1
		}
		size = n
	}
	e := &BBQ[T]{
		buf:  make([]T, size),
		size: size,
		mask: size - 1,
	}
	e.canRead = sync.NewCond(&e.mu)
	e.canWrite = sync.NewCond(&e.mu)
	return e
}

// Write adds one or more items to the queue.
//   - Blocks if the buffer is full until space becomes available or the queue is closed.
//   - Returns the number of items added or ErrQueueClosed if the queue has been closed.
//
// Example:
//
//	n, err := q.Write(item1, item2, item3)
//	if err != nil {
//	    // Handle error (e.g., queue is closed).
//	}
func (e *BBQ[T]) Write(items ...T) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.done {
		return 0, ErrQueueClosed
	}

	var (
		writes int
		total  int
		p1     int
	)

	// Process items in chunks that fit the buffer
	for len(items) > 0 {
		for e.size-e.count == 0 {
			if e.done {
				return total, ErrQueueClosed
			}
			e.canWrite.Wait()
			continue
		}

		// Calculate how many items to insert in this iteration
		writes = min(len(items), e.size-e.count)
		total += writes

		if e.tail+writes <= e.size {
			copy(e.buf[e.tail:], items[:writes])
		} else {
			// Wrap-around case: copy in two steps
			p1 = e.size - e.tail
			copy(e.buf[e.tail:], items[:p1])
			copy(e.buf[:writes-p1], items[p1:writes])
		}

		e.tail = (e.tail + writes) & e.mask
		e.count += writes

		// Update remaining items
		items = items[writes:]

		// Notify the consumers that new data is available
		e.canRead.Signal()
	}

	return total, nil
}

// read retrieves items from the queue into the provided slice.
//   - If waitForFull is true, it blocks until the requested number of items (len(b)) is available.
//   - If waitForFull is false, it retrieves as many items as are currently available, up to len(b).
func (e *BBQ[T]) read(b []T, waitForFull bool) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		reads int
		p1    int
	)

	for {
		// Handle closed queue
		if e.done {
			if e.count == 0 {
				return 0, ErrQueueClosed
			}
			break
		}

		// Handle expired timer; drain the buffer if there's data
		if e.expired {
			e.expired = false // Reset expired flag for the next cycle
			if e.count > 0 {
				break // Proceed to read data
			}
			// Buffer is empty; continue waiting
		}

		if waitForFull && e.count < len(b) || !waitForFull && e.count == 0 {
			e.canRead.Wait()
			continue
		}

		break
	}

	// Determine how many items to read
	reads = min(len(b), e.count)

	if reads > 0 {
		// Read items from the buffer
		if e.head+reads <= e.size {
			copy(b[:reads], e.buf[e.head:e.head+reads])
		} else {
			// Wraparound case: copy in two steps
			p1 = e.size - e.head
			copy(b[:p1], e.buf[e.head:])
			copy(b[p1:], e.buf[:reads-p1])
		}

		// Update the head pointer and count
		e.head = (e.head + reads) & e.mask
		e.count -= reads
	}

	e.canWrite.Signal() // Notify producers

	return reads, nil
}

// Read reads up to len(b) items from the queue into the provided slice b.
//   - Blocks if the buffer is empty until items become available or the queue is closed.
//   - Returns the number of items added or ErrQueueClosed if the queue has been closed.
//
// Example:
//
//	buffer := make([]T, 10)
//	n, err := queue.Read(buffer)
//	if err != nil {
//	    // Handle error (e.g., queue is closed).
//	}
//	fmt.Println("Got items:", buffer[:n])
func (e *BBQ[T]) Read(b []T) (int, error) {
	return e.read(b, false)
}

// Close marks the queue as closed and prevents potential deadlocks.
//   - After closing, no new items can be added using Put, but the consumer
//     will continue retrieving data until the buffer is fully drained, at
//     which point a queue closed error will be returned.
//   - Any goroutines blocked on Put or Read will be unblocked.
//   - Subsequent calls to Close will have no effect.
func (e *BBQ[T]) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.done {
		e.done = true
		e.canRead.Broadcast()
		e.canWrite.Broadcast()
	}
}

// Pipe transfers items from the source BBQ to the destination BBQ.
//   - The source will close if the destination is closed, but the destination remains
//     unaffected if the source is closed.
//
// Returns the number of items written to the destination in the final operation,
// or an error if one of the queues is closed.
func (e *BBQ[T]) Pipe(dest *BBQ[T]) (int, error) {
	buf := make([]T, min(e.Size(), dest.Size()))

	var (
		n   int
		err error
	)

	for {
		n, err = e.read(buf, false)
		if err != nil || n == 0 {
			return 0, err
		}

		if n, err = dest.Write(buf[:n]...); err != nil {
			if errors.Is(err, ErrQueueClosed) {
				e.Close()
			}
			return n, err
		}
	}
}

// ReadUntil retrieves items from the queue into the provided slice b, waiting until
// either len(b) items are available, or the specified timeout duration elapses. If no more
// batches are expected, the returned number of items may be less than len(b).
//
// Returns the number of items read and any error encountered.
func (e *BBQ[T]) ReadUntil(b []T, timeout time.Duration) (n int, err error) {
	stopTimer := make(chan struct{})
	defer close(stopTimer)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	go func(wait <-chan time.Time, stop <-chan struct{}) {
		select {
		case _, ok := <-wait:
			if ok {
				e.mu.Lock()
				e.expired = true
				e.canRead.Signal()
				e.mu.Unlock()
			}
		case <-stop:
		}
	}(timer.C, stopTimer)

	n, err = e.read(b, true)

	return
}

// Items returns an iterator to read items from the BBQ buffer.
func (e *BBQ[T]) Items() iter.Seq[T] {
	next, stop := iter.Pull(e.getIterator(make([]T, e.Size()), false, 0))
	return func(yield func(T) bool) {
		var (
			xs []T
			x  T
			ok bool
		)
		for {
			xs, ok = next()
			if !ok {
				return
			}
			for _, x = range xs {
				if !yield(x) {
					stop()
					return
				}
			}
		}
	}
}

// Slices returns an iterator to read items from the BBQ buffer in batches of up to maxItems.
//   - If maxItems is less than or equal to 0, or exceeds the buffer size, it
//     defaults to the buffer size.
func (e *BBQ[T]) Slices(maxItems int) iter.Seq[[]T] {
	if maxItems <= 0 || maxItems > e.Size() {
		maxItems = e.Size()
	}
	return e.getIterator(make([]T, maxItems), false, 0)
}

// SlicesWhen returns an iterator that reads items from the BBQ buffer into batches of
// requiredItems, or fewer if the buffer is closed or the timeout expires.
//   - If requiredItems is less than or equal to 0, or exceeds the buffer size, it
//     defaults to the buffer size.
//   - If timeout is greater than 0, the iterator emits the current buffer contents
//     when the timeout elapses. A value of 0 disables the timeout.
func (e *BBQ[T]) SlicesWhen(requiredItems int, timeout time.Duration) iter.Seq[[]T] {
	if requiredItems <= 0 || requiredItems > e.Size() {
		requiredItems = e.Size()
	}
	return e.getIterator(make([]T, requiredItems), true, timeout)
}

func (e *BBQ[T]) getIterator(buf []T, waitForFull bool, timeout time.Duration) iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		var (
			n   int
			err error
		)

		var (
			timer     *time.Timer
			stopTimer chan struct{}
		)

		if timeout > 0 {
			stopTimer = make(chan struct{})
			defer close(stopTimer)

			timer = time.NewTimer(timeout)
			defer timer.Stop()

			go func() {
				for {
					select {
					case <-timer.C:
						e.mu.Lock()
						e.expired = true
						e.canRead.Signal()
						e.mu.Unlock()
					case <-stopTimer:
						return
					}
				}
			}()

			waitForFull = true
		}

		for {
			n, err = e.read(buf, waitForFull)
			if err != nil || n == 0 {
				return
			}
			if !yield(buf[:n]) {
				return
			}
			if timer != nil {
				timer.Reset(timeout)
			}
		}
	}
}

// Size returns the total size of the queue.
func (e *BBQ[T]) Size() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.size
}

// IsClosed returns true if the queue is closed.
func (e *BBQ[T]) IsClosed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.done
}

// Available calculates the remaining space in the queue for new items, indicating
// how many more can be buffered without blocking.
func (e *BBQ[T]) Available() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.size - e.count
}

// Used provides the number of items currently in the queue that are not read yet.
func (e *BBQ[T]) Used() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count
}

// IsFull returns true if the queue is full.
func (e *BBQ[T]) IsFull() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count == e.size
}

// IsEmpty returns true if the queue is empty.
func (e *BBQ[T]) IsEmpty() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.count == 0
}
