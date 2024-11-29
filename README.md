# BBQ 🥩

**BBQ** is a thread-safe bounded queue with batch reads/writes,  timeouts and iterators for streaming data processing.

## Installation

To install BBQ, run:

```bash
go get github.com/onur1/bbq
```

## Example

```go
package main

import (
	"fmt"

	"github.com/onur1/bbq"
)

func main() {
	// Create a queue with size 16
	queue := bbq.New[int](16)

	// Producer:
	go func() {
		for i := range 100 {
			_, err := queue.Write(i)
			if err != nil {
				fmt.Println("Write error:", err)
				return
			}
		}
		queue.Close() // Close the queue after writing
	}()

	// Consumer:
	buffer := make([]int, 4) // Batch size of 4
	for {
		n, err := queue.Read(buffer)
		if err != nil {
			if err == bbq.ErrQueueClosed {
				fmt.Println("Queue closed")
				break
			}
			fmt.Println("Read error:", err)
			continue
		}
		fmt.Println("Read:", buffer[:n])
	}
}
```

## API

### Creating a Queue

```go
q := bbq.New[int](size)
```

Creates a new `BBQ` instance with the specified `size`, rounding up to the nearest power of two for optimal performance.


### Writing to the Queue

```go
n, err := q.Write(items...)
```

Adds one or more items to the queue, blocking if the queue is full until space becomes available or the queue is closed. Returns the number of items written or an `ErrQueueClosed` error.

### Reading from the Queue

#### `Read`

```go
n, err := q.Read(buffer)
```

Reads up to `len(buffer)` items from the queue, blocking if the queue is empty until data becomes available or the queue is closed. Returns the number of items read or ErrQueueClosed if the queue has been closed.

#### `ReadUntil`

```go
n, err := q.ReadUntil(buffer, timeout)
```

Reads exactly `len(buffer)` items or until the specified `timeout` elapses, returning early if data becomes available. Returns `ErrQueueClosed` if the queue is closed and fully drained.

### Iterators

#### Stream Items

```go
for item := range q.Items() {
	fmt.Println(item)
}
```

Provides an iterator to stream individual items from the queue.

#### Stream Batches

```go
for batch := range q.Slices(4) {
	fmt.Println(batch)
}
```

Streams batches of items (up to `maxItems`) from the queue.

#### Stream Batches with Timeout

```go
for batch := range q.SlicesWhen(4, time.Second) {
	fmt.Println(batch)
}
```

Streams batches of a specific size or fewer when the timeout expires.

### Managing the Queue

#### Close the Queue

```go
q.Close()
```

Prevents further writes while allowing the consumer to drain remaining data. Once the buffer is fully drained, operations will return `ErrQueueClosed`.

#### Inspecting the Queue

```go
q.Size()       // Total size of the queue
q.Available()  // Remaining space for writes
q.Used()       // Items currently in the queue
q.IsFull()     // Checks if the queue is full
q.IsEmpty()    // Checks if the queue is empty
```

### Piping Between Queues

```go
n, err := src.Pipe(dst)
```

Transfers items from `source` to `dest`, closing `source` if `dest` is closed while keeping `dest` open.

## License

MIT License. See [LICENSE](LICENSE) for details.
