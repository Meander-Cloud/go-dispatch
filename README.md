# go-dispatch
Lightweight event traffic dispatcher for sequential execution on user specified serial construct

Concept:
- upon `New`, depending on caller-specified `Options`, `Dispatcher` will instantiate and spawn a fixed number of goroutines
- each goroutine will have an event queue associated, the length of which is caller-specified via `Options`
- each event carries a caller-specified `serialID`, which must be of `comparable` type

Simple dispatch algorithm:
- for each event at time of dispatch, if an event with the same serialID is waiting in a goroutine queue or currently being processed, push event to this goroutine queue
- otherwise, push event to a goroutine queue with the least load, defined as count of events in queue or currently being processed

Guarantees:
- for any one caller goroutine dispatching events, execution for events pertaining to each particular serialID is guaranteed to be in sequence
- however if multiple caller goroutines are concurrently dispatching events pertaining to the same serialID, although these may land in the same goroutine queue, the sequence across such events originating from multiple caller goroutines is not guaranteed
- in the case where a goroutine queue is full, dispatch will block only for callers attempting to push event to the full queue, as assigned by dispatch algorithm; concurrently dispatching events to other non-full queues will not block

Limits:
- `GoroutineCount` in `Options` is of type `uint16`, therefore one can instantiate `Dispatcher` with up to 65535 goroutines, however this many goroutines is seldomly needed in a well designed application, and is against the package intention of saving runtime resources by fixing the number of goroutines and event queues at initialization
- `QueueLength` in `Options` is of type `uint16`, therefore the max number of pending events for each goroutine queue before push would block is 65535, users are advised to specify a queue length within this limit that is inline with the characteristics of the application
- event count in dispatcher internal structures is of type `uint32`, this is to accommodate excessive burst of calls to dispatch that may overflow `uint16`, however this condition may indicate a suboptimal usage pattern, or a traffic pattern not well supported by the dispatch algorithm in this package

Dependencies:
- this package relies on github.com/emirpasic/gods for parts of internal structures
