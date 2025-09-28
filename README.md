# go-dispatch
Lightweight event traffic dispatcher for sequential execution on user specified serial construct

Concept:
- upon `New`, depending on user-specified `Options`, `Dispatcher` will instantiate and spawn a fixed number of goroutines
- each goroutine will have an event queue associated, the length of which is dynamic
- each event carries a user-specified `serialID`, which must be of `comparable` type

Simple dispatch algorithm:
- for each event at time of dispatch, if an event with the same serialID is waiting in a goroutine queue or currently being processed, push event to this goroutine queue
- otherwise, push event to a goroutine queue with the least load, defined as count of events in queue or currently being processed

Guarantees:
- for any one user goroutine dispatching events, execution for events pertaining to each particular serialID is guaranteed to be in sequence
- however if multiple user goroutines are concurrently dispatching events pertaining to the same serialID, although these may land in the same goroutine queue, the sequence across such events originating from multiple user goroutines cannot be guaranteed
- in the case where a goroutine queue is temporarily full, dispatch will block only for users attempting to push event to this full queue, if assigned by dispatch algorithm; concurrently dispatching events to other non-full queues will not block
- upon `Stop`, dispatch is disabled immediately; each spawned goroutine will exit after all assigned events are drained

Limits:
- `GoroutineCount` in `Options` is of type `uint16`, therefore one can instantiate `Dispatcher` with up to 65535 goroutines, however this many goroutines is seldomly needed in a well designed application, and is against the package intention of saving runtime resources by fixing the number of goroutines and event queues at initialization

Dependencies:
- github.com/emirpasic/gods for parts of internal structure
- github.com/Meander-Cloud/go-chdyn for dynamic channels
