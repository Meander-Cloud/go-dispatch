package dispatcher

import (
	"cmp"
	"fmt"
	"log"
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/v2/trees/redblacktree"
)

type Options struct {
	// number of goroutines to be spawned by this dispatcher instance
	GoroutineCount uint16

	// max number of pending events for each goroutine queue
	QueueLength uint16

	// whether to log event lifecycle
	EnableLogging bool
}

type event[K comparable] struct {
	serialID K
	functor  func()

	t0 time.Time
	t1 time.Time
	t2 time.Time
}

func (e *event[K]) reset() {
	var serialID K
	e.serialID = serialID
	e.functor = nil

	e.t0 = time.Time{}
	e.t1 = time.Time{}
	e.t2 = time.Time{}
}

type goroutineState[K comparable] struct {
	queue chan *event[K]
	load  uint32
}

type Dispatcher[K comparable] struct {
	options   *Options
	eventPool *sync.Pool

	statemu sync.Mutex
	ready   bool

	// goroutineIndex -> goroutineState
	goroutineMap map[uint32]*goroutineState[K]

	// load, goroutineIndex -> not used
	loadTree *rbt.Tree[[2]uint32, struct{}]

	// serialID -> goroutineIndex, inflight serial event count
	serialMap map[K][2]uint32
}

func (d *Dispatcher[K]) process(goroutineIndex uint32, pqueue *chan *event[K]) {
	for {
		err := func() error {
			evt, sentBeforeClosed := <-(*pqueue)
			if !sentBeforeClosed {
				// exit condition
				err := fmt.Errorf("goroutine=%d exiting", goroutineIndex)
				if d.options.EnableLogging {
					log.Printf("%s", err.Error())
				}
				return err
			}

			// recycle event
			defer func() {
				evt.reset()
				d.eventPool.Put(evt)
			}()

			t3 := time.Now().UTC()

			// invoke caller provided functor
			// recover from panic, if any
			func() {
				defer func() {
					rec := recover()
					if rec != nil {
						if d.options.EnableLogging {
							log.Printf("recovered from event functor panic: %s", rec)
						}
					}
				}()

				evt.functor()
			}()

			t4 := time.Now().UTC()
			var t5 time.Time

			// now update structures
			var serialGoroutineIndexCount [2]uint32
			var newGoroutineLoad uint32
			func() {
				d.statemu.Lock()
				defer d.statemu.Unlock()
				t5 = time.Now().UTC()

				goroutineState, found := d.goroutineMap[goroutineIndex]
				if !found {
					// fatal error
					panic(fmt.Errorf("invalid dispatcher state [goroutine map corrupt], fatal"))
				}

				serialGoroutineIndexCount, found = d.serialMap[evt.serialID]
				if !found {
					// fatal error
					panic(fmt.Errorf("invalid dispatcher state [serial map corrupt], fatal"))
				}

				newGoroutineLoad = goroutineState.load - 1

				// update load tree
				d.loadTree.Remove([2]uint32{goroutineState.load, goroutineIndex})
				d.loadTree.Put([2]uint32{newGoroutineLoad, goroutineIndex}, struct{}{})

				// update goroutine state
				goroutineState.load = newGoroutineLoad

				// update serial map
				serialGoroutineIndexCount[1] -= 1
				if serialGoroutineIndexCount[1] > 0 {
					d.serialMap[evt.serialID] = serialGoroutineIndexCount
				} else {
					delete(d.serialMap, evt.serialID)
				}
			}()

			t6 := time.Now().UTC()

			// log event lifecycle
			if d.options.EnableLogging {
				log.Printf(
					"serialID=%v finished on goroutine=%d, load=%d, count=%d, "+
						"dspLockWait=%d, dspAlgElapsed=%d, goQueueWait=%d, evtFuncElapsed=%d, finLockWait=%d, finAlgElapsed=%d",
					evt.serialID,
					goroutineIndex,
					newGoroutineLoad,
					serialGoroutineIndexCount[1],
					evt.t1.Sub(evt.t0).Microseconds(),
					evt.t2.Sub(evt.t1).Microseconds(),
					t3.Sub(evt.t2).Microseconds(),
					t4.Sub(t3).Microseconds(),
					t5.Sub(t4).Microseconds(),
					t6.Sub(t5).Microseconds(),
				)
			}

			return nil
		}()
		if err != nil {
			return
		}
	}
}

func (d *Dispatcher[K]) Stop() {
	d.statemu.Lock()
	defer d.statemu.Unlock()

	// disable dispatch
	d.ready = false

	// close channels to join goroutines
	for _, goroutineState := range d.goroutineMap {
		close(goroutineState.queue)
	}
}

func (d *Dispatcher[K]) Dispatch(serialID K, functor func()) error {
	eventAny := d.eventPool.Get()
	evt, ok := eventAny.(*event[K])
	if !ok {
		err := fmt.Errorf("failed to cast event, corrupted pool, cannot dispatch")
		if d.options.EnableLogging {
			log.Printf("%s", err.Error())
		}
		return err
	}

	evt.serialID = serialID
	evt.functor = functor
	evt.t0 = time.Now().UTC()

	var serialGoroutineIndexCount [2]uint32
	var newGoroutineLoad uint32
	var pqueue *chan *event[K]

	err := func() error {
		d.statemu.Lock()
		defer d.statemu.Unlock()
		evt.t1 = time.Now().UTC()

		if !d.ready {
			err := fmt.Errorf("dispatcher is not ready, cannot dispatch")
			if d.options.EnableLogging {
				log.Printf("%s", err.Error())
			}
			return err
		}

		var goroutineState *goroutineState[K]
		var found bool

		// first check if any inflight event already dispatched for this serialID
		serialGoroutineIndexCount, found = d.serialMap[serialID]
		if found {
			goroutineState, found = d.goroutineMap[serialGoroutineIndexCount[0]]
			if !found {
				err := fmt.Errorf("invalid dispatcher state [goroutine map corrupt], cannot dispatch")
				if d.options.EnableLogging {
					log.Printf("%s", err.Error())
				}
				return err
			}
		} else {
			// find goroutine with least load
			it := d.loadTree.Iterator()
			first := it.Next()
			if !first {
				err := fmt.Errorf("invalid dispatcher state [load tree corrupt], cannot dispatch")
				if d.options.EnableLogging {
					log.Printf("%s", err.Error())
				}
				return err
			}

			loadGoroutineIndex := it.Key()
			goroutineState, found = d.goroutineMap[loadGoroutineIndex[1]]
			if !found {
				err := fmt.Errorf("invalid dispatcher state [goroutine map corrupt], cannot dispatch")
				if d.options.EnableLogging {
					log.Printf("%s", err.Error())
				}
				return err
			}

			// prepare for update
			serialGoroutineIndexCount = [2]uint32{loadGoroutineIndex[1], 0}
		}

		newGoroutineLoad = goroutineState.load + 1

		// update load tree
		d.loadTree.Remove([2]uint32{goroutineState.load, serialGoroutineIndexCount[0]})
		d.loadTree.Put([2]uint32{newGoroutineLoad, serialGoroutineIndexCount[0]}, struct{}{})

		// update goroutine state
		goroutineState.load = newGoroutineLoad

		// update serial map
		serialGoroutineIndexCount[1] += 1
		d.serialMap[serialID] = serialGoroutineIndexCount

		// important note: here we must release statemu before attempting to push to goroutine queue
		// otherwise, if queue is already full, this will deadlock since process() needs to acquire
		// statemu to update structures before pulling the next event from queue
		pqueue = &goroutineState.queue

		// release statemu

		return nil
	}()
	if err != nil {
		return err
	}

	evt.t2 = time.Now().UTC()

	if d.options.EnableLogging {
		log.Printf(
			"dispatching serialID=%v -> goroutine=%d, load=%d, count=%d",
			serialID,
			serialGoroutineIndexCount[0],
			newGoroutineLoad,
			serialGoroutineIndexCount[1],
		)
	}

	// in the very rare scenario where queues are closed during stop,
	// and this caller happens to be concurrently pushing to this queue,
	// recover from push to closed channel panic explicitly
	func() {
		defer func() {
			rec := recover()
			if rec != nil {
				err = fmt.Errorf("%s", rec)
				if d.options.EnableLogging {
					log.Printf("%s", err.Error())
				}
			}
		}()

		(*pqueue) <- evt
	}()
	if err != nil {
		return err
	}
	// do not access evt beyond this point

	return nil
}

func New[K cmp.Ordered](options *Options) *Dispatcher[K] {
	if options == nil {
		panic(fmt.Errorf("nil options"))
	}

	if options.GoroutineCount <= 0 {
		panic(fmt.Errorf("invalid GoroutineCount=%d", options.GoroutineCount))
	}

	if options.QueueLength <= 0 {
		panic(fmt.Errorf("invalid QueueLength=%d", options.QueueLength))
	}

	d := &Dispatcher[K]{
		options: options,
		eventPool: &sync.Pool{
			New: func() any {
				return &event[K]{}
			},
		},

		// statemu
		ready: false,

		goroutineMap: make(map[uint32]*goroutineState[K]),
		loadTree: rbt.NewWith[[2]uint32, struct{}](
			func(a, b [2]uint32) int {
				if a[0] > b[0] {
					return 1
				} else if a[0] < b[0] {
					return -1
				}

				if a[1] > b[1] {
					return 1
				} else if a[1] < b[1] {
					return -1
				}

				return 0
			},
		),
		serialMap: make(map[K][2]uint32),
	}

	// since we are initializing, and there can be no concurrent invocation of Dispatch, no need to lock here

	var i uint16
	for i = 0; i < options.GoroutineCount; i++ {
		goroutineState := &goroutineState[K]{
			queue: make(chan *event[K], options.QueueLength),
			load:  0,
		}

		d.goroutineMap[uint32(i)] = goroutineState

		d.loadTree.Put([2]uint32{0, uint32(i)}, struct{}{})

		// spawn goroutine
		go d.process(uint32(i), &goroutineState.queue)
	}

	// enable dispatch
	d.ready = true

	return d
}
