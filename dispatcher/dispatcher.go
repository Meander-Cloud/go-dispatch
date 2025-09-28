package dispatcher

import (
	"fmt"
	"log"
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/v2/trees/redblacktree"

	"github.com/Meander-Cloud/go-chdyn/chdyn"
)

type Options struct {
	GoroutineCount uint16 // number of goroutines to be spawned by this dispatcher instance

	LogPrefix string
	LogDebug  bool
	LogEvent  bool
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
	exitch  chan struct{}
	eventch *chdyn.Chan[*event[K]]

	// must acquire mutex before accessing
	load uint32
}

type Dispatcher[K comparable] struct {
	*Options
	exitwg  sync.WaitGroup
	eventpl sync.Pool

	mutex sync.Mutex
	ready bool

	// index -> state
	goroutineMap map[uint32]*goroutineState[K]

	// load, index -> not used
	loadTree *rbt.Tree[[2]uint32, struct{}]

	// serialID -> index, inflight event count
	serialMap map[K][2]uint32
}

func New[K comparable](options *Options) *Dispatcher[K] {
	if options == nil {
		panic(fmt.Errorf("nil options"))
	}

	if options.GoroutineCount == 0 {
		panic(fmt.Errorf("%s: invalid GoroutineCount", options.LogPrefix))
	}

	d := &Dispatcher[K]{
		Options: options,
		exitwg:  sync.WaitGroup{},
		eventpl: sync.Pool{
			New: func() any {
				return &event[K]{}
			},
		},
		mutex:        sync.Mutex{},
		ready:        false,
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

	// since we are initializing, there can be no concurrent invocation of Dispatch, no need to lock here

	var i uint16
	for i = 0; i < options.GoroutineCount; i++ {
		index := uint32(i)
		state := &goroutineState[K]{
			exitch: make(chan struct{}, 1),
			eventch: chdyn.New(
				&chdyn.Options[*event[K]]{
					InSize:    chdyn.InSize,
					OutSize:   chdyn.OutSize,
					LogPrefix: fmt.Sprintf("%s<%d>", options.LogPrefix, index),
					LogDebug:  options.LogDebug,
				},
			),
			load: 0,
		}

		d.goroutineMap[index] = state
		d.loadTree.Put([2]uint32{0, index}, struct{}{})

		// spawn goroutine
		d.exitwg.Add(1)
		go d.process(
			index,
			state,
		)
	}

	// enable dispatch
	d.ready = true
	if options.LogDebug {
		log.Printf(
			"%s: dispatch enabled, goroutineMap<%d>, loadTree<%d>, serialMap<%d>",
			options.LogPrefix,
			len(d.goroutineMap),
			d.loadTree.Size(),
			len(d.serialMap),
		)
	}

	return d
}

func (d *Dispatcher[K]) Stop() {
	if d.LogDebug {
		log.Printf("%s: synchronized stop starting", d.LogPrefix)
	}

	func() {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		// disable dispatch
		d.ready = false
		if d.LogDebug {
			log.Printf(
				"%s: dispatch disabled, goroutineMap<%d>, loadTree<%d>, serialMap<%d>",
				d.LogPrefix,
				len(d.goroutineMap),
				d.loadTree.Size(),
				len(d.serialMap),
			)
		}

		// signal goroutines to exit
		for index, state := range d.goroutineMap {
			select {
			case state.exitch <- struct{}{}:
			default:
				if d.LogDebug {
					log.Printf("%s<%d>: exitch already signaled", d.LogPrefix, index)
				}
			}
		}
	}()

	d.exitwg.Wait()
	if d.LogDebug {
		log.Printf("%s: synchronized stop done", d.LogPrefix)
	}
}

func (d *Dispatcher[K]) getEvent() *event[K] {
	evtAny := d.eventpl.Get()
	evt, ok := evtAny.(*event[K])
	if !ok {
		err := fmt.Errorf("%s: failed to cast event, evtAny=%#v", d.LogPrefix, evtAny)
		log.Printf("%s", err.Error())
		panic(err)
	}
	return evt
}

func (d *Dispatcher[K]) returnEvent(evt *event[K]) {
	// recycle event
	evt.reset()
	d.eventpl.Put(evt)
}

func (d *Dispatcher[K]) process(index uint32, state *goroutineState[K]) {
	if d.LogDebug {
		log.Printf("%s<%d>: process goroutine starting", d.LogPrefix, index)
	}

	defer func() {
		state.eventch.Stop()

		if d.LogDebug {
			log.Printf("%s<%d>: process goroutine exiting", d.LogPrefix, index)
		}
		d.exitwg.Done()
	}()

	inShutdown := false

	handle := func(evt *event[K]) bool {
		defer d.returnEvent(evt)

		t3 := time.Now().UTC()

		// invoke user functor
		func() {
			defer func() {
				rec := recover()
				if rec != nil {
					if d.LogDebug {
						log.Printf(
							"%s<%d>: functor recovered from panic: %+v",
							d.LogPrefix,
							index,
							rec,
						)
					}
				}
			}()
			evt.functor()
		}()

		t4 := time.Now().UTC()
		var t5 time.Time
		var indexCountArray [2]uint32
		var newLoad uint32

		func() {
			d.mutex.Lock()
			defer d.mutex.Unlock()

			t5 = time.Now().UTC()

			var found bool
			indexCountArray, found = d.serialMap[evt.serialID]
			if !found {
				err := fmt.Errorf("%s<%d>: serialID=%v not found in serial map", d.LogPrefix, index, evt.serialID)
				log.Printf("%s", err.Error())
				panic(err)
			}

			newLoad = state.load - 1

			// update load tree
			d.loadTree.Remove([2]uint32{state.load, index})
			d.loadTree.Put([2]uint32{newLoad, index}, struct{}{})

			// update goroutine state
			state.load = newLoad

			// update serial map
			indexCountArray[1] -= 1
			if indexCountArray[1] > 0 {
				d.serialMap[evt.serialID] = indexCountArray
			} else {
				delete(d.serialMap, evt.serialID)
			}
		}()

		t6 := time.Now().UTC()

		// log event lifecycle
		if d.LogEvent {
			log.Printf(
				"%s: %v <- <%d|%d|%d>, dspLockWait=%dµs, dspAlgElapsed=%dµs, evtQueueWait=%dµs, evtFuncElapsed=%dµs, finLockWait=%dµs, finAlgElapsed=%dµs",
				d.LogPrefix,
				evt.serialID,
				index,
				newLoad,
				indexCountArray[1],
				evt.t1.Sub(evt.t0).Microseconds(),
				evt.t2.Sub(evt.t1).Microseconds(),
				t3.Sub(evt.t2).Microseconds(),
				t4.Sub(t3).Microseconds(),
				t5.Sub(t4).Microseconds(),
				t6.Sub(t5).Microseconds(),
			)
		}

		if inShutdown &&
			newLoad == 0 {
			if d.LogDebug {
				log.Printf("%s<%d>: all load drained", d.LogPrefix, index)
			}
			return true
		} else {
			return false
		}
	}

	for {
		select {
		case <-state.exitch:
			if d.LogDebug {
				log.Printf("%s<%d>: exitch received", d.LogPrefix, index)
			}

			inShutdown = true

			exit := func() bool {
				d.mutex.Lock()
				defer d.mutex.Unlock()

				if state.load == 0 {
					return true
				} else {
					if d.LogDebug {
						log.Printf("%s<%d>: waiting for load to drain", d.LogPrefix, index)
					}
					return false
				}
			}()
			if exit {
				return // exit
			}
		case evt := <-state.eventch.Out():
			exit := handle(evt)
			if exit {
				return // exit
			}
		}
	}
}

func (d *Dispatcher[K]) Dispatch(serialID K, functor func()) error {
	evt := d.getEvent()
	evt.serialID = serialID
	evt.functor = functor
	evt.t0 = time.Now().UTC()

	var indexCountArray [2]uint32
	var newLoad uint32
	var eventch *chdyn.Chan[*event[K]]

	err := func() error {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		evt.t1 = time.Now().UTC()

		if !d.ready {
			err := fmt.Errorf("%s: ready=%t, cannot dispatch", d.LogPrefix, d.ready)
			if d.LogDebug {
				log.Printf("%s", err.Error())
			}
			return err
		}

		var state *goroutineState[K]
		var found bool

		// first check if any inflight event already dispatched for this serialID
		indexCountArray, found = d.serialMap[serialID]
		if found {
			state, found = d.goroutineMap[indexCountArray[0]]
			if !found {
				err := fmt.Errorf("%s: index=%d not found in goroutine map", d.LogPrefix, indexCountArray[0])
				log.Printf("%s", err.Error())
				return err
			}
		} else {
			// find goroutine with least load
			it := d.loadTree.Iterator()
			first := it.First()
			if !first {
				err := fmt.Errorf("%s: invalid load tree", d.LogPrefix)
				log.Printf("%s", err.Error())
				return err
			}

			loadIndexArray := it.Key()
			state, found = d.goroutineMap[loadIndexArray[1]]
			if !found {
				err := fmt.Errorf("%s: index=%d not found in goroutine map", d.LogPrefix, loadIndexArray[1])
				log.Printf("%s", err.Error())
				return err
			}

			// prepare for update
			indexCountArray = [2]uint32{loadIndexArray[1], 0}
		}

		newLoad = state.load + 1

		// update load tree
		d.loadTree.Remove([2]uint32{state.load, indexCountArray[0]})
		d.loadTree.Put([2]uint32{newLoad, indexCountArray[0]}, struct{}{})

		// update goroutine state
		state.load = newLoad

		// update serial map
		indexCountArray[1] += 1
		d.serialMap[serialID] = indexCountArray

		// prepare to push event
		eventch = state.eventch

		return nil
	}()
	if err != nil {
		d.returnEvent(evt)
		return err
	}

	evt.t2 = time.Now().UTC()

	// log event lifecycle
	if d.LogEvent {
		log.Printf(
			"%s: %v -> <%d|%d|%d>",
			d.LogPrefix,
			serialID,
			indexCountArray[0],
			newLoad,
			indexCountArray[1],
		)
	}

	eventch.In() <- evt
	// do not access evt beyond this point

	return nil
}
