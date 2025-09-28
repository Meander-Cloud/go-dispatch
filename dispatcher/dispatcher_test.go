package dispatcher_test

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/Meander-Cloud/go-dispatch/dispatcher"
)

func Test1(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	d := dispatcher.New[uint8](
		&dispatcher.Options{
			GoroutineCount: 3,
			LogPrefix:      "Test1",
			LogDebug:       true,
			LogEvent:       true,
		},
	)

	go func() {
		for range 10 {
			// functor execution time is greater than dispatch interval
			// therefore all events here of serialID 1 are expected to be dispatched to the same goroutine queue
			d.Dispatch(
				1,
				func() {
					log.Printf("Test1: functor starting")
					<-time.After(time.Millisecond * 500)
					log.Printf("Test1: functor ending")
				},
			)
			<-time.After(time.Millisecond * 400)
		}
	}()

	<-time.After(time.Second * 6)
	d.Stop()
}

func Test2(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	d := dispatcher.New[uint8](
		&dispatcher.Options{
			GoroutineCount: 3,
			LogPrefix:      "Test2",
			LogDebug:       false,
			LogEvent:       true,
		},
	)

	go func() {
		for range 10 {
			// functor execution time is less than dispatch interval
			// therefore events here of serialID 2 may get dispatched to different goroutine queues
			// however execution sequence of events is guaranteed to be in order
			d.Dispatch(
				2,
				func() {
					log.Printf("Test2: functor starting")
					<-time.After(time.Millisecond * 400)
					log.Printf("Test2: functor ending")
				},
			)
			<-time.After(time.Millisecond * 500)
		}
	}()

	<-time.After(time.Second * 6)
	d.Stop()
}

func Test3(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	d := dispatcher.New[string](
		&dispatcher.Options{
			GoroutineCount: 3,
			LogPrefix:      "Test3",
			LogDebug:       true,
			LogEvent:       true,
		},
	)

	go func() {
		for i := 0; i < 50; i++ {
			// here events vary by serialID, and will be dispatched to goroutines based on load at time of each dispatch
			serialID := fmt.Sprintf("%d", i)
			d.Dispatch(
				serialID,
				func() {
					log.Printf("Test3: functor for %s starting", serialID)
					<-time.After(time.Millisecond * 100)
					log.Printf("Test3: functor for %s ending", serialID)
				},
			)
			<-time.After(time.Millisecond * 20)
		}
	}()

	// test stop before all events are processed
	<-time.After(time.Millisecond * 1600)
	d.Stop()
}

func Test4(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	d := dispatcher.New[struct{}](
		&dispatcher.Options{
			GoroutineCount: 1,
			LogPrefix:      "Test4",
			LogDebug:       true,
			LogEvent:       true,
		},
	)

	d.Stop()

	// this is expected to fail since dispatcher has stopped
	d.Dispatch(
		struct{}{},
		func() {
			log.Printf("Test4: functor cannot reach")
		},
	)
}
