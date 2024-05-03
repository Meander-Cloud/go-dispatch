package main

import (
	"log"
	"time"

	"github.com/Meander-Cloud/go-dispatch/dispatcher"
)

func test1() {
	// instantiate dispatcher and spawn goroutines
	d := dispatcher.New[int64](
		&dispatcher.Options{
			GoroutineCount: 3,
			QueueLength:    6,
			EnableLogging:  true,
		},
	)

	go func() {
		for i := 0; i < 10; i++ {
			// functor execution time is less than dispatch interval
			// therefore events here of serialID 24 may be dispatched to different goroutine queues
			// however execution sequence of events is guaranteed to be in order
			d.Dispatch(
				24,
				func() {
					log.Printf("functor 1 starting")
					<-time.After(time.Millisecond * 1000)
					log.Printf("functor 1 ending")
				},
			)
			<-time.After(time.Millisecond * 1200)
		}
	}()

	<-time.After(time.Millisecond * 500)

	go func() {
		for i := 0; i < 10; i++ {
			// functor execution time is greater than dispatch interval
			// therefore all events here of serialID 21 are expected to be dispatched to the same goroutine queue
			d.Dispatch(
				21,
				func() {
					log.Printf("functor 2 starting")
					<-time.After(time.Millisecond * 1000)
					log.Printf("functor 2 ending")
				},
			)
			<-time.After(time.Millisecond * 800)
		}
	}()

	<-time.After(time.Millisecond * 500)

	go func() {
		for i := 0; i < 50; i++ {
			// here events vary by serialID, and will be dispatched to goroutines based on load at time of each dispatch
			d.Dispatch(
				int64(i),
				func() {
					log.Printf("functor 3 starting")
					<-time.After(time.Millisecond * 250)
					log.Printf("functor 3 ending")
				},
			)
			<-time.After(time.Millisecond * 200)
		}
	}()

	<-time.After(time.Second * 10)

	// disable dispatch and drain events
	d.Stop()

	// this is expected to fail since dispatcher has stopped
	d.Dispatch(
		24,
		func() {
			log.Printf("functor 1 starting")
			<-time.After(time.Millisecond * 1000)
			log.Printf("functor 1 ending")
		},
	)

	<-time.After(time.Second * 10)
}

func main() {
	// enable microsecond logging
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	test1()
}
