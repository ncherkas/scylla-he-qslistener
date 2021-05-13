package qslistener

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Connection wrapper which calculates and logs the throughput metric
type loggingConn struct {
	net.Conn
	mu     sync.Mutex
	events chan interface{}
	closed bool
}

func newLoggingConn(conn net.Conn) *loggingConn {
	rc := &loggingConn{Conn: conn, events: make(chan interface{})}
	go rc.startLogging()
	return rc
}

func (r *loggingConn) startLogging() {
	go func() {
		tick := time.NewTicker(1 * time.Second)
		for t := range tick.C {
			if r.isClosed() {
				tick.Stop()
				close(r.events)
				return
			}

			r.events <- t
		}
	}()
	var bCounter int64
	for e := range r.events {
		switch e.(type) {
		case int64:
			bCounter += e.(int64)
		case time.Time:
			fmt.Printf("Connection[%v]::Current throughput is %d bytes/sec.\n", r.RemoteAddr(), bCounter)
			bCounter = 0
		}
	}
}

func (r *loggingConn) Write(b []byte) (n int, err error) {
	r.events <- int64(len(b))
	return r.Conn.Write(b)
}

func (r *loggingConn) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *loggingConn) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return r.Conn.Close()
}
