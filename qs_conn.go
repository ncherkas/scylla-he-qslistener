package qslistener

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"math"
	"net"
	"time"
)

type qsConn struct {
	net.Conn
	limiter    *rate.Limiter
	connCloseC chan<- string
}

func newQSConn(conn net.Conn, limitPerConn int, limitUpdateC <-chan int,
	connCloseC chan<- string) *qsConn {

	limiter := rate.NewLimiter(rate.Limit(limitPerConn), 1)
	limiter.SetBurstAt(time.Now().Add(1*time.Second), limitPerConn)
	go func() {
		for updatedLimit := range limitUpdateC {
			if limiter.Limit() != rate.Limit(updatedLimit) {
				limiter.SetLimit(rate.Limit(updatedLimit))
				limiter.SetBurst(updatedLimit)
			}
		}
	}()
	return &qsConn{Conn: conn, limiter: limiter, connCloseC: connCloseC}
}

func (c qsConn) Read(b []byte) (n int, err error) {
	return c.callWithQS(c.Conn.Read, b)
}

func (c qsConn) Write(b []byte) (n int, err error) {
	return c.callWithQS(c.Conn.Write, b)
}

type ioFunc func(b []byte) (n int, err error)

func (c qsConn) callWithQS(targetFunc ioFunc, b []byte) (n int, err error) {
	bLen := len(b)
	var limit int

	for l := 0; l < bLen; l += limit {
		limit = int(c.limiter.Limit())

		var waitN int
		waited := false
		for attempts := 0; attempts < 3 && !waited; attempts++ {
			waitN = int(math.Min(float64(limit), float64(bLen-l)))
			if waited = c.limiter.WaitN(context.Background(), waitN) == nil; !waited {
				limit = int(c.limiter.Limit())
			}
		}
		if !waited {
			return n, fmt.Errorf("failed to wait for limiter permit for %d events: "+
				"all attemts unexpectedly failed due to exceeded burst", waitN)
		}

		r := int(math.Min(float64(l+limit), float64(bLen)))
		subN, err := targetFunc(b[l:r])
		if err != nil {
			return n, err
		}
		n += subN
	}

	return n, err
}

func (c qsConn) Close() error {
	c.connCloseC <- c.RemoteAddr().String()
	return c.Conn.Close()
}
