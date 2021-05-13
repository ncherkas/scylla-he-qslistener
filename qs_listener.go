package qslistener

import (
	"net"
	"sync"
)

// net.Listener with Quality-of-Service support
type QSListener struct {
	net.Listener
	mu               sync.Mutex
	limitPerConn     int // Throughput limit per connection
	limitGlobal      int // Global throughput limit across all listener connections
	connLimitUpdateC map[string]chan int
	connCloseC       chan string
	logThroughput    bool // Whether to log the throughput metrics
}

func New(l net.Listener) *QSListener {
	qsl := &QSListener{Listener: l, connLimitUpdateC: make(map[string]chan int), connCloseC: make(chan string)}
	go qsl.handleConnCloseC()
	return qsl
}

func (l *QSListener) handleConnCloseC() {
	// Trigger limits recalculation on connection close
	for addr := range l.connCloseC {
		l.mu.Lock()
		close(l.connLimitUpdateC[addr])
		delete(l.connLimitUpdateC, addr)
		l.propagateLimitChange()
		l.mu.Unlock()
	}
}

func (l *QSListener) propagateLimitChange() {
	actualLimitPerConn := l.actualLimitPerConn()
	for _, c := range l.connLimitUpdateC {
		c <- actualLimitPerConn
	}
}

func (l *QSListener) SetLimitPerConn(limitPerConn int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limitPerConn = limitPerConn
	l.propagateLimitChange()
}

func (l *QSListener) actualLimitPerConn() int {
	connCount := len(l.connLimitUpdateC)
	limitPerConn := l.limitPerConn
	limitGlobal := l.limitGlobal

	if limitGlobal > 0 && connCount > 0 {
		globalLimitPerConn := limitGlobal / connCount
		if globalLimitPerConn < limitPerConn {
			return globalLimitPerConn
		}
	}
	return limitPerConn
}

func (l *QSListener) SetLimitGlobal(limitGlobal int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limitGlobal = limitGlobal
	l.propagateLimitChange()
}

func (l *QSListener) EnableThroughputLogging() {
	// Applies only to new connections
	l.logThroughput = true
}

func (l *QSListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	limitUpdateC := make(chan int)
	if l.logThroughput {
		conn = newLoggingConn(conn)
	}
	qsc := newQSConn(conn, l.limitPerConn, limitUpdateC, l.connCloseC)
	l.connLimitUpdateC[conn.RemoteAddr().String()] = limitUpdateC
	l.propagateLimitChange() // Trigger limits recalculation on new connection
	return qsc, nil
}

func (l *QSListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.connLimitUpdateC) == 0 {
		close(l.connCloseC)
	}
	return l.Listener.Close()
}
