package qslistener

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	testifyAssert "github.com/stretchr/testify/assert"
)

const (
	serverAddr = "localhost:0" // Allows to pick a random port
	connType   = "tcp"

	filePath = "testdata/scylla-monitoring-scylla-monitoring-3.7.0.tar.gz"
	oneMb    = 1024 * 1024
)

// This test is mainly for demo purpose (though we verify there are no errors), here is a scenario:
// 1) We start a TCP server with QoS on random available port and start accepting client connections.
// 	  When client connects, we serve a file download,
//	  in our case we use scylla-monitoring-scylla-monitoring-3.7.0.tar.gz
// 	  as a sample file. Throttling is set to 2 MB per sec.
// 2) Then we run a single FileDownloadClient which starts consuming data given a bandwidth of 2 MB per sec.
// 3) After 5 sec. we dynamically update global limit to 1 MB per sec. which decreases a bandwidth for already
//    open connection.
// 4) After another 5 sec. we start another FileDownloadClient which automatically changes throttling and decreases
//	  bandwidth down to 0.5 MB per sec.
// 5) After some time, 1st FileDownloadClient finishes download and bandwidth automatically
//	  increases up to 1 MB per sec. This is because we have a global limit to 1 MB per sec. set previously.
// 6) 2nd FileDownloadClient finishes download and the test ends on this.
func TestFileDownloadThrottling(t *testing.T) {
	assert := testifyAssert.New(t)

	fmt.Println(`This test is mainly for demo purpose (though we verify there are no errors), here is a scenario:
        1) We start a TCP server with QoS on random available port and start accepting client connections.
  		   When client connects, we serve a file download,
		   in our case we use scylla-monitoring-scylla-monitoring-3.7.0.tar.gz
		   as a sample file. Throttling is set to 2 MB per sec.
        2) Then we run a single FileDownloadClient which starts consuming data given a bandwidth of 2 MB per sec. 
        3) After 5 sec. we dynamically update global limit to 1 MB per sec. which decreases a bandwidth for already
           open connection.
        4) After another 5 sec. we start another FileDownloadClient which automatically changes throttling and decreases 
		   bandwidth down to 0.5 MB per sec.
        5) After some time, 1st FileDownloadClient finishes download and bandwidth automatically 
		   increases up to 1 MB per sec. This is because we have a global limit to 1 MB per sec. set previously.
        6) 2nd FileDownloadClient finishes download and the test ends on this.`)

	var wg sync.WaitGroup

	addr, c, err := runFileServer()
	assert.NoError(err)

	wg.Add(1)
	go func() {
		err := runFileDownloadClient(addr)
		assert.NoError(err)
		fmt.Println("TestFileDownloadThrottling::1st client finished its job which will trigger automatic limits update")
		wg.Done()
	}()

	time.Sleep(10 * time.Second)

	wg.Add(1)
	go func() {
		fmt.Println("TestFileDownloadThrottling::Running another client to simulate automatic limits update")
		err := runFileDownloadClient(addr)
		assert.NoError(err)
		wg.Done()
	}()

	wg.Wait()
	c.Close()
}

func runFileServer() (net.Addr, io.Closer, error) {
	fmt.Println("Server::Starting...")

	listener, err := net.Listen(connType, serverAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("Server::Error listening: %v", err)
	}

	qsListener := New(listener)
	qsListener.SetLimitPerConn(oneMb * 2)
	qsListener.EnableThroughputLogging()
	fmt.Println("Server::Set initial limitPerCon of 2 mb/sec.")

	fmt.Printf("Server::Listening on %s\n", listener.Addr().String())

	go func() {
		time.Sleep(5 * time.Second)
		fmt.Println("Server::Global limit will be set to 1 mb/sec.")
		qsListener.SetLimitGlobal(oneMb)
	}()

	go func() {
		for {
			conn, err := qsListener.Accept()
			if err != nil {
				fmt.Printf("Server::Warn about Accept() failure: %v\n", err)
				return
			}
			fmt.Printf("Server::Accepted new connection from %s\n", conn.RemoteAddr().String())

			// Handle connections in a new goroutine.
			go serveFileDownload(conn)
		}
	}()

	return qsListener.Addr(), qsListener, nil
}

func serveFileDownload(conn net.Conn) {
	defer conn.Close()

	f, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Errorf("Server::Error opening file %s: %v", filePath, err))
	}
	defer f.Close()

	copyStart := time.Now()
	bCopied, err := io.Copy(conn, f)
	if err != nil {
		panic(fmt.Errorf("Server::Error copying data from %s: %v", filePath, err))
	}
	fmt.Printf("Server::Transfered total %d bytes to %s, took %v sec.\n", bCopied, conn.LocalAddr().String(),
		time.Since(copyStart).Seconds())
}

type noopWriter struct {
}

func (w noopWriter) Write(b []byte) (n int, err error) {
	return len(b), nil
}

func runFileDownloadClient(serverAddr net.Addr) error {
	fmt.Println("Client::Starting...")
	conn, err := net.Dial(connType, serverAddr.String())
	if err != nil {
		return fmt.Errorf("Client::Error dialing to %s: %v", serverAddr.String(), err)
	}
	defer conn.Close()

	bCopied, err := io.Copy(&noopWriter{}, conn)
	if err != nil {
		return fmt.Errorf("Client[%s]::Error copying data: %v", conn.LocalAddr(), err)
	}
	fmt.Printf("Client[%s]::Copied %d bytes\n", conn.LocalAddr(), bCopied)

	return nil
}
