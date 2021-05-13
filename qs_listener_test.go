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

func TestFileDownloadThrottling(t *testing.T) {
	assert := testifyAssert.New(t)

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
	// Listen for incoming connections.
	listener, err := net.Listen(connType, serverAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("Server::Error listening: %v", err)
	}

	qsListener := New(listener)
	qsListener.SetLimitPerConn(oneMb * 2)
	qsListener.EnableThroughputLogging()
	fmt.Println("Server::Set initial limitPerCon of 2 mb/sec.")

	// Close the listener when the application closes.
	fmt.Printf("Server::Listening on %s\n", listener.Addr().String())

	go func() {
		time.Sleep(5 * time.Second)
		fmt.Println("Server::Global limit will be set to 1 mb/sec.")
		qsListener.SetLimitGlobal(oneMb)
	}()

	go func() {
		for {
			// Listen for an incoming connection.
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
