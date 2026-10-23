package p2p

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

// TCPPeer represents the remote node over a TCP established connection.
type TCPPeer struct {
	// The underlying connection of the peer. Which in this case
	// is a TCP connection.
	net.Conn
	// if we dial and retrieve a conn => outbound == true
	// if we accept and retrieve a conn => outbound == false
	outbound bool

	// sendMu makes sure frames from different goroutines are never
	// interleaved on the connection.
	sendMu sync.Mutex
	// wg pauses the read loop while a consumer reads an incoming stream.
	wg *sync.WaitGroup
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outbound: outbound,
		wg:       &sync.WaitGroup{},
	}
}

func (p *TCPPeer) CloseStream() {
	p.wg.Done()
}

func (p *TCPPeer) SendMessage(payload []byte) error {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	return p.writeMessage(payload)
}

func (p *TCPPeer) SendMessageWithStream(payload []byte, size int64, writeFn func(io.Writer) error) error {
	p.sendMu.Lock()
	defer p.sendMu.Unlock()

	if err := p.writeMessage(payload); err != nil {
		return err
	}

	header := make([]byte, 9)
	header[0] = IncomingStream
	binary.LittleEndian.PutUint64(header[1:], uint64(size))
	if _, err := p.Conn.Write(header); err != nil {
		return err
	}

	cw := &countingWriter{w: p.Conn}
	err := writeFn(cw)
	if err == nil && cw.n != size {
		err = fmt.Errorf("stream wrote %d bytes, announced %d", cw.n, size)
	}
	if err != nil {
		// The receiver is now out of sync with us, the only safe thing to
		// do is to drop the connection.
		p.Conn.Close()
	}
	return err
}

func (p *TCPPeer) writeMessage(payload []byte) error {
	if len(payload) > MaxMessageSize {
		return fmt.Errorf("message of %d bytes exceeds limit of %d", len(payload), MaxMessageSize)
	}
	frame := make([]byte, 5+len(payload))
	frame[0] = IncomingMessage
	binary.LittleEndian.PutUint32(frame[1:], uint32(len(payload)))
	copy(frame[5:], payload)
	_, err := p.Conn.Write(frame)
	return err
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.n += int64(n)
	return n, err
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

type TCPTransport struct {
	TCPTransportOpts
	listener net.Listener
	rpcch    chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch:            make(chan RPC, 1024),
	}
}

// Addr implements the Transport interface return the address
// the transport is accepting connections.
func (t *TCPTransport) Addr() string {
	return t.ListenAddr
}

// Consume implements the Tranport interface, which will return read-only channel
// for reading the incoming messages received from another peer in the network.
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

// Close implements the Transport interface.
func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

// Dial implements the Transport interface.
func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	go t.handleConn(conn, true)

	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	log.Printf("TCP transport listening on port: %s\n", t.ListenAddr)

	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}

		if err != nil {
			fmt.Printf("TCP accept error: %s\n", err)
			continue
		}

		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var err error

	defer func() {
		fmt.Printf("dropping peer connection: %s\n", err)
		conn.Close()
	}()

	peer := NewTCPPeer(conn, outbound)

	if err = t.HandshakeFunc(peer); err != nil {
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

	// Read loop. This is the only goroutine reading from the connection,
	// except while it is paused waiting for a stream to be consumed.
	for {
		rpc := RPC{}
		err = t.Decoder.Decode(conn, &rpc)
		if err != nil {
			return
		}

		rpc.From = conn.RemoteAddr().String()

		if rpc.Stream {
			// Hand the stream to the consumer and wait until it has read
			// all StreamSize bytes, so we never read concurrently with it.
			peer.wg.Add(1)
			t.rpcch <- rpc
			peer.wg.Wait()
			continue
		}

		t.rpcch <- rpc
	}
}
