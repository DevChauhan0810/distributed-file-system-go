package p2p

import (
	"io"
	"net"
)

// Peer is an interface that represents the remote node.
type Peer interface {
	net.Conn
	// SendMessage sends one control message frame.
	SendMessage(payload []byte) error
	// SendMessageWithStream sends a control message immediately followed by
	// a stream of exactly size bytes produced by writeFn. Nothing else can be
	// sent to this peer in between, so the receiver always knows which
	// message a stream belongs to.
	SendMessageWithStream(payload []byte, size int64, writeFn func(io.Writer) error) error
	// CloseStream resumes the peer's read loop after an incoming stream
	// has been fully read.
	CloseStream()
}

// Transport is anything that handles the communication
// between the nodes in the network. This can be of the
// form (TCP, UDP, websockets, ...)
type Transport interface {
	Addr() string
	Dial(string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}
