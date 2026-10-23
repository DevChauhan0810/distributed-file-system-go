package p2p

// Every frame on the wire starts with a one-byte type tag followed by a
// fixed-size length header, so the receiver always knows exactly how many
// bytes belong to the frame:
//
//	IncomingMessage: [0x1][uint32 payload length][payload]
//	IncomingStream:  [0x2][int64 stream length][raw stream bytes]
const (
	IncomingMessage = 0x1
	IncomingStream  = 0x2
)

// MaxMessageSize caps the payload of a control message, so a misbehaving peer
// cannot make us allocate an arbitrary amount of memory.
const MaxMessageSize = 1 << 20 // 1 MB

// RPC holds any arbitrary data that is being sent over the
// each transport between two nodes in the network.
type RPC struct {
	From    string
	Payload []byte
	// Stream is true when raw stream bytes follow on the connection. The
	// peer's read loop is paused until the consumer reads exactly
	// StreamSize bytes from the peer and calls CloseStream.
	Stream     bool
	StreamSize int64
}
