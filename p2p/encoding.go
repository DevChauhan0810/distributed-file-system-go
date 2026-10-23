package p2p

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
)

type Decoder interface {
	Decode(io.Reader, *RPC) error
}

type GOBDecoder struct{}

func (dec GOBDecoder) Decode(r io.Reader, msg *RPC) error {
	return gob.NewDecoder(r).Decode(msg)
}

// DefaultDecoder reads one length-prefixed frame (see message.go).
type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader, msg *RPC) error {
	tag := make([]byte, 1)
	if _, err := io.ReadFull(r, tag); err != nil {
		return err
	}

	switch tag[0] {
	case IncomingStream:
		// We do not read the stream itself here, only its length. The
		// consumer reads the bytes directly from the peer.
		var size int64
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			return err
		}
		if size < 0 {
			return fmt.Errorf("invalid stream size %d", size)
		}
		msg.Stream = true
		msg.StreamSize = size
		return nil

	case IncomingMessage:
		var size uint32
		if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
			return err
		}
		if size > MaxMessageSize {
			return fmt.Errorf("message of %d bytes exceeds limit of %d", size, MaxMessageSize)
		}
		msg.Payload = make([]byte, size)
		_, err := io.ReadFull(r, msg.Payload)
		return err

	default:
		return fmt.Errorf("unknown frame type 0x%x", tag[0])
	}
}
