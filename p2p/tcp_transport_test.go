package p2p

import (
	"bytes"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	opts := TCPTransportOpts{
		ListenAddr:    ":3000",
		HandshakeFunc: NOPHandshakeFunc,
		Decoder:       DefaultDecoder{},
	}
	tr := NewTCPTransport(opts)
	assert.Equal(t, tr.ListenAddr, ":3000")

	assert.Nil(t, tr.ListenAndAccept())
	tr.Close()
}

// Messages and streams sent back to back must come out as separate frames,
// with the stream bytes left untouched on the connection for the consumer.
func TestFraming(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	peer := NewTCPPeer(client, true)
	stream := bytes.Repeat([]byte("stream-bytes "), 10000) // larger than one read

	go func() {
		peer.SendMessage([]byte("first"))
		peer.SendMessageWithStream([]byte("second"), int64(len(stream)), func(w io.Writer) error {
			_, err := w.Write(stream)
			return err
		})
		peer.SendMessage([]byte("third"))
	}()

	dec := DefaultDecoder{}
	read := func() RPC {
		var rpc RPC
		assert.Nil(t, dec.Decode(server, &rpc))
		return rpc
	}

	assert.Equal(t, "first", string(read().Payload))
	assert.Equal(t, "second", string(read().Payload))

	rpc := read()
	assert.True(t, rpc.Stream)
	assert.Equal(t, int64(len(stream)), rpc.StreamSize)
	got := make([]byte, rpc.StreamSize)
	_, err := io.ReadFull(server, got)
	assert.Nil(t, err)
	assert.Equal(t, stream, got)

	assert.Equal(t, "third", string(read().Payload))
}

func TestStreamSizeMismatchClosesConnection(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()

	peer := NewTCPPeer(client, true)
	go io.Copy(io.Discard, server)

	err := peer.SendMessageWithStream([]byte("msg"), 100, func(w io.Writer) error {
		_, err := w.Write([]byte("only 10 b."))
		return err
	})
	assert.NotNil(t, err)

	// The connection must be closed so the receiver cannot get out of sync.
	_, err = client.Write([]byte("x"))
	assert.NotNil(t, err)
}

func TestDecoderRejectsOversizedMessage(t *testing.T) {
	frame := []byte{IncomingMessage, 0xff, 0xff, 0xff, 0x7f}
	var rpc RPC
	assert.NotNil(t, DefaultDecoder{}.Decode(bytes.NewReader(frame), &rpc))
}
