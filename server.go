package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/anthdm/foreverstore/p2p"
)

// peerResponseTimeout is how long Get waits for a peer to answer a request
// before moving on to the next peer.
const peerResponseTimeout = 5 * time.Second

// bootstrapAttempts is how many times to try reaching a bootstrap node
// (about 13 seconds in total with the backoff).
const bootstrapAttempts = 8

type FileServerOpts struct {
	ID                string
	EncKey            []byte
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerLock sync.Mutex
	peers    map[string]p2p.Peer

	// pendingStreams holds, per peer, the message announcing the stream that
	// arrives next on that connection. Only touched by the loop goroutine.
	pendingStreams map[string]any

	// fetchLock allows one network fetch at a time, and getLock protects
	// pendingGets, the responses Get is waiting for, keyed by peer address.
	fetchLock   sync.Mutex
	getLock     sync.Mutex
	pendingGets map[string]chan getResponse

	store  *Store
	quitch chan struct{}
}

type getResponse struct {
	found bool
	size  int64
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = generateID()
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          NewStore(storeOpts),
		quitch:         make(chan struct{}),
		peers:          make(map[string]p2p.Peer),
		pendingStreams: make(map[string]any),
		pendingGets:    make(map[string]chan getResponse),
	}
}

type Message struct {
	Payload any
}

// MessageStoreFile announces an encrypted file stream that follows it.
type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

// MessageGetFile asks a peer for a file it stores for us.
type MessageGetFile struct {
	ID  string
	Key string
}

// MessageGetFileResponse answers a MessageGetFile. When Found is true the
// file stream follows it.
type MessageGetFileResponse struct {
	Key   string
	Found bool
	Size  int64
}

func encodeMessage(msg *Message) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *FileServer) peerList() []p2p.Peer {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	peers := make([]p2p.Peer, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (s *FileServer) getPeer(addr string) (p2p.Peer, bool) {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	peer, ok := s.peers[addr]
	return peer, ok
}

// Get returns the file for key, fetching it from the network if it is not on
// the local disk. Peers are asked one at a time, and the first peer that has
// the file is the only one that sends it. The caller must close the reader.
func (s *FileServer) Get(key string) (io.ReadCloser, error) {
	if s.store.Has(s.ID, key) {
		fmt.Printf("[%s] serving file (%s) from local disk\n", s.Transport.Addr(), key)
		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}

	fmt.Printf("[%s] dont have file (%s) locally, fetching from network...\n", s.Transport.Addr(), key)

	s.fetchLock.Lock()
	defer s.fetchLock.Unlock()

	payload, err := encodeMessage(&Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: hashKey(key),
		},
	})
	if err != nil {
		return nil, err
	}

	for _, peer := range s.peerList() {
		addr := peer.RemoteAddr().String()

		resp, err := s.requestFile(peer, payload)
		if err != nil {
			log.Printf("[%s] request to %s failed: %v", s.Transport.Addr(), addr, err)
			continue
		}
		if !resp.found {
			fmt.Printf("[%s] peer %s does not have (%s), trying next peer\n", s.Transport.Addr(), addr, key)
			continue
		}

		stream := &io.LimitedReader{R: peer, N: resp.size}
		n, err := s.store.WriteDecrypt(s.EncKey, s.ID, key, stream, resp.size)
		// Consume whatever is left of the stream before resuming the read loop.
		io.Copy(io.Discard, stream)
		peer.CloseStream()

		if err != nil {
			log.Printf("[%s] bad copy of (%s) from %s: %v", s.Transport.Addr(), key, addr, err)
			continue
		}

		fmt.Printf("[%s] received (%d) bytes over the network from (%s)\n", s.Transport.Addr(), n, addr)

		_, r, err := s.store.Read(s.ID, key)
		return r, err
	}

	return nil, fmt.Errorf("[%s] file (%s) not found on any peer", s.Transport.Addr(), key)
}

// requestFile sends a get request to peer and waits for its answer. When the
// answer is found, the file stream is ready to be read from peer.
func (s *FileServer) requestFile(peer p2p.Peer, payload []byte) (getResponse, error) {
	addr := peer.RemoteAddr().String()
	ch := make(chan getResponse, 1)

	s.getLock.Lock()
	s.pendingGets[addr] = ch
	s.getLock.Unlock()

	if err := peer.SendMessage(payload); err != nil {
		s.getLock.Lock()
		delete(s.pendingGets, addr)
		s.getLock.Unlock()
		return getResponse{}, err
	}

	select {
	case resp := <-ch:
		return resp, nil
	case <-time.After(peerResponseTimeout):
		s.getLock.Lock()
		_, stillPending := s.pendingGets[addr]
		delete(s.pendingGets, addr)
		s.getLock.Unlock()
		if stillPending {
			return getResponse{}, fmt.Errorf("no response after %s", peerResponseTimeout)
		}
		// The response was delivered just as we timed out.
		return <-ch, nil
	}
}

// deliverGetResponse hands resp to a waiting Get, if there is one.
func (s *FileServer) deliverGetResponse(from string, resp getResponse) bool {
	s.getLock.Lock()
	ch, ok := s.pendingGets[from]
	delete(s.pendingGets, from)
	s.getLock.Unlock()

	if ok {
		ch <- resp
	}
	return ok
}

// Store writes the file to the local disk, then streams an encrypted copy to
// every peer straight from that file. The file is never held in memory, so
// memory use stays the same whatever the file size.
func (s *FileServer) Store(key string, r io.Reader) error {
	size, err := s.store.Write(s.ID, key, r)
	if err != nil {
		return err
	}

	encSize := encryptedSize(size)
	payload, err := encodeMessage(&Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  hashKey(key),
			Size: encSize,
		},
	})
	if err != nil {
		return err
	}

	peers := s.peerList()
	replicated := 0
	for _, peer := range peers {
		err := peer.SendMessageWithStream(payload, encSize, func(w io.Writer) error {
			_, f, err := s.store.Read(s.ID, key)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = copyEncrypt(s.EncKey, f, w)
			return err
		})
		if err != nil {
			log.Printf("[%s] failed to replicate (%s) to %s: %v", s.Transport.Addr(), key, peer.RemoteAddr(), err)
			continue
		}
		replicated++
	}

	fmt.Printf("[%s] stored (%s): %d bytes on local disk, encrypted copy sent to %d/%d peers\n",
		s.Transport.Addr(), key, size, replicated, len(peers))

	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) OnPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	s.peers[p.RemoteAddr().String()] = p

	log.Printf("connected with remote %s", p.RemoteAddr())

	return nil
}

func (s *FileServer) loop() {
	defer func() {
		log.Println("file server stopped due to error or user quit action")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			if rpc.Stream {
				s.handleStream(rpc)
				continue
			}

			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("decoding error: ", err)
				continue
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error: ", err)
			}

		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		s.pendingStreams[from] = v
	case MessageGetFile:
		// Serve in the background so a large transfer does not block
		// messages from other peers.
		go func() {
			if err := s.handleMessageGetFile(from, v); err != nil {
				log.Println("handle message error: ", err)
			}
		}()
	case MessageGetFileResponse:
		if v.Found {
			s.pendingStreams[from] = v
		} else {
			s.deliverGetResponse(from, getResponse{found: false})
		}
	}

	return nil
}

// handleStream dispatches an incoming stream to whatever the message right
// before it on the same connection announced.
func (s *FileServer) handleStream(rpc p2p.RPC) {
	peer, ok := s.getPeer(rpc.From)
	if !ok {
		log.Printf("[%s] stream from unknown peer %s", s.Transport.Addr(), rpc.From)
		return
	}

	announced := s.pendingStreams[rpc.From]
	delete(s.pendingStreams, rpc.From)

	switch m := announced.(type) {
	case MessageStoreFile:
		go s.receiveFile(peer, m, rpc.StreamSize)
	case MessageGetFileResponse:
		// Get reads the stream itself and calls CloseStream.
		if s.deliverGetResponse(rpc.From, getResponse{found: true, size: rpc.StreamSize}) {
			return
		}
		log.Printf("[%s] late response for (%s) from %s, discarding", s.Transport.Addr(), m.Key, rpc.From)
		go drainStream(peer, rpc.StreamSize)
	default:
		log.Printf("[%s] unexpected stream from %s, discarding", s.Transport.Addr(), rpc.From)
		go drainStream(peer, rpc.StreamSize)
	}
}

func drainStream(peer p2p.Peer, size int64) {
	io.CopyN(io.Discard, peer, size)
	peer.CloseStream()
}

// receiveFile stores an encrypted copy that a peer replicated to us.
func (s *FileServer) receiveFile(peer p2p.Peer, msg MessageStoreFile, size int64) {
	stream := &io.LimitedReader{R: peer, N: size}
	n, err := s.store.Write(msg.ID, msg.Key, stream)
	io.Copy(io.Discard, stream)
	peer.CloseStream()

	if err == nil && n != size {
		err = fmt.Errorf("got %d of %d bytes", n, size)
	}
	if err != nil {
		log.Printf("[%s] failed to store copy from %s: %v", s.Transport.Addr(), peer.RemoteAddr(), err)
		s.store.Remove(msg.ID, msg.Key)
		return
	}

	fmt.Printf("[%s] written %d bytes to disk\n", s.Transport.Addr(), n)
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	peer, ok := s.getPeer(from)
	if !ok {
		return fmt.Errorf("peer %s not in map", from)
	}

	notFound := func() error {
		payload, err := encodeMessage(&Message{Payload: MessageGetFileResponse{Key: msg.Key}})
		if err != nil {
			return err
		}
		return peer.SendMessage(payload)
	}

	if !s.store.Has(msg.ID, msg.Key) {
		fmt.Printf("[%s] asked for file (%s) but it does not exist on disk\n", s.Transport.Addr(), msg.Key)
		return notFound()
	}

	fileSize, r, err := s.store.Read(msg.ID, msg.Key)
	if err != nil {
		notFound()
		return err
	}
	defer r.Close()

	payload, err := encodeMessage(&Message{
		Payload: MessageGetFileResponse{Key: msg.Key, Found: true, Size: fileSize},
	})
	if err != nil {
		return err
	}

	fmt.Printf("[%s] serving file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	err = peer.SendMessageWithStream(payload, fileSize, func(w io.Writer) error {
		_, err := io.Copy(w, r)
		return err
	})
	if err != nil {
		return err
	}

	fmt.Printf("[%s] written (%d) bytes over the network to %s\n", s.Transport.Addr(), fileSize, from)

	return nil
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}

		go func(addr string) {
			// The remote node may not be listening yet, so retry with backoff.
			delay := 100 * time.Millisecond
			for attempt := 1; ; attempt++ {
				fmt.Printf("[%s] attemping to connect with remote %s\n", s.Transport.Addr(), addr)
				err := s.Transport.Dial(addr)
				if err == nil {
					return
				}
				if attempt == bootstrapAttempts {
					log.Printf("dial error: giving up on %s after %d attempts: %v", addr, attempt, err)
					return
				}
				time.Sleep(delay)
				delay *= 2
			}
		}(addr)
	}

	return nil
}

func (s *FileServer) Start() error {
	fmt.Printf("[%s] starting fileserver...\n", s.Transport.Addr())

	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()

	s.loop()

	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageGetFileResponse{})
}
