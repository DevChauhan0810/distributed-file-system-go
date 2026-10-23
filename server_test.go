package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/anthdm/foreverstore/p2p"
)

func newTestServer(t *testing.T, listenAddr string, nodes ...string) *FileServer {
	tr := p2p.NewTCPTransport(p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	})
	s := NewFileServer(FileServerOpts{
		EncKey:            newEncryptionKey(),
		StorageRoot:       t.TempDir(),
		PathTransformFunc: CASPathTransformFunc,
		Transport:         tr,
		BootstrapNodes:    nodes,
	})
	tr.OnPeer = s.OnPeer
	return s
}

// startNetwork starts two storage nodes and a client node connected to both.
func startNetwork(t *testing.T, basePort int) (client, a, b *FileServer) {
	addrA := fmt.Sprintf(":%d", basePort)
	addrB := fmt.Sprintf(":%d", basePort+1)
	a = newTestServer(t, addrA)
	b = newTestServer(t, addrB)
	client = newTestServer(t, fmt.Sprintf(":%d", basePort+2), addrA, addrB)

	for _, s := range []*FileServer{a, b, client} {
		go s.Start()
		t.Cleanup(s.Stop)
	}

	waitFor(t, func() bool {
		return len(client.peerList()) == 2 && len(a.peerList()) == 1 && len(b.peerList()) == 1
	})
	return client, a, b
}

// A node started before its bootstrap peers are listening must still connect.
func TestClientStartsBeforePeers(t *testing.T) {
	addrA, addrB := ":4150", ":4151"
	client := newTestServer(t, ":4152", addrA, addrB)
	go client.Start()
	t.Cleanup(client.Stop)

	time.Sleep(300 * time.Millisecond)
	for _, addr := range []string{addrA, addrB} {
		s := newTestServer(t, addr)
		go s.Start()
		t.Cleanup(s.Stop)
	}

	waitFor(t, func() bool { return len(client.peerList()) == 2 })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func getString(t *testing.T, s *FileServer, key string) (string, error) {
	t.Helper()
	r, err := s.Get(key)
	if err != nil {
		return "", err
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	return string(b), err
}

func hasReplica(s *FileServer, owner *FileServer, key string) bool {
	return s.store.Has(owner.ID, hashKey(key))
}

func TestStoreReplicatesAndGetRecovers(t *testing.T) {
	client, a, b := startNetwork(t, 4100)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file_%d", i)
		data := strings.Repeat(fmt.Sprintf("data %d ", i), 1000)

		if err := client.Store(key, strings.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return hasReplica(a, client, key) && hasReplica(b, client, key) })

		if err := client.store.Delete(client.ID, key); err != nil {
			t.Fatal(err)
		}

		got, err := getString(t, client, key)
		if err != nil {
			t.Fatal(err)
		}
		if got != data {
			t.Fatalf("%s: got %d bytes back, want %d", key, len(got), len(data))
		}
	}
}

// A peer that does not have the file must not make Get hang or fail, the next
// peer should be asked instead.
func TestGetFallsBackWhenPeerIsMissingFile(t *testing.T) {
	client, a, b := startNetwork(t, 4110)

	// Remove the copy from a different peer each time, so both orders are tested.
	for i, missing := range []*FileServer{a, b, a, b} {
		key := fmt.Sprintf("file_%d", i)
		if err := client.Store(key, strings.NewReader("hello")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return hasReplica(a, client, key) && hasReplica(b, client, key) })

		if err := missing.store.Remove(client.ID, hashKey(key)); err != nil {
			t.Fatal(err)
		}
		client.store.Delete(client.ID, key)

		got, err := getString(t, client, key)
		if err != nil {
			t.Fatal(err)
		}
		if got != "hello" {
			t.Fatalf("got %q", got)
		}
	}
}

func TestGetReturnsErrorWhenNoPeerHasFile(t *testing.T) {
	client, _, _ := startNetwork(t, 4120)

	done := make(chan error, 1)
	go func() {
		_, err := client.Get("does-not-exist")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Get hung instead of returning an error")
	}
}

// A peer that modifies its copy must be detected, and the good copy from the
// other peer used instead. The tampered data must never be returned.
func TestGetRejectsTamperedCopy(t *testing.T) {
	client, a, b := startNetwork(t, 4130)

	for i, bad := range []*FileServer{a, b} {
		key := fmt.Sprintf("file_%d", i)
		if err := client.Store(key, strings.NewReader("important data")); err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return hasReplica(a, client, key) && hasReplica(b, client, key) })

		corrupt(t, bad, client, key)
		client.store.Delete(client.ID, key)

		got, err := getString(t, client, key)
		if err != nil {
			t.Fatal(err)
		}
		if got != "important data" {
			t.Fatalf("got %q", got)
		}
	}

	// With both copies corrupted there is nothing valid to return.
	key := "file_both"
	client.Store(key, strings.NewReader("important data"))
	waitFor(t, func() bool { return hasReplica(a, client, key) && hasReplica(b, client, key) })
	corrupt(t, a, client, key)
	corrupt(t, b, client, key)
	client.store.Delete(client.ID, key)

	if _, err := getString(t, client, key); err == nil {
		t.Fatal("expected an error when every copy is tampered with")
	}
	if client.store.Has(client.ID, key) {
		t.Fatal("tampered data must not be saved")
	}
}

// corrupt flips one byte in the copy of key that peer stores for owner.
func corrupt(t *testing.T, peer, owner *FileServer, key string) {
	t.Helper()
	path := fmt.Sprintf("%s/%s/%s", peer.store.Root, owner.ID, CASPathTransformFunc(hashKey(key)).FullPath())
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// Many back-to-back transfers without any sleeps must never mix up frames.
func TestBackToBackTransfersStayInSync(t *testing.T) {
	client, a, b := startNetwork(t, 4140)

	var keys []string
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("file_%d", i)
		data := bytes.Repeat([]byte{byte(i)}, 1000+i*997)
		if err := client.Store(key, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	waitFor(t, func() bool {
		for _, key := range keys {
			if !hasReplica(a, client, key) || !hasReplica(b, client, key) {
				return false
			}
		}
		return true
	})

	for i, key := range keys {
		client.store.Delete(client.ID, key)
		got, err := getString(t, client, key)
		if err != nil {
			t.Fatal(err)
		}
		want := string(bytes.Repeat([]byte{byte(i)}, 1000+i*997))
		if got != want {
			t.Fatalf("%s: wrong contents", key)
		}
	}
}
