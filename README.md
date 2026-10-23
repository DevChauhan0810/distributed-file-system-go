# Distributed File System in Go

A peer-to-peer, content-addressable distributed file storage system written in Go. Every node can store files, copy them to its peers over TCP, and fetch files it doesn't have from the network. Replicas are encrypted with AES before they leave the node.

## Features

- **Peer-to-peer networking**: nodes talk over a custom TCP transport with no central server. A node can start with a list of bootstrap peers and connects to them on startup.
- **Content-addressable storage (CAS)**: file keys are hashed with SHA-1, and the hash is split into a nested directory path. Files are spread across many small directories, and the same key always maps to the same location.
- **Automatic replication**: when a node stores a file, it sends an encrypted copy to every connected peer.
- **Network fetch**: if a node is asked for a file it doesn't have on disk, it asks its peers, streams the file back, decrypts it and saves it locally.
- **Encryption**: copies sent to peers are encrypted with AES-256 in CTR mode. A random IV is attached to the front of each stream.
- **Pluggable transport**: networking sits behind a `Transport` interface, so TCP could be swapped for UDP, WebSockets or something else.

## How it works

### Storing a file

1. `FileServer.Store(key, data)` writes the file to the node's local disk.
2. The node sends a `MessageStoreFile` message to all peers. It contains the node's ID, the MD5 hash of the key, and the file size.
3. The file is encrypted and streamed to all peers at once.
4. Each peer writes the incoming bytes to its own disk.

### Getting a file

1. `FileServer.Get(key)` checks the local disk first.
2. If the file isn't there, the node sends a `MessageGetFile` message to its peers.
3. A peer that has the file sends back its size followed by the file bytes.
4. The requesting node decrypts the stream, saves the file locally and returns it.

### On-disk layout

With the `CASPathTransformFunc`, a key such as `picture_1.png` is hashed with SHA-1 and stored at a path like this:

```
<storage-root>/<node-id>/a1b2c/3d4e5/.../<full-sha1-hash>
```

Each node keeps its data in its own folder, named after its listen address (for example `:3000_network`).

### Wire protocol

Each frame sent over TCP starts with one byte that says what follows:

| Byte  | Meaning                                                    |
|-------|------------------------------------------------------------|
| `0x1` | `IncomingMessage`: a gob-encoded control message follows   |
| `0x2` | `IncomingStream`: raw file bytes follow                    |

During a stream the peer's read loop pauses and waits until the file transfer is finished. Then it goes back to reading messages.

## Project structure

```
.
├── main.go            # Demo: starts 3 nodes, stores and fetches files
├── server.go          # FileServer: peers, replication, message handling
├── store.go           # Content-addressable on-disk storage
├── crypto.go          # AES-CTR encryption, key and ID generation, hashing
├── store_test.go      # Storage tests
├── crypto_test.go     # Encryption tests
├── Makefile           # build / run / test shortcuts
└── p2p/
    ├── transport.go       # Transport and Peer interfaces
    ├── tcp_transport.go   # TCP implementation of the transport
    ├── message.go         # RPC type and stream/message flags
    ├── encoding.go        # Decoders for incoming data
    ├── handshake.go       # Handshake hook (currently a no-op)
    └── tcp_transport_test.go
```

## Getting started

### Requirements

- [Go](https://go.dev/dl/) 1.18 or newer

### Run the demo

```bash
git clone https://github.com/DevChauhan0810/distributed-file-system-go.git
cd distributed-file-system-go
go run .
```

Or, with `make`:

```bash
make run
```

The demo in `main.go` does the following:

1. Starts two nodes on `:3000` and `:7000`.
2. Starts a third node on `:5000`, which connects to the other two.
3. Stores 20 files from node `:5000`, so they are copied to both peers.
4. Deletes each file from node `:5000`'s local disk.
5. Calls `Get` for each file. Since the local copy is gone, node `:5000` fetches it back from the network and prints its contents.

### Run the tests

```bash
go test ./...
```

or `make test`.

## Limitations and ideas for future work

This is a learning project. Known limitations:

- The handshake does nothing, so peers aren't authenticated.
- Each node gets a new random encryption key at startup, and the key isn't saved anywhere.
- Some steps use fixed `time.Sleep` delays to wait for the network instead of proper acknowledgements.
- There is no peer discovery beyond the bootstrap list, and no handling for peers that drop out.

Possible next steps: authenticated handshakes, saved keys, a CLI or HTTP API, peer discovery, and smarter replication (for example, a set number of copies instead of copying to every peer).

## Acknowledgements

Based on the "foreverstore" distributed file storage project by [Anthony GG (anthdm)](https://github.com/anthdm).
