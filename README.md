# Distributed File System in Go

A peer-to-peer, content-addressable distributed file storage system written in Go. Every node can store files, copy them to its peers over TCP, and fetch files it doesn't have from the network. Replicas are encrypted and authenticated before they leave the node, and files of any size are streamed with constant memory use.

## Features

- **Peer-to-peer networking**: nodes talk over a custom TCP transport with no central server. A node can start with a list of bootstrap peers and connects to them on startup, retrying with backoff if they aren't up yet.
- **Content-addressable storage (CAS)**: file keys are hashed with SHA-1, and the hash is split into a nested directory path. Files are spread across many small directories, and the same key always maps to the same location.
- **Automatic replication**: when a node stores a file, it sends an encrypted copy to every connected peer.
- **Fault-tolerant fetch**: if a node doesn't have a file on disk, it asks its peers **one at a time**. If a peer doesn't have the file, doesn't answer within 5 seconds, or sends a copy that fails verification, the next peer is tried. Only one peer ever sends the file.
- **Authenticated encryption**: copies sent to peers are encrypted with AES-256-CTR and protected with an HMAC-SHA256 tag (encrypt-then-MAC). Peers can't read the files, and if a peer changes even one byte, the change is detected and the copy is rejected.
- **Constant memory, any file size**: files are streamed from disk through a fixed 32 KB buffer and never loaded into memory. A 5 GB file uses the same ~11 MB of RAM as a 1 KB file (see [Performance](#performance)).
- **Pluggable transport**: networking sits behind a `Transport` interface, so TCP could be swapped for UDP, WebSockets or something else.

## How it works

### Storing a file

1. `FileServer.Store(key, data)` writes the file to the node's local disk.
2. For each peer, the node sends a `MessageStoreFile` message (the node's ID, the MD5 hash of the key and the encrypted size), immediately followed by the encrypted file. The file is streamed straight from disk.
3. Each peer writes the incoming bytes to a temporary file and renames it into place when complete, so a half-received copy never shows up as a real file.

### Getting a file

1. `FileServer.Get(key)` checks the local disk first.
2. If the file isn't there, the node sends a `MessageGetFile` request to **one** peer and waits for its answer.
3. The peer replies with `MessageGetFileResponse`. If `Found` is false, the node moves on to the next peer. If `Found` is true, the file stream follows.
4. The node decrypts the stream into a temporary file while checking the HMAC tag. Only if the tag is valid is the file moved into place and returned. Otherwise it's deleted and the next peer is tried.
5. If no peer has a valid copy, `Get` returns an error instead of hanging.

### Encryption format

```
[16-byte IV][AES-256-CTR ciphertext][32-byte HMAC-SHA256 over IV + ciphertext]
```

Separate encryption and MAC keys are derived from the node's 32-byte master key. Each encrypted copy is 48 bytes larger than the original.

### On-disk layout

With the `CASPathTransformFunc`, a key such as `picture_1.png` is hashed with SHA-1 and stored at a path like this:

```
<storage-root>/<node-id>/a1b2c/3d4e5/.../<full-sha1-hash>
```

Each node keeps its data in its own folder, named after its listen port (for example `3000_network`).

### Wire protocol

Every frame on a TCP connection starts with a one-byte type tag and a fixed-size length header, so the receiver always knows exactly where a frame ends:

| Frame | Layout | Meaning |
|---|---|---|
| Message | `[0x1][uint32 length][payload]` | A gob-encoded control message (max 1 MB) |
| Stream | `[0x2][int64 length][raw bytes]` | File data, read directly from the connection |

Control messages and file streams share one TCP connection per peer. Two rules keep them from ever getting mixed up:

- **One reader at a time.** When the read loop sees a stream frame, it hands the stream to the code that is waiting for it and pauses on a `sync.WaitGroup` until exactly `length` bytes have been read. Then it resumes reading frames. The read loop and the stream consumer never read the connection at the same time.
- **One writer at a time.** Each peer has a send lock, so frames from different goroutines are never interleaved. A message and the stream it announces are sent under the same lock, so the stream always comes right after its message. If a stream ends up shorter or longer than announced, the connection is closed rather than left out of sync.

No `time.Sleep` calls are used to coordinate transfers.

## Project structure

```
.
├── main.go            # Demo: starts 3 nodes, stores and fetches files
├── server.go          # FileServer: peers, replication, fetch with fallback
├── store.go           # Content-addressable on-disk storage
├── crypto.go          # AES-256-CTR + HMAC-SHA256 streaming encryption
├── server_test.go     # End-to-end tests with 3 real nodes over TCP
├── store_test.go      # Storage tests
├── crypto_test.go     # Encryption and tamper-detection tests
├── Makefile           # build / run / test shortcuts
└── p2p/
    ├── transport.go       # Transport and Peer interfaces
    ├── tcp_transport.go   # TCP transport, framing, read loop
    ├── message.go         # Frame types and RPC struct
    ├── encoding.go        # Frame decoder
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
5. Calls `Get` for each file. Since the local copy is gone, node `:5000` fetches it back from one of its peers and prints its contents.

Each of the 20 rounds runs the full cycle:

> **store → replicate to peers (encrypted) → lose the local copy → recover it from the network → data comes back unchanged**

The peers only ever hold encrypted copies and can't read them, because only node `:5000` has the key. The stored copies are 70 bytes: the 22-byte message plus the 16-byte IV and the 32-byte HMAC tag.

#### What you'll see

One round of output (`picture_0.png`):

```
[:5000] stored (picture_0.png): 22 bytes on local disk, encrypted copy sent to 2/2 peers
[:7000] written 70 bytes to disk                       # peer saves encrypted copy
[:3000] written 70 bytes to disk                       # peer saves encrypted copy
[:5000] dont have file (picture_0.png) locally, fetching from network...
[:7000] serving file (44b2a6b69cffe1ea7922da80d75577e7) over the network
[:7000] written (70) bytes over the network to 127.0.0.1:49679
[:5000] received (22) bytes over the network from (127.0.0.1:7000)
my big data file here!                                 # verified, decrypted and intact
```

Only one peer serves each file. If `my big data file here!` is printed 20 times, everything worked.

### Run the tests

```bash
go test ./...
```

or `make test`. The tests start real nodes on local ports 4100 to 4152 and cover:

- store → delete → fetch round trips
- fetching when the first peer asked doesn't have the file, in both peer orders
- `Get` returning an error, not hanging, when no peer has the file
- tampered copies being rejected and the good copy used instead, and an error when every copy is tampered with
- 50 back-to-back transfers of different sizes with no sleeps, checking frames never get mixed up
- a node starting before its bootstrap peers
- framing, size-mismatch and oversized-message handling in the transport
- encryption round trips, tamper detection, wrong keys and truncated data

## Performance

Measured on a Windows laptop with all three nodes on one machine. Each file is stored, deleted locally, fetched back from the network and checked against its SHA-256 hash.

| File size | Result | Peak RAM | Store | Fetch |
|---|---|---|---|---|
| 1 GB | identical | 11 MB | 5.9 s | 3.3 s |
| 3 GB | identical | 11 MB | 26.7 s | 11.8 s |
| 5 GB | identical | 11 MB | 71.3 s | 47.2 s |

Before streaming from disk, `Store` buffered the whole file in memory. Peak RAM was about 2× the file size, and a 3 GB file ran out of memory.

## Limitations and ideas for future work

Known limitations:

- **The encryption key lives only in the storing node's memory.** It's random at startup and never saved, so if that node restarts, its replicas can't be decrypted. This makes the storing node a single point of failure for its own files.
- The handshake does nothing, so peers aren't authenticated. A malicious peer can't read or silently modify files, but it can refuse to serve them.
- `Store` doesn't wait for peers to confirm they saved their copy.
- Each file is copied to every connected peer, one peer at a time.
- `Delete` removes the file's whole top-level directory, which could also remove other files whose hashes share the same first 5 characters.
- There is no peer discovery beyond the bootstrap list, and disconnected peers aren't removed from the peer list (requests to them fail and the next peer is tried).

Possible next steps: persistent, shareable keys, authenticated handshakes, store acknowledgements, a fixed replication factor, peer discovery, and a CLI or HTTP API.

## Acknowledgements

Based on the "foreverstore" distributed file storage project by [Anthony GG (anthdm)](https://github.com/anthdm). Changes made on top of the original:

- Streaming `Store` from disk, for constant memory use at any file size (previously buffered whole files in RAM)
- Length-prefixed framing, a per-peer send lock and stream handoff, replacing `time.Sleep`-based coordination
- Fetching from one peer at a time with fallback, not-found responses and timeouts (previously every peer sent the file, and `Get` could hang)
- HMAC-SHA256 authentication of encrypted copies, and verify-before-save
- Atomic writes (temporary file + rename), bootstrap retry with backoff
- Windows support (path names, file handle leaks)
- End-to-end, framing and tamper-detection tests
