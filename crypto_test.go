package main

import (
	"bytes"
	"errors"
	"testing"
)

func TestCopyEncryptDecrypt(t *testing.T) {
	payload := "Foo not bar"
	src := bytes.NewReader([]byte(payload))
	dst := new(bytes.Buffer)
	key := newEncryptionKey()

	nw, err := copyEncrypt(key, src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if nw != encryptedSize(int64(len(payload))) || int64(dst.Len()) != nw {
		t.Fatalf("encrypted size: returned %d, wrote %d, want %d", nw, dst.Len(), encryptedSize(int64(len(payload))))
	}

	out := new(bytes.Buffer)
	n, err := copyDecrypt(key, dst, out, nw)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) {
		t.Errorf("decrypted %d bytes, want %d", n, len(payload))
	}
	if out.String() != payload {
		t.Errorf("decryption failed!!!")
	}
}

func TestDecryptDetectsTampering(t *testing.T) {
	key := newEncryptionKey()
	enc := new(bytes.Buffer)
	if _, err := copyEncrypt(key, bytes.NewReader([]byte("some secret file contents")), enc); err != nil {
		t.Fatal(err)
	}

	// Flip one bit in the IV, the ciphertext and the tag.
	for _, pos := range []int{0, ivSize + 3, enc.Len() - 1} {
		tampered := append([]byte(nil), enc.Bytes()...)
		tampered[pos] ^= 0x01

		_, err := copyDecrypt(key, bytes.NewReader(tampered), new(bytes.Buffer), int64(len(tampered)))
		if !errors.Is(err, ErrAuthFailed) {
			t.Errorf("byte %d flipped: want ErrAuthFailed, got %v", pos, err)
		}
	}

	_, err := copyDecrypt(newEncryptionKey(), bytes.NewReader(enc.Bytes()), new(bytes.Buffer), int64(enc.Len()))
	if !errors.Is(err, ErrAuthFailed) {
		t.Errorf("wrong key: want ErrAuthFailed, got %v", err)
	}
}

func TestDecryptRejectsTruncatedData(t *testing.T) {
	key := newEncryptionKey()
	enc := new(bytes.Buffer)
	if _, err := copyEncrypt(key, bytes.NewReader([]byte("some secret file contents")), enc); err != nil {
		t.Fatal(err)
	}

	short := enc.Bytes()[:enc.Len()-5]
	if _, err := copyDecrypt(key, bytes.NewReader(short), new(bytes.Buffer), int64(enc.Len())); err == nil {
		t.Error("expected an error for truncated data")
	}
}
