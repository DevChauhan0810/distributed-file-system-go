package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// Encrypted stream layout (encrypt-then-MAC):
//
//	[16-byte IV][AES-256-CTR ciphertext][32-byte HMAC-SHA256 of IV+ciphertext]
const (
	ivSize   = aes.BlockSize
	tagSize  = sha256.Size
	overhead = ivSize + tagSize

	copyBufferSize = 32 * 1024
)

// ErrAuthFailed means the encrypted data was modified or the wrong key was used.
var ErrAuthFailed = errors.New("authentication failed: data was tampered with or the key is wrong")

func generateID() string {
	buf := make([]byte, 32)
	io.ReadFull(rand.Reader, buf)
	return hex.EncodeToString(buf)
}

func hashKey(key string) string {
	hash := md5.Sum([]byte(key))
	return hex.EncodeToString(hash[:])
}

func newEncryptionKey() []byte {
	keyBuf := make([]byte, 32)
	io.ReadFull(rand.Reader, keyBuf)
	return keyBuf
}

// encryptedSize returns the size of the encrypted stream for a plaintext of n bytes.
func encryptedSize(n int64) int64 {
	return n + overhead
}

// deriveKeys derives separate encryption and MAC keys from the master key, so
// the same key is never used for two different purposes.
func deriveKeys(key []byte) (encKey, macKey []byte) {
	derive := func(label string) []byte {
		m := hmac.New(sha256.New, key)
		m.Write([]byte(label))
		return m.Sum(nil)
	}
	return derive("foreverstore encryption"), derive("foreverstore mac")
}

// copyStream XORs src with the cipher stream into dst through a fixed 32KB
// buffer, feeding the ciphertext side to mac. Memory use does not depend on
// the size of src.
func copyStream(stream cipher.Stream, mac io.Writer, encrypt bool, src io.Reader, dst io.Writer) (int64, error) {
	var (
		buf = make([]byte, copyBufferSize)
		nw  int64
	)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if !encrypt {
				mac.Write(chunk)
			}
			stream.XORKeyStream(chunk, chunk)
			if encrypt {
				mac.Write(chunk)
			}
			nn, werr := dst.Write(chunk)
			nw += int64(nn)
			if werr != nil {
				return nw, werr
			}
		}
		if err == io.EOF {
			return nw, nil
		}
		if err != nil {
			return nw, err
		}
	}
}

// copyEncrypt encrypts src into dst and returns the number of bytes written
// to dst, which is encryptedSize(len(src)).
func copyEncrypt(key []byte, src io.Reader, dst io.Writer) (int64, error) {
	encKey, macKey := deriveKeys(key)
	block, err := aes.NewCipher(encKey)
	if err != nil {
		return 0, err
	}

	iv := make([]byte, ivSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return 0, err
	}
	if _, err := dst.Write(iv); err != nil {
		return 0, err
	}

	mac := hmac.New(sha256.New, macKey)
	mac.Write(iv)

	n, err := copyStream(cipher.NewCTR(block, iv), mac, true, src, dst)
	if err != nil {
		return 0, err
	}

	if _, err := dst.Write(mac.Sum(nil)); err != nil {
		return 0, err
	}

	return n + overhead, nil
}

// copyDecrypt decrypts exactly size encrypted bytes from src into dst and
// returns the number of plaintext bytes written. It returns ErrAuthFailed if
// the data was modified. Plaintext is written before the tag can be checked,
// so callers must discard dst when an error is returned.
func copyDecrypt(key []byte, src io.Reader, dst io.Writer, size int64) (int64, error) {
	if size < overhead {
		return 0, fmt.Errorf("encrypted data too short: %d bytes", size)
	}

	encKey, macKey := deriveKeys(key)
	block, err := aes.NewCipher(encKey)
	if err != nil {
		return 0, err
	}

	iv := make([]byte, ivSize)
	if _, err := io.ReadFull(src, iv); err != nil {
		return 0, err
	}

	mac := hmac.New(sha256.New, macKey)
	mac.Write(iv)

	ciphertextLen := size - overhead
	n, err := copyStream(cipher.NewCTR(block, iv), mac, false, io.LimitReader(src, ciphertextLen), dst)
	if err != nil {
		return n, err
	}
	if n != ciphertextLen {
		return n, io.ErrUnexpectedEOF
	}

	tag := make([]byte, tagSize)
	if _, err := io.ReadFull(src, tag); err != nil {
		return n, err
	}
	if !hmac.Equal(tag, mac.Sum(nil)) {
		return n, ErrAuthFailed
	}

	return n, nil
}
