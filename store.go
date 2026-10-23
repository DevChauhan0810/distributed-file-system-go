package main

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

const defaultRootFolderName = "ggnetwork"

func CASPathTransformFunc(key string) PathKey {
	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	sliceLen := len(hashStr) / blocksize
	paths := make([]string, sliceLen)

	for i := 0; i < sliceLen; i++ {
		from, to := i*blocksize, (i*blocksize)+blocksize
		paths[i] = hashStr[from:to]
	}

	return PathKey{
		PathName: strings.Join(paths, "/"),
		Filename: hashStr,
	}
}

type PathTransformFunc func(string) PathKey

type PathKey struct {
	PathName string
	Filename string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.PathName, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullPath() string {
	return fmt.Sprintf("%s/%s", p.PathName, p.Filename)
}

type StoreOpts struct {
	// Root is the folder name of the root, containing all the folders/files of the system.
	Root              string
	PathTransformFunc PathTransformFunc
}

var DefaultPathTransformFunc = func(key string) PathKey {
	return PathKey{
		PathName: key,
		Filename: key,
	}
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	if opts.PathTransformFunc == nil {
		opts.PathTransformFunc = DefaultPathTransformFunc
	}
	if len(opts.Root) == 0 {
		opts.Root = defaultRootFolderName
	}

	return &Store{
		StoreOpts: opts,
	}
}

func (s *Store) Has(id string, key string) bool {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	_, err := os.Stat(fullPathWithRoot)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Clear() error {
	return os.RemoveAll(s.Root)
}

func (s *Store) Delete(id string, key string) error {
	pathKey := s.PathTransformFunc(key)

	defer func() {
		log.Printf("deleted [%s] from disk", pathKey.Filename)
	}()

	firstPathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FirstPathName())

	return os.RemoveAll(firstPathNameWithRoot)
}

// Remove deletes only the file for key, leaving its directories in place.
func (s *Store) Remove(id string, key string) error {
	pathKey := s.PathTransformFunc(key)
	return os.Remove(fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath()))
}

func (s *Store) Write(id string, key string, r io.Reader) (int64, error) {
	return s.writeStream(id, key, r)
}

// WriteDecrypt decrypts size encrypted bytes from r and stores the plaintext.
// The data is written to a temporary file that only replaces the real file
// once the authentication tag has been verified, so tampered or incomplete
// data never ends up in the store.
func (s *Store) WriteDecrypt(encKey []byte, id string, key string, r io.Reader, size int64) (int64, error) {
	fullPath, err := s.prepareDir(id, key)
	if err != nil {
		return 0, err
	}
	tmpPath := fullPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}
	n, err := copyDecrypt(encKey, r, f, size)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpPath)
		return 0, err
	}

	return n, os.Rename(tmpPath, fullPath)
}

// prepareDir creates the directories for key and returns the full file path.
func (s *Store) prepareDir(id string, key string) (string, error) {
	pathKey := s.PathTransformFunc(key)
	pathNameWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.PathName)
	if err := os.MkdirAll(pathNameWithRoot, os.ModePerm); err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath()), nil
}

// writeStream writes to a temporary file and renames it once complete, so the
// file never appears in the store half-written.
func (s *Store) writeStream(id string, key string, r io.Reader) (int64, error) {
	fullPath, err := s.prepareDir(id, key)
	if err != nil {
		return 0, err
	}
	tmpPath := fullPath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, r)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpPath)
		return 0, err
	}

	return n, os.Rename(tmpPath, fullPath)
}

// Read opens the file for key. The caller must close the returned reader.
func (s *Store) Read(id string, key string) (int64, io.ReadCloser, error) {
	return s.readStream(id, key)
}

func (s *Store) readStream(id string, key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	fullPathWithRoot := fmt.Sprintf("%s/%s/%s", s.Root, id, pathKey.FullPath())

	file, err := os.Open(fullPathWithRoot)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, nil, err
	}

	return fi.Size(), file, nil
}
