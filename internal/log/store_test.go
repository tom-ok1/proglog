package log

import (
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenUint64
)

func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Fatal(err)
	}

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	if err != nil {
		t.Fatal(err)
	}
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Fatal(err)
		}
		if pos+n != width*i {
			t.Fatalf("expected pos+n=%d, got %d", width*i, pos+n)
		}
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		if err != nil {
			t.Fatal(err)
		}
		if string(read) != string(write) {
			t.Fatalf("expected %s, got %s", write, read)
		}
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenUint64)
		n, err := s.ReadAt(b, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != lenUint64 {
			t.Fatalf("expected %d bytes, got %d", lenUint64, n)
		}
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != string(write) {
			t.Fatalf("expected %s, got %s", write, b)
		}
		if n != int(size) {
			t.Fatalf("expected %d bytes, got %d", size, n)
		}
		off += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	s, err := newStore(f)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = s.Append(write)
	if err != nil {
		t.Fatal(err)
	}

	f, beforeSize, err := openFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, afterSize, err := openFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}

	if afterSize <= beforeSize {
		t.Fatalf("expected afterSize > beforeSize, got afterSize=%d, beforeSize=%d", afterSize, beforeSize)
	}
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
