package log

import (
	"os"
	"testing"
)

var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

func TestStore(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		s *store,
	){
		"append and read succeeds":   testStoreAppendRead,
		"read at specific position":  testStoreReadAt,
		"close flushes data to disk": testStoreClose,
		"reopen preserves data":      testStoreReopen,
	} {
		t.Run(scenario, func(t *testing.T) {
			s, teardown := setupStore(t)
			defer teardown()
			fn(t, s)
		})
	}
}

func setupStore(t *testing.T) (
	s *store,
	teardown func(),
) {
	t.Helper()

	f, err := os.CreateTemp("", "store_test")
	if err != nil {
		t.Fatal(err)
	}

	s, err = newStore(f)
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}

	return s, func() {
		s.Close()
		os.Remove(f.Name())
	}
}

func testStoreAppendRead(t *testing.T, s *store) {
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		if err != nil {
			t.Fatal(err)
		}
		if pos+n != width*i {
			t.Fatalf("expected pos+n=%d, got %d", width*i, pos+n)
		}
	}

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

func testStoreReadAt(t *testing.T, s *store) {
	for i := uint64(1); i < 4; i++ {
		_, _, err := s.Append(write)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		if err != nil {
			t.Fatal(err)
		}
		if n != lenWidth {
			t.Fatalf("expected %d bytes, got %d", lenWidth, n)
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

func testStoreClose(t *testing.T, s *store) {
	_, _, err := s.Append(write)
	if err != nil {
		t.Fatal(err)
	}

	name := s.Name()
	_, beforeSize, err := openFile(name)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	_, afterSize, err := openFile(name)
	if err != nil {
		t.Fatal(err)
	}

	if afterSize <= beforeSize {
		t.Fatalf("expected afterSize > beforeSize, got afterSize=%d, beforeSize=%d", afterSize, beforeSize)
	}
}

func testStoreReopen(t *testing.T, s *store) {
	for i := uint64(1); i < 4; i++ {
		_, _, err := s.Append(write)
		if err != nil {
			t.Fatal(err)
		}
	}

	name := s.Name()
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, _, err := openFile(name)
	if err != nil {
		t.Fatal(err)
	}

	s, err = newStore(f)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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
