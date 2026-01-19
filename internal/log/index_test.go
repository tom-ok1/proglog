package log

import (
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp("", "index_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	// Test reading from empty index returns EOF
	_, _, err = idx.Read(-1)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	// Test file was truncated to MaxIndexBytes
	fi, err := os.Stat(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != int64(c.Segment.MaxIndexBytes) {
		t.Fatalf("expected file size %d, got %d", c.Segment.MaxIndexBytes, fi.Size())
	}

	// Test writing and reading entries
	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
		{off: 2, pos: 20},
	}

	for _, want := range entries {
		err = idx.Write(want.off, want.pos)
		if err != nil {
			t.Fatalf("Write(%d, %d) failed: %v", want.off, want.pos, err)
		}

		// Read the entry we just wrote using the offset as the index
		off, pos, err := idx.Read(int64(want.off))
		if err != nil {
			t.Fatalf("Read(%d) failed: %v", want.off, err)
		}
		if off != want.off {
			t.Fatalf("expected off=%d, got %d", want.off, off)
		}
		if pos != want.pos {
			t.Fatalf("expected pos=%d, got %d", want.pos, pos)
		}
	}

	// Test reading with -1 returns last entry
	off, pos, err := idx.Read(-1)
	if err != nil {
		t.Fatalf("Read(-1) failed: %v", err)
	}
	lastEntry := entries[len(entries)-1]
	if off != lastEntry.off {
		t.Fatalf("expected off=%d, got %d", lastEntry.off, off)
	}
	if pos != lastEntry.pos {
		t.Fatalf("expected pos=%d, got %d", lastEntry.pos, pos)
	}

	// Test reading past the end returns EOF
	_, _, err = idx.Read(int64(len(entries)))
	if err != io.EOF {
		t.Fatalf("expected io.EOF when reading past end, got %v", err)
	}

	// Test Close
	err = idx.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	// Verify file was truncated to actual size after close
	fi, err = os.Stat(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := int64(len(entries)) * int64(entWidth)
	if fi.Size() != expectedSize {
		t.Fatalf("expected file size %d after close, got %d", expectedSize, fi.Size())
	}
}

func TestIndexReopenAfterClose(t *testing.T) {
	f, err := os.CreateTemp("", "index_reopen_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	// Write some entries
	err = idx.Write(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Write(1, 10)
	if err != nil {
		t.Fatal(err)
	}

	// Close the index
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen the file and create a new index
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	idx, err = newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	// Verify we can read the previously written entries
	off, pos, err := idx.Read(0)
	if err != nil {
		t.Fatalf("Read(0) after reopen failed: %v", err)
	}
	if off != 0 || pos != 0 {
		t.Fatalf("expected (0, 0), got (%d, %d)", off, pos)
	}

	off, pos, err = idx.Read(1)
	if err != nil {
		t.Fatalf("Read(1) after reopen failed: %v", err)
	}
	if off != 1 || pos != 10 {
		t.Fatalf("expected (1, 10), got (%d, %d)", off, pos)
	}

	// Verify Read(-1) returns the last entry
	off, pos, err = idx.Read(-1)
	if err != nil {
		t.Fatalf("Read(-1) after reopen failed: %v", err)
	}
	if off != 1 || pos != 10 {
		t.Fatalf("expected (1, 10), got (%d, %d)", off, pos)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexWriteFull(t *testing.T) {
	f, err := os.CreateTemp("", "index_full_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	c := Config{}
	// Set max size to only fit 3 entries (each entry is 12 bytes)
	c.Segment.MaxIndexBytes = uint64(entWidth) * 3

	idx, err := newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}

	// Write 3 entries (should succeed)
	for i := uint32(0); i < 3; i++ {
		err = idx.Write(i, uint64(i*10))
		if err != nil {
			t.Fatalf("Write(%d, %d) failed unexpectedly: %v", i, i*10, err)
		}
	}

	// Try to write 4th entry (should fail with EOF)
	err = idx.Write(3, 30)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when index is full, got %v", err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}
