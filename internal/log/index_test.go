package log

import (
	"io"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		idx *index,
		c Config,
	){
		"read from empty index returns EOF":     testIndexReadEmpty,
		"write and read entries succeeds":       testIndexWriteRead,
		"read with -1 returns last entry":       testIndexReadLast,
		"read past end returns EOF":             testIndexReadPastEnd,
		"close truncates file to actual size":   testIndexClose,
		"reopen after close preserves entries":  testIndexReopenAfterClose,
		"write to full index returns EOF":       testIndexWriteFull,
	} {
		t.Run(scenario, func(t *testing.T) {
			idx, c, teardown := setupIndex(t)
			defer teardown()
			fn(t, idx, c)
		})
	}
}

func setupIndex(t *testing.T) (
	idx *index,
	c Config,
	teardown func(),
) {
	t.Helper()

	f, err := os.CreateTemp("", "index_test")
	if err != nil {
		t.Fatal(err)
	}

	c = Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err = newIndex(f, c)
	if err != nil {
		os.Remove(f.Name())
		t.Fatal(err)
	}

	return idx, c, func() {
		idx.Close()
		os.Remove(f.Name())
	}
}

func testIndexReadEmpty(t *testing.T, idx *index, c Config) {
	_, _, err := idx.Read(-1)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func testIndexWriteRead(t *testing.T, idx *index, c Config) {
	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
		{off: 2, pos: 20},
	}

	for _, want := range entries {
		err := idx.Write(want.off, want.pos)
		if err != nil {
			t.Fatalf("Write(%d, %d) failed: %v", want.off, want.pos, err)
		}

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
}

func testIndexReadLast(t *testing.T, idx *index, c Config) {
	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
		{off: 2, pos: 20},
	}

	for _, entry := range entries {
		err := idx.Write(entry.off, entry.pos)
		if err != nil {
			t.Fatal(err)
		}
	}

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
}

func testIndexReadPastEnd(t *testing.T, idx *index, c Config) {
	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
		{off: 2, pos: 20},
	}

	for _, entry := range entries {
		err := idx.Write(entry.off, entry.pos)
		if err != nil {
			t.Fatal(err)
		}
	}

	_, _, err := idx.Read(int64(len(entries)))
	if err != io.EOF {
		t.Fatalf("expected io.EOF when reading past end, got %v", err)
	}
}

func testIndexClose(t *testing.T, idx *index, c Config) {
	entries := []struct {
		off uint32
		pos uint64
	}{
		{off: 0, pos: 0},
		{off: 1, pos: 10},
		{off: 2, pos: 20},
	}

	for _, entry := range entries {
		err := idx.Write(entry.off, entry.pos)
		if err != nil {
			t.Fatal(err)
		}
	}

	name := idx.Name()

	err := idx.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	fi, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	expectedSize := int64(len(entries)) * int64(entWidth)
	if fi.Size() != expectedSize {
		t.Fatalf("expected file size %d after close, got %d", expectedSize, fi.Size())
	}
}

func testIndexReopenAfterClose(t *testing.T, idx *index, c Config) {
	err := idx.Write(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Write(1, 10)
	if err != nil {
		t.Fatal(err)
	}

	name := idx.Name()
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}

	idx, err = newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

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

	off, pos, err = idx.Read(-1)
	if err != nil {
		t.Fatalf("Read(-1) after reopen failed: %v", err)
	}
	if off != 1 || pos != 10 {
		t.Fatalf("expected (1, 10), got (%d, %d)", off, pos)
	}
}

func testIndexWriteFull(t *testing.T, idx *index, c Config) {
	// Close the default index and create one with small max size
	name := idx.Name()
	idx.Close()
	os.Remove(name)

	f, err := os.CreateTemp("", "index_full_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	c.Segment.MaxIndexBytes = uint64(entWidth) * 3

	idx, err = newIndex(f, c)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	for i := uint32(0); i < 3; i++ {
		err = idx.Write(i, uint64(i*10))
		if err != nil {
			t.Fatalf("Write(%d, %d) failed unexpectedly: %v", i, i*10, err)
		}
	}

	err = idx.Write(3, 30)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when index is full, got %v", err)
	}
}
