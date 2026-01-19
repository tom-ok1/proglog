package log

import (
	"io"
	"os"
	"testing"

	api "github.com/tom-ok1/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = uint64(entWidth) * 3

	s, err := newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	if s.nextOffset != 16 {
		t.Fatalf("expected nextOffset=16, got %d", s.nextOffset)
	}
	if s.IsMaxed() {
		t.Fatal("segment should not be maxed")
	}

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		if err != nil {
			t.Fatal(err)
		}
		if off != 16+i {
			t.Fatalf("expected offset=%d, got %d", 16+i, off)
		}

		got, err := s.Read(off)
		if err != nil {
			t.Fatal(err)
		}
		if string(got.Value) != string(want.Value) {
			t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
		}
	}

	// After 3 appends, the index should be maxed
	if !s.IsMaxed() {
		t.Fatal("segment should be maxed")
	}

	// Appending to a maxed segment should fail
	_, err = s.Append(want)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when segment is maxed, got %v", err)
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Test reopening existing segment
	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	// nextOffset should be restored after reopening
	if s.nextOffset != 19 {
		t.Fatalf("expected nextOffset=19 after reopen, got %d", s.nextOffset)
	}

	if !s.IsMaxed() {
		t.Fatal("segment should be maxed after reopen")
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSegmentRemove(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment_remove_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	s, err := newSegment(dir, 0, c)
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Append(&api.Record{Value: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	storeName := s.store.Name()
	indexName := s.index.Name()

	// Verify files exist
	if _, err := os.Stat(storeName); os.IsNotExist(err) {
		t.Fatal("store file should exist")
	}
	if _, err := os.Stat(indexName); os.IsNotExist(err) {
		t.Fatal("index file should exist")
	}

	err = s.Remove()
	if err != nil {
		t.Fatal(err)
	}

	// Verify files are removed
	if _, err := os.Stat(storeName); !os.IsNotExist(err) {
		t.Fatal("store file should be removed")
	}
	if _, err := os.Stat(indexName); !os.IsNotExist(err) {
		t.Fatal("index file should be removed")
	}
}

func TestSegmentReadOutOfRange(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment_read_range_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	s, err := newSegment(dir, 0, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Reading from empty segment should fail
	_, err = s.Read(0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when reading from empty segment, got %v", err)
	}

	// Append a record
	off, err := s.Append(&api.Record{Value: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	// Reading beyond the last offset should fail
	_, err = s.Read(off + 1)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when reading past end, got %v", err)
	}
}

func TestSegmentStoreMaxed(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment_store_maxed_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	// Set store size small enough that it will be maxed quickly
	c.Segment.MaxStoreBytes = 50
	c.Segment.MaxIndexBytes = 1024

	s, err := newSegment(dir, 0, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Append records until store is maxed
	record := &api.Record{Value: []byte("hello world")}
	var count int
	for !s.IsMaxed() {
		_, err := s.Append(record)
		if err != nil {
			break
		}
		count++
		if count > 100 {
			t.Fatal("too many iterations, store should be maxed")
		}
	}

	if !s.IsMaxed() {
		t.Fatal("segment should be maxed by store size")
	}
}

func TestSegmentWithNonZeroBaseOffset(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment_base_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	baseOffset := uint64(100)
	s, err := newSegment(dir, baseOffset, c)
	if err != nil {
		t.Fatal(err)
	}

	if s.baseOffset != baseOffset {
		t.Fatalf("expected baseOffset=%d, got %d", baseOffset, s.baseOffset)
	}
	if s.nextOffset != baseOffset {
		t.Fatalf("expected nextOffset=%d, got %d", baseOffset, s.nextOffset)
	}

	// Append records and verify offsets
	for i := uint64(0); i < 5; i++ {
		off, err := s.Append(&api.Record{Value: []byte("test")})
		if err != nil {
			t.Fatal(err)
		}
		expectedOff := baseOffset + i
		if off != expectedOff {
			t.Fatalf("expected offset=%d, got %d", expectedOff, off)
		}

		// Verify we can read it back
		record, err := s.Read(off)
		if err != nil {
			t.Fatal(err)
		}
		if string(record.Value) != "test" {
			t.Fatalf("expected value=test, got %s", record.Value)
		}
		if record.Offset != expectedOff {
			t.Fatalf("expected record.Offset=%d, got %d", expectedOff, record.Offset)
		}
	}

	err = s.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen and verify state
	s, err = newSegment(dir, baseOffset, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.nextOffset != baseOffset+5 {
		t.Fatalf("expected nextOffset=%d after reopen, got %d", baseOffset+5, s.nextOffset)
	}
}
