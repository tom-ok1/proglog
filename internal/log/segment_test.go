package log

import (
	"io"
	"os"
	"testing"

	api "github.com/tom-ok1/proglog/api/v1"
)

func TestSegment(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		s *segment,
		dir string,
		c Config,
	){
		"append and read succeeds":              testSegmentAppendRead,
		"segment becomes maxed after filling":   testSegmentIsMaxed,
		"reopen preserves state":                testSegmentReopen,
		"remove deletes files":                  testSegmentRemove,
		"read from empty segment returns EOF":   testSegmentReadEmpty,
		"read past end returns EOF":             testSegmentReadPastEnd,
		"store maxed triggers segment maxed":    testSegmentStoreMaxed,
		"non-zero base offset works correctly":  testSegmentWithNonZeroBaseOffset,
	} {
		t.Run(scenario, func(t *testing.T) {
			s, dir, c, teardown := setupSegment(t)
			defer teardown()
			fn(t, s, dir, c)
		})
	}
}

func setupSegment(t *testing.T) (
	s *segment,
	dir string,
	c Config,
	teardown func(),
) {
	t.Helper()

	dir, err := os.MkdirTemp("", "segment_test")
	if err != nil {
		t.Fatal(err)
	}

	c = Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = uint64(entWidth) * 3

	s, err = newSegment(dir, 16, c)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	return s, dir, c, func() {
		s.Close()
		os.RemoveAll(dir)
	}
}

func testSegmentAppendRead(t *testing.T, s *segment, dir string, c Config) {
	want := &api.Record{Value: []byte("hello world")}

	off, err := s.Append(want)
	if err != nil {
		t.Fatal(err)
	}
	if off != 16 {
		t.Fatalf("expected offset=16, got %d", off)
	}

	got, err := s.Read(off)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != string(want.Value) {
		t.Fatalf("expected value=%s, got %s", want.Value, got.Value)
	}
}

func testSegmentIsMaxed(t *testing.T, s *segment, dir string, c Config) {
	want := &api.Record{Value: []byte("hello world")}

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
	}

	if !s.IsMaxed() {
		t.Fatal("segment should be maxed")
	}

	_, err := s.Append(want)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when segment is maxed, got %v", err)
	}
}

func testSegmentReopen(t *testing.T, s *segment, dir string, c Config) {
	want := &api.Record{Value: []byte("hello world")}

	for i := uint64(0); i < 3; i++ {
		_, err := s.Append(want)
		if err != nil {
			t.Fatal(err)
		}
	}

	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}

	s, err = newSegment(dir, 16, c)
	if err != nil {
		t.Fatal(err)
	}

	if s.nextOffset != 19 {
		t.Fatalf("expected nextOffset=19 after reopen, got %d", s.nextOffset)
	}

	if !s.IsMaxed() {
		t.Fatal("segment should be maxed after reopen")
	}
}

func testSegmentRemove(t *testing.T, s *segment, dir string, c Config) {
	_, err := s.Append(&api.Record{Value: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	storeName := s.store.Name()
	indexName := s.index.Name()

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

	if _, err := os.Stat(storeName); !os.IsNotExist(err) {
		t.Fatal("store file should be removed")
	}
	if _, err := os.Stat(indexName); !os.IsNotExist(err) {
		t.Fatal("index file should be removed")
	}
}

func testSegmentReadEmpty(t *testing.T, s *segment, dir string, c Config) {
	// Create a fresh segment at offset 0 for this test
	s.Close()

	newDir, err := os.MkdirTemp("", "segment_read_empty_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(newDir)

	s, err = newSegment(newDir, 0, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, err = s.Read(0)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when reading from empty segment, got %v", err)
	}
}

func testSegmentReadPastEnd(t *testing.T, s *segment, dir string, c Config) {
	off, err := s.Append(&api.Record{Value: []byte("test")})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.Read(off + 1)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when reading past end, got %v", err)
	}
}

func testSegmentStoreMaxed(t *testing.T, s *segment, dir string, c Config) {
	// Close the default segment and create one with small store size
	s.Close()

	newDir, err := os.MkdirTemp("", "segment_store_maxed_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(newDir)

	c.Segment.MaxStoreBytes = 50
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(newDir, 0, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

func testSegmentWithNonZeroBaseOffset(t *testing.T, s *segment, dir string, c Config) {
	// Close the default segment and create one with different base offset
	s.Close()

	newDir, err := os.MkdirTemp("", "segment_base_offset_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(newDir)

	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = 1024

	baseOffset := uint64(100)
	s, err = newSegment(newDir, baseOffset, c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if s.baseOffset != baseOffset {
		t.Fatalf("expected baseOffset=%d, got %d", baseOffset, s.baseOffset)
	}
	if s.nextOffset != baseOffset {
		t.Fatalf("expected nextOffset=%d, got %d", baseOffset, s.nextOffset)
	}

	for i := uint64(0); i < 5; i++ {
		off, err := s.Append(&api.Record{Value: []byte("test")})
		if err != nil {
			t.Fatal(err)
		}
		expectedOff := baseOffset + i
		if off != expectedOff {
			t.Fatalf("expected offset=%d, got %d", expectedOff, off)
		}

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
}
